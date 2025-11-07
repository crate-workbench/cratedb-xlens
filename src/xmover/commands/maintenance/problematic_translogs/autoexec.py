"""
Automatic execution logic for problematic translog replica reset operations

This module contains the AutoExecHandler class for filtering and executing automatic
replica reset operations, along with the TableResetProcessor state machine that manages
individual table replica reset workflows.
"""

import sys
import time
from contextlib import contextmanager
from enum import Enum
from typing import Dict, Any, List, Union, Optional

from loguru import logger
from rich.console import Console

from ..base import TableInfo, QueryResultHelper, json_logging_mode

console = Console()


class TableResetState(Enum):
    """States for the table replica reset state machine"""
    DETECTED = "detected"
    SETTING_REPLICAS_ZERO = "setting_replicas_zero"
    MONITORING_LEASES = "monitoring_leases"
    RESTORING_REPLICAS = "restoring_replicas"
    COMPLETED = "completed"
    FAILED = "failed"


class TableResetProcessor:
    """State machine processor for individual table replica reset operations"""

    def __init__(self, table_info: Union[Dict[str, Any], TableInfo], client,
                 dry_run: bool = False, max_wait: int = 720, log_format: str = "console"):
        # Convert dict to TableInfo if needed (backward compatibility)
        if isinstance(table_info, dict):
            self.table_info = TableInfo.from_dict(table_info)
        else:
            self.table_info = table_info

        self.client = client
        self.dry_run = dry_run
        self.max_wait = max_wait
        self.log_format = log_format

        # Extract commonly used fields for convenience
        self.schema_name = self.table_info.schema_name
        self.table_name = self.table_info.table_name
        self.partition_values = self.table_info.partition_values or ''
        self.partition_ident = self.table_info.partition_ident or ''
        self.original_replicas = self.table_info.current_replicas

        self.state = TableResetState.DETECTED
        self.start_time = None
        self.error_message = None

        # Setup logger
        if log_format == "json":
            self.logger = logger
        else:
            self.logger = None

    def get_table_display_name(self) -> str:
        """Get human-readable table name"""
        return self.table_info.get_display_name()

    def _validate_identifier(self, identifier: str) -> None:
        """Validate SQL identifier to prevent injection"""
        if not identifier:
            raise ValueError("Identifier cannot be empty")
        # Check for dangerous characters that could break out of quoted identifiers
        if '"' in identifier:
            raise ValueError(f"Identifier contains invalid character: {identifier}")

    def _build_alter_replicas_sql(self, replica_count: int) -> str:
        """Build ALTER TABLE SQL for setting replica count safely

        Args:
            replica_count: The number of replicas to set

        Returns:
            SQL string with properly quoted identifiers

        Raises:
            ValueError: If identifiers contain invalid characters
        """
        # Validate identifiers to prevent injection
        self._validate_identifier(self.schema_name)
        self._validate_identifier(self.table_name)

        # Build base ALTER TABLE statement with quoted identifiers
        sql = f'ALTER TABLE "{self.schema_name}"."{self.table_name}"'

        # Add partition clause if this is a partitioned table
        if self.table_info.has_partition():
            # Partition values are already formatted correctly from the database
            sql += f' PARTITION {self.partition_values}'

        # Add the SET clause
        sql += f' SET ("number_of_replicas" = {replica_count});'

        return sql

    def process(self) -> bool:
        """Process through all states, returns True if successful"""
        self.start_time = time.time()

        try:
            if not self._set_replicas_to_zero():
                return False
            if not self._monitor_retention_leases():
                return False
            if not self._restore_replicas():
                return False

            self._transition_to_state(TableResetState.COMPLETED)
            self._log_info(f"Successfully completed replica reset in {time.time() - self.start_time:.1f}s")
            return True

        except Exception as e:
            self._handle_failure(f"Unexpected error: {e}")
            return False

    def _set_replicas_to_zero(self) -> bool:
        """Set table replicas to 0"""
        self._transition_to_state(TableResetState.SETTING_REPLICAS_ZERO)

        try:
            sql = self._build_alter_replicas_sql(0)

            self._log_info(f"Setting replicas to 0 (original: {self.original_replicas})")
            self._log_info(f"Executing: {sql}")

            if not self.dry_run:
                result = self.client.execute_query(sql)
                if QueryResultHelper.is_error(result):
                    error_msg = QueryResultHelper.get_error_message(result)
                    self._handle_failure(f"Failed to set replicas to 0: {error_msg}")
                    return False
            else:
                self._log_info(f"DRY RUN: Would execute: {sql}")

            return True

        except Exception as e:
            self._handle_failure(f"Error setting replicas to 0: {e}")
            return False

    def _monitor_retention_leases(self) -> bool:
        """Monitor retention leases with incremental backoff"""
        self._transition_to_state(TableResetState.MONITORING_LEASES)

        delays = self._get_backoff_delays()
        start_time = time.time()

        for attempt, delay in enumerate(delays, 1):
            if not self.dry_run:
                lease_count = self._check_retention_leases()
                expected_count = self.table_info.total_primary_shards

                if lease_count == expected_count:
                    elapsed = time.time() - start_time
                    self._log_info(f"Retention leases cleared after {elapsed:.1f}s ({attempt} attempts)")
                    return True

                elapsed = time.time() - start_time
                remaining_time = self.max_wait - elapsed

                if remaining_time <= 0:
                    self._handle_failure(f"Timeout after {self.max_wait}s - {lease_count} leases remaining (expected {expected_count})")
                    return False

                actual_delay = min(delay, remaining_time)
                self._log_info(f"Attempt {attempt}/{len(delays)}: {lease_count} leases remaining, waiting {actual_delay}s")

                time.sleep(actual_delay)
            else:
                self._log_info(f"DRY RUN: Would wait {delay}s (attempt {attempt}/{len(delays)})")
                if attempt >= 3:  # Simulate success after 3 attempts in dry run
                    self._log_info("DRY RUN: Simulating retention leases cleared")
                    return True

        self._handle_failure(f"Timeout after {self.max_wait}s - retention leases not cleared")
        return False

    def _restore_replicas(self) -> bool:
        """Restore original replica count"""
        self._transition_to_state(TableResetState.RESTORING_REPLICAS)

        try:
            sql = self._build_alter_replicas_sql(self.original_replicas)

            self._log_info(f"Restoring replicas to {self.original_replicas}")
            self._log_info(f"Executing: {sql}")

            if not self.dry_run:
                result = self.client.execute_query(sql)
                if QueryResultHelper.is_error(result):
                    error_msg = QueryResultHelper.get_error_message(result)
                    self._handle_failure(f"CRITICAL: Failed to restore replicas: {error_msg}")
                    return False
            else:
                self._log_info(f"DRY RUN: Would execute: {sql}")

            return True

        except Exception as e:
            self._handle_failure(f"CRITICAL: Error restoring replicas: {e}")
            return False

    def _check_retention_leases(self) -> int:
        """Check current retention lease count"""
        try:
            if self.table_info.has_partition():
                sql = """
                SELECT array_length(retention_leases['leases'], 1) as cnt_leases
                FROM sys.shards
                WHERE table_name = ?
                  AND schema_name = ?
                  AND partition_ident = ?
                """
                params = [self.table_name, self.schema_name, self.partition_ident]
            else:
                sql = """
                SELECT array_length(retention_leases['leases'], 1) as cnt_leases
                FROM sys.shards
                WHERE table_name = ?
                  AND schema_name = ?
                """
                params = [self.table_name, self.schema_name]

            result = self.client.execute_query(sql, params)
            rows = QueryResultHelper.get_rows(result)
            if rows:
                # Return the maximum lease count across all shards
                return max(row[0] or 0 for row in rows)
            return 0

        except Exception as e:
            self._log_error(f"Error checking retention leases: {e}")
            return -1  # Error condition

    def _get_backoff_delays(self) -> List[int]:
        """Generate incremental backoff delays"""
        # Predefined sequence: 10, 15, 30, 45, 60, 90, 135, 200, 300, 450, 720
        base_delays = [10, 15, 30, 45, 60, 90, 135, 200, 300, 450, 720]
        delays = []
        total_time = 0

        for delay in base_delays:
            if total_time >= self.max_wait:
                break

            actual_delay = min(delay, self.max_wait - total_time)
            if actual_delay > 0:
                delays.append(actual_delay)
                total_time += actual_delay

            if total_time >= self.max_wait:
                break

        return delays

    def _transition_to_state(self, new_state: TableResetState) -> None:
        """Transition to a new state with logging"""
        old_state = self.state
        self.state = new_state

        elapsed = time.time() - self.start_time if self.start_time else 0
        self._log_info(f"State transition: {old_state.value} → {new_state.value} ({elapsed:.1f}s)")

    def _handle_failure(self, error_msg: str) -> None:
        """Handle failure state with rollback attempt"""
        self.error_message = error_msg

        # CRITICAL FIX: Save previous state BEFORE transitioning to FAILED
        previous_state = self.state
        self._transition_to_state(TableResetState.FAILED)
        self._log_error(error_msg)

        # Attempt rollback if we were in monitoring or restoring phase
        # Use the saved previous_state instead of self.state (which is now FAILED)
        if previous_state in [TableResetState.MONITORING_LEASES, TableResetState.RESTORING_REPLICAS]:
            self._attempt_rollback()

    def _attempt_rollback(self) -> None:
        """Attempt to rollback by restoring original replica count"""
        if self.dry_run:
            self._log_info("DRY RUN: Would attempt rollback to original replica count")
            return

        try:
            self._log_info(f"Attempting rollback: restoring {self.original_replicas} replicas")

            sql = self._build_alter_replicas_sql(self.original_replicas)

            self._log_info(f"Rollback executing: {sql}")
            result = self.client.execute_query(sql)
            if QueryResultHelper.is_success(result):
                self._log_info("Rollback successful")
            else:
                error_msg = QueryResultHelper.get_error_message(result)
                self._log_error(f"MANUAL INTERVENTION REQUIRED: Rollback failed - {error_msg}")

        except Exception as e:
            self._log_error(f"MANUAL INTERVENTION REQUIRED: Rollback exception - {e}")

    def _log_info(self, message: str) -> None:
        """Log info message"""
        if self.logger and self.log_format == "json":
            self.logger.info(message,
                           table=self.get_table_display_name(),
                           state=self.state.value,
                           original_replicas=self.original_replicas)
        else:
            console.print(f"[dim]{time.strftime('%H:%M:%S')}[/dim] [blue]INFO[/blue] {self.get_table_display_name()}: {message}")

    def _log_error(self, message: str) -> None:
        """Log error message"""
        if self.logger and self.log_format == "json":
            self.logger.error(message,
                            table=self.get_table_display_name(),
                            state=self.state.value,
                            original_replicas=self.original_replicas)
        else:
            console.print(f"[dim]{time.strftime('%H:%M:%S')}[/dim] [red]ERROR[/red] {self.get_table_display_name()}: {message}")


class AutoExecHandler:
    """Handler for automatic execution of replica reset operations"""

    def __init__(self, client, console):
        """Initialize the AutoExecHandler

        Args:
            client: Database client for executing queries
            console: Rich console for output
        """
        self.client = client
        self.console = console
        self._autoexec_exit_code = 1

    def execute_autoexec(self, summary_rows: List[Dict[str, Any]], dry_run: bool,
                         percentage: int, max_wait: int, log_format: str,
                         get_current_replica_count_fn) -> bool:
        """Execute automatic replica reset for all problematic tables

        Args:
            summary_rows: List of table summary data
            dry_run: Whether to run in dry-run mode
            percentage: Percentage threshold for filtering tables
            max_wait: Maximum wait time in seconds
            log_format: Log format (console or json)
            get_current_replica_count_fn: Function to get current replica count

        Returns:
            True if all operations succeeded, False otherwise
        """

        # Filter tables based on percentage threshold
        filtered_tables = self._filter_tables_by_percentage(
            summary_rows, percentage, get_current_replica_count_fn
        )

        if not filtered_tables:
            self.console.print(f"[green]✅ No tables exceed {percentage}% of their threshold[/green]")
            self._autoexec_exit_code = 0
            return True

        self.console.print(f"[yellow]Processing {len(filtered_tables)} table(s) exceeding {percentage}% threshold[/yellow]")

        success_count = 0
        failure_count = 0
        failed_tables = []

        start_time = time.time()

        # Use context manager for JSON logging to avoid global state mutation
        log_context = json_logging_mode() if log_format == "json" else contextmanager(lambda: iter([None]))()

        with log_context:
            # Process each table
            for table_info in filtered_tables:
                processor = TableResetProcessor(table_info, self.client, dry_run, max_wait, log_format)

                table_display = processor.get_table_display_name()
                self.console.print(f"\n[cyan]Processing: {table_display}[/cyan]")

                if processor.process():
                    success_count += 1
                    self.console.print(f"[green]✅ {table_display} completed successfully[/green]")
                else:
                    failure_count += 1
                    failed_tables.append(table_display)
                    self.console.print(f"[red]❌ {table_display} failed[/red]")

        # Summary
        total_time = time.time() - start_time
        self.console.print(f"\n[bold]AutoExec Summary:[/bold]")
        self.console.print(f"  • Total tables processed: {len(filtered_tables)}")
        self.console.print(f"  • Successful: {success_count}")
        self.console.print(f"  • Failed: {failure_count}")
        self.console.print(f"  • Total time: {total_time:.1f}s")

        if failed_tables:
            self.console.print(f"\n[red]Failed tables requiring manual intervention:[/red]")
            for table in failed_tables:
                self.console.print(f"  • {table}")

        # Set exit code tracking
        if failure_count == 0:
            self._autoexec_exit_code = 0
        elif success_count > 0:
            self._autoexec_exit_code = 3  # Partial failure
        else:
            self._autoexec_exit_code = 2  # Complete failure

        return failure_count == 0

    def _filter_tables_by_percentage(self, summary_rows: List[Dict[str, Any]],
                                     percentage: int,
                                     get_current_replica_count_fn) -> List[Dict[str, Any]]:
        """Filter tables that exceed the percentage threshold

        Args:
            summary_rows: List of table summary data
            percentage: Percentage threshold
            get_current_replica_count_fn: Function to get current replica count

        Returns:
            List of filtered table info dictionaries
        """
        filtered = []

        for table_info in summary_rows:
            max_translog_mb = table_info['max_translog_uncommitted_mb']

            # Use actual adaptive threshold from table configuration
            threshold_mb = table_info.get('adaptive_threshold_mb', 563)  # Fallback to 563MB if not available

            # Calculate percentage
            if threshold_mb > 0:
                current_percentage = (max_translog_mb / threshold_mb) * 100
                if current_percentage >= percentage:
                    # Add current replica count
                    schema = table_info['schema_name']
                    table = table_info['table_name']
                    partition_values = table_info.get('partition_values', '')
                    partition_ident = table_info.get('partition_ident')
                    current_replicas = get_current_replica_count_fn(
                        schema, table, partition_ident, partition_values
                    )
                    table_info['current_replicas'] = current_replicas
                    filtered.append(table_info)

        return filtered

    def get_autoexec_exit_code(self) -> int:
        """Get the appropriate exit code for autoexec operations"""
        return self._autoexec_exit_code
