"""
Problematic translog command implementation

This module contains the main ProblematicTranslogsCommand class that orchestrates
the detection, analysis, and remediation of tables with problematic translog sizes.
"""

import sys
from typing import List, Dict, Any, Union, Optional

from rich.panel import Panel

from ...base import BaseCommand
from ..base import TableInfo, QueryResultHelper
from .display import ProblematicTranslogsDisplay
from .sql_generator import ProblematicTranslogsSQLGenerator
from .autoexec import AutoExecHandler


class ProblematicTranslogsCommand(BaseCommand):
    """Command handler for problematic translog analysis and remediation"""

    def __init__(self, client):
        """Initialize the problematic translogs command

        Args:
            client: Database client for executing queries
        """
        super().__init__(client)
        self.display = ProblematicTranslogsDisplay(self.console)
        self.sql_generator = ProblematicTranslogsSQLGenerator(client, self.console)
        self.autoexec_handler = AutoExecHandler(client, self.console)
        self.debug = False  # Will be set by execute() method

    def execute(self, sizemb: int, generate_sql: bool, autoexec: bool = False,
                             dry_run: bool = False, percentage: int = 200,
                             max_wait: int = 720, log_format: str = "console", debug: bool = False) -> None:
        """Find tables with problematic translog sizes and optionally execute automatic replica reset

        This command identifies tables with replica shards that have large uncommitted translog sizes
        indicating replication issues.

        In analysis mode (default), it generates a complete sequence including:
        1. Stop automatic shard rebalancing
        2. REROUTE CANCEL commands for problematic shards
        3. REROUTE ALLOCATE commands to recreate replicas
        4. Re-enable automatic shard rebalancing

        In autoexec mode (--autoexec), it automatically executes:
        1. Set replicas to 0 for each problematic table
        2. Monitor retention leases until cleared
        3. Restore original replica count

        Args:
            sizemb: Minimum translog size in MB to consider problematic
            generate_sql: Whether to generate comprehensive SQL commands
            autoexec: Whether to automatically execute replica reset operations
            dry_run: Whether to run in dry-run mode (autoexec only)
            percentage: Percentage threshold for filtering tables (autoexec only)
            max_wait: Maximum wait time in seconds (autoexec only)
            log_format: Log format (console or json)
        """
        if not self.validate_connection():
            return

        # Enable debug mode on the client if requested
        self.debug = debug
        self.client.debug = debug
        if debug:
            self.console.print("[yellow]🐛 DEBUG MODE ENABLED - Will log node names and SQL queries[/yellow]")

        self.console.print(Panel.fit(f"[bold blue]Problematic Translog Analysis[/bold blue]"))
        self.console.print(f"[dim]Using adaptive thresholds based on table flush_threshold_size settings (≥ {sizemb} MB baseline)[/dim]")

        if autoexec:
            mode_desc = "DRY RUN" if dry_run else "AUTOEXEC"
            self.console.print(f"[red]🤖 {mode_desc} MODE - automatically executing replica reset operations[/red]")
        elif generate_sql:
            self.console.print("[yellow]⚠️  COMMAND GENERATION MODE - SQL commands will be generated for display[/yellow]")
        else:
            self.console.print("[green]🔍 ANALYSIS MODE - showing problematic shards only[/green]")

        self.console.print()

        try:
            # Get both individual shards and table summaries using adaptive thresholds
            individual_shards, summary_rows = self._get_problematic_translogs(sizemb)

            if not individual_shards:
                self.console.print("[green]✅ No problematic translog shards found using adaptive thresholds![/green]")
                return

            # Display individual problematic shards with adaptive threshold info
            self.display.display_individual_problematic_shards(individual_shards, sizemb)

            # Display summary by table
            self.display.display_table_summary(summary_rows, self._get_current_replica_count)

            if autoexec:
                # Execute automatic replica reset
                success = self.autoexec_handler.execute_autoexec(
                    summary_rows, dry_run, percentage, max_wait, log_format,
                    self._get_current_replica_count
                )
                if not success:
                    sys.exit(self.autoexec_handler.get_autoexec_exit_code())
            elif generate_sql:
                self.sql_generator.generate_comprehensive_commands(
                    individual_shards, summary_rows, self._get_current_replica_count
                )
            else:
                self.console.print()
                self.console.print("[dim]💡 Use --execute flag to generate comprehensive shard management commands for display[/dim]")
                self.console.print("[dim]💡 Use --autoexec flag to automatically execute replica reset operations[/dim]")

        except Exception as e:
            self.console.print(f"[red]Error analyzing problematic translogs: {e}[/red]")
            if autoexec:
                sys.exit(1)

    def _get_problematic_translogs(self, min_size_mb: int) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Get individual shards and table summaries with problematic translog sizes using adaptive thresholds

        Args:
            min_size_mb: Minimum translog size in MB

        Returns:
            Tuple of (individual_shards, summary_rows)
        """

        # Step 1: Find shards above initial threshold
        initial_shards, initial_summary = self._get_initial_problematic_translogs(min_size_mb)

        # Step 2: Get table-specific flush thresholds for problematic tables only
        table_thresholds = self._get_table_flush_thresholds(initial_shards)

        # Step 3: Apply adaptive thresholds and re-filter results
        adaptive_shards, adaptive_summary = self._apply_adaptive_thresholds(
            initial_shards, initial_summary, table_thresholds, min_size_mb
        )

        return adaptive_shards, adaptive_summary

    def _get_initial_problematic_translogs(self, min_size_mb: int) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Get initial problematic shards using basic threshold

        Args:
            min_size_mb: Minimum translog size in MB

        Returns:
            Tuple of (individual_shards, summary_rows)
        """

        # Query for individual problematic shards (for REROUTE CANCEL commands)
        individual_shards_query = """
            SELECT
                sh.schema_name,
                sh.table_name,
                translate(p.values::text, ':{}', '=()') as partition_values,
                sh.id AS shard_id,
                COALESCE(node['name'], 'unknown-' || COALESCE(node['id'], 'corrupted')) AS node_name,
                COALESCE(sh.translog_stats['uncommitted_size'] / 1024^2, 0) AS translog_uncommitted_mb
            FROM
                sys.shards AS sh
            LEFT JOIN information_schema.table_partitions p
                ON sh.table_name = p.table_name
                AND sh.schema_name = p.table_schema
                AND sh.partition_ident = p.partition_ident
            WHERE
                sh.state = 'STARTED'
                AND COALESCE(sh.translog_stats['uncommitted_size'], 0) > ? * 1024^2
                AND sh.primary = FALSE
            ORDER BY
                COALESCE(sh.translog_stats['uncommitted_size'], 0) DESC
        """

        # Query to find tables with problematic replica shards (grouped by table/partition)
        summary_query = """
            SELECT
                all_shards.schema_name,
                all_shards.table_name,
                translate(p.values::text, ':{}', '=()') as partition_values,
                p.partition_ident,
                COUNT(CASE WHEN all_shards.primary=FALSE AND COALESCE(all_shards.translog_stats['uncommitted_size'], 0) > ? * 1024^2 THEN 1 END) as problematic_replica_shards,
                MAX(CASE WHEN all_shards.primary=FALSE AND COALESCE(all_shards.translog_stats['uncommitted_size'], 0) > ? * 1024^2 THEN COALESCE(all_shards.translog_stats['uncommitted_size'] / 1024^2, 0) END) AS max_translog_uncommitted_mb,
                COUNT(CASE WHEN all_shards.primary=TRUE THEN 1 END) as total_primary_shards,
                COUNT(CASE WHEN all_shards.primary=FALSE THEN 1 END) as total_replica_shards,
                SUM(CASE WHEN all_shards.primary=TRUE THEN COALESCE(all_shards.size / 1024^3, 0) ELSE 0 END) as total_primary_size_gb,
                SUM(CASE WHEN all_shards.primary=FALSE THEN COALESCE(all_shards.size / 1024^3, 0) ELSE 0 END) as total_replica_size_gb
            FROM
                sys.shards AS all_shards
            LEFT JOIN information_schema.table_partitions p
                ON all_shards.table_name = p.table_name
                AND all_shards.schema_name = p.table_schema
                AND all_shards.partition_ident = p.partition_ident
            WHERE
                all_shards.state = 'STARTED'
                AND all_shards.schema_name || '.' || all_shards.table_name || COALESCE(all_shards.partition_ident, '') IN (
                    SELECT DISTINCT sh.schema_name || '.' || sh.table_name || COALESCE(sh.partition_ident, '')
                    FROM sys.shards AS sh
                    WHERE sh.state = 'STARTED'
                    AND COALESCE(sh.translog_stats['uncommitted_size'], 0) > ? * 1024^2
                    AND sh.primary=FALSE
                )
            GROUP BY
                all_shards.schema_name, all_shards.table_name, partition_values, p.partition_ident
            ORDER BY
                max_translog_uncommitted_mb DESC
        """

        # Execute both queries
        individual_result = self.client.execute_query(individual_shards_query, [min_size_mb])
        individual_shards = individual_result.get('rows', [])

        summary_result = self.client.execute_query(summary_query, [min_size_mb, min_size_mb, min_size_mb])
        summary_rows = summary_result.get('rows', [])

        # Convert individual shards to dictionaries
        individual_shard_dicts = []
        for row in individual_shards:
            schema_name, table_name, partition_values, shard_id, node_name, translog_mb = row
            individual_shard_dicts.append({
                'schema_name': schema_name,
                'table_name': table_name,
                'partition_values': partition_values,
                'shard_id': shard_id,
                'node_name': node_name,
                'translog_size_mb': translog_mb
            })

        # Convert summary data to dictionaries
        summary_dicts = []
        for row in summary_rows:
            schema_name, table_name, partition_values, partition_ident, problematic_replica_shards, max_translog_mb, total_primary_shards, total_replica_shards, total_primary_size_gb, total_replica_size_gb = row
            summary_dicts.append({
                'schema_name': schema_name,
                'table_name': table_name,
                'partition_values': partition_values,
                'partition_ident': partition_ident,
                'problematic_replica_shards': problematic_replica_shards,
                'max_translog_uncommitted_mb': max_translog_mb,
                'total_primary_shards': total_primary_shards,
                'total_replica_shards': total_replica_shards,
                'total_primary_size_gb': total_primary_size_gb,
                'total_replica_size_gb': total_replica_size_gb
            })

        return individual_shard_dicts, summary_dicts

    def _get_table_flush_thresholds(self, individual_shards: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
        """Get flush threshold settings for tables that have problematic shards

        Args:
            individual_shards: List of individual problematic shards

        Returns:
            Dictionary mapping table keys to threshold info
        """

        if not individual_shards:
            return {}

        # Get unique table/schema combinations from problematic shards
        unique_tables = set()
        table_partitions = set()

        for shard in individual_shards:
            schema = shard['schema_name']
            table = shard['table_name']
            unique_tables.add((schema, table))

            # Also track partitions for partition-specific settings
            if shard.get('partition_values'):
                table_partitions.add((schema, table, shard.get('partition_values', '')))

        # Query table-level flush thresholds
        table_thresholds = {}

        if unique_tables:
            # Build query for table settings
            table_conditions = []
            params = []
            for schema, table in unique_tables:
                table_conditions.append("(table_schema = ? AND table_name = ?)")
                params.extend([schema, table])

            table_query = f"""
                SELECT
                    table_schema,
                    table_name,
                    COALESCE(settings['translog']['flush_threshold_size'], 536870912) as flush_threshold_bytes
                FROM information_schema.tables
                WHERE {' OR '.join(table_conditions)}
            """

            result = self.client.execute_query(table_query, params)
            for row in result.get('rows', []):
                schema, table, threshold_bytes = row
                table_key = f"{schema}.{table}"
                config_mb = threshold_bytes / (1024 ** 2)
                threshold_mb = config_mb * 1.1
                table_thresholds[table_key] = {
                    'config_mb': config_mb,
                    'threshold_mb': threshold_mb
                }

        # Query partition-level flush thresholds (if different from table)
        if table_partitions:
            partition_conditions = []
            partition_params = []
            for schema, table, partition_values in table_partitions:
                if partition_values:  # Only check partitions that actually exist
                    partition_conditions.append("(table_schema = ? AND table_name = ?)")
                    partition_params.extend([schema, table])

            if partition_conditions:
                partition_query = f"""
                    SELECT
                        table_schema,
                        table_name,
                        translate(values::text, ':{{}}', '=()') as partition_values,
                        COALESCE(settings['translog']['flush_threshold_size'], 536870912) as flush_threshold_bytes
                    FROM information_schema.table_partitions
                    WHERE {' OR '.join(partition_conditions)}
                """

                result = self.client.execute_query(partition_query, partition_params)
                for row in result.get('rows', []):
                    schema, table, partition_values, threshold_bytes = row
                    partition_key = f"{schema}.{table}.{partition_values}"
                    config_mb = threshold_bytes / (1024 ** 2)
                    threshold_mb = config_mb * 1.1
                    table_thresholds[partition_key] = {
                        'config_mb': config_mb,
                        'threshold_mb': threshold_mb
                    }

        return table_thresholds

    def _apply_adaptive_thresholds(self, initial_shards: List[Dict[str, Any]],
                                 initial_summary: List[Dict[str, Any]],
                                 table_thresholds: Dict[str, Dict[str, float]],
                                 fallback_threshold_mb: int) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Enrich shards with adaptive threshold information

        Note: --sizeMB (fallback_threshold_mb) is always respected as the minimum threshold.
        Tables with higher configured flush_threshold_size will use their higher threshold.

        Args:
            initial_shards: List of initial problematic shards
            initial_summary: List of initial summary data
            table_thresholds: Dictionary of table-specific thresholds
            fallback_threshold_mb: Fallback threshold in MB

        Returns:
            Tuple of (adaptive_shards, adaptive_summary)
        """

        adaptive_shards = []
        adaptive_summary = []

        # Enrich shards with adaptive threshold information
        for shard in initial_shards:
            schema = shard['schema_name']
            table = shard['table_name']
            partition_values = shard.get('partition_values', '')
            translog_mb = shard['translog_size_mb']

            # Try partition-specific threshold first, then table-level, then fallback
            partition_key = f"{schema}.{table}.{partition_values}" if partition_values else None
            table_key = f"{schema}.{table}"

            threshold_info = None
            if partition_key and partition_key in table_thresholds:
                threshold_info = table_thresholds[partition_key]
            elif table_key in table_thresholds:
                threshold_info = table_thresholds[table_key]

            if threshold_info:
                config_mb = threshold_info['config_mb']
                threshold_mb = threshold_info['threshold_mb']
            else:
                config_mb = fallback_threshold_mb
                threshold_mb = fallback_threshold_mb

            # Use the higher of user-specified threshold (--sizeMB) or table-specific threshold
            # This ensures we respect table configurations while allowing user override
            effective_threshold = max(fallback_threshold_mb, threshold_mb)
            if translog_mb > effective_threshold:
                shard['adaptive_config_mb'] = config_mb
                shard['adaptive_threshold_mb'] = threshold_mb
                adaptive_shards.append(shard)

        # Build summary data - only keep tables that have problematic shards
        adaptive_table_keys = set()
        for shard in adaptive_shards:
            schema = shard['schema_name']
            table = shard['table_name']
            partition_values = shard.get('partition_values', '')
            key = f"{schema}.{table}.{partition_values}" if partition_values else f"{schema}.{table}"
            adaptive_table_keys.add(key)

        for summary in initial_summary:
            schema = summary['schema_name']
            table = summary['table_name']
            partition_values = summary.get('partition_values', '')
            summary_key = f"{schema}.{table}.{partition_values}" if partition_values else f"{schema}.{table}"

            if summary_key in adaptive_table_keys:
                # Add adaptive threshold information to summary
                threshold_info = table_thresholds.get(summary_key)
                if threshold_info:
                    summary['adaptive_config_mb'] = threshold_info['config_mb']
                    summary['adaptive_threshold_mb'] = threshold_info['threshold_mb']
                else:
                    summary['adaptive_config_mb'] = fallback_threshold_mb
                    summary['adaptive_threshold_mb'] = fallback_threshold_mb
                adaptive_summary.append(summary)

        return adaptive_shards, adaptive_summary

    def _get_current_replica_count(self, schema_name: str, table_name: str,
                                   partition_ident: Optional[str] = None,
                                   partition_values: Optional[str] = None) -> Union[int, str]:
        """Look up current replica count for table or partition

        Args:
            schema_name: Schema name
            table_name: Table name
            partition_ident: Partition identifier (optional)
            partition_values: Partition values (optional)

        Returns:
            int: Replica count if successfully parsed
            str: "unknown" if lookup fails, or raw value if parsing fails
        """
        try:
            # Check if this is a partitioned table
            if partition_ident and partition_ident != 'NULL':
                # Query for partition-specific replica count
                query = """
                    SELECT number_of_replicas
                    FROM information_schema.table_partitions
                    WHERE table_schema = ?
                      AND table_name = ?
                      AND partition_ident = ?
                """
                params = [schema_name, table_name, partition_ident]
            else:
                # Query for table-level replica count
                query = """
                    SELECT number_of_replicas
                    FROM information_schema.tables
                    WHERE table_schema = ?
                      AND table_name = ?
                """
                params = [schema_name, table_name]

            result = self.client.execute_query(query, params)

            if QueryResultHelper.is_error(result):
                return "unknown"

            rows = QueryResultHelper.get_rows(result)
            if not rows or not rows[0]:
                return "unknown"

            # Parse replica count - handle various formats
            replica_value = rows[0][0]
            if replica_value is None:
                return "unknown"

            # Try to parse as integer
            try:
                # Handle range format "0-1" by taking the first value
                if isinstance(replica_value, str) and '-' in replica_value:
                    return int(replica_value.split('-')[0])
                return int(replica_value)
            except (ValueError, TypeError):
                # Return raw value if parsing fails
                return str(replica_value)

        except Exception as e:
            error_msg = str(e)

            # Provide more specific guidance based on error type
            if '404' in error_msg:
                self.console.print(f"[red]Warning: SQL endpoint returned 404 for {schema_name}.{table_name}[/red]")
                self.console.print(f"[dim]  This indicates severe cluster degradation - AWS LB may be routing to dead nodes[/dim]")
                self.console.print(f"[dim]  Run 'xmover test-connection --diagnose' to check load balancer health[/dim]")
            elif 'timeout' in error_msg.lower():
                self.console.print(f"[yellow]Warning: Timeout querying replica count for {schema_name}.{table_name}[/yellow]")
                self.console.print(f"[dim]  Cluster is slow - consider increasing CRATE_DISCOVERY_TIMEOUT[/dim]")
            else:
                self.console.print(f"[yellow]Warning: Could not determine replica count for {schema_name}.{table_name}: {e}[/yellow]")

            return "?"
