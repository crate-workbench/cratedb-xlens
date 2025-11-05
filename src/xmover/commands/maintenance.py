"""
Maintenance command handlers for XMover

This module contains commands related to cluster maintenance operations:
- shard_distribution: Analyze shard distribution anomalies across cluster nodes
- problematic_translogs: Find tables with problematic translog sizes and generate shard management commands
"""

import sys
import time
from enum import Enum
from typing import Optional, List, Dict, Any, Union
import click
from rich.table import Table
from rich.panel import Panel
from rich import box
from rich.console import Console

from loguru import logger

from .base import BaseCommand
from ..distribution_analyzer import DistributionAnalyzer
from ..shard_size_monitor import ShardSizeMonitor
from ..utils import format_size

console = Console()


class MaintenanceCommands(BaseCommand):
    """Command handlers for cluster maintenance operations"""

    def execute(self, command: str, **kwargs) -> None:
        """Execute a maintenance command by name"""
        if command == 'shard_distribution':
            self.shard_distribution(**kwargs)
        elif command == 'problematic_translogs':
            self.problematic_translogs(**kwargs)
        else:
            raise ValueError(f"Unknown maintenance command: {command}")

    def shard_distribution(self, top_tables: int, table: Optional[str]) -> None:
        """Analyze shard distribution anomalies across cluster nodes

        This command analyzes the largest tables in your cluster to detect:
        • Uneven shard count distribution between nodes
        • Storage imbalances across nodes
        • Missing node coverage for tables
        • Document count anomalies within tables
        """
        if not self.validate_connection():
            return

        self.console.print(Panel.fit("[bold blue]Shard Distribution Analysis[/bold blue]"))

        analyzer = DistributionAnalyzer(self.client)

        if table:
            # Analyze specific table
            self.console.print(f"[dim]Analyzing table: {table}[/dim]")
            try:
                distribution = analyzer.get_table_distribution_detailed(table)
                self._display_table_distribution(distribution, table)
            except Exception as e:
                self.console.print(f"[red]Error analyzing table {table}: {e}[/red]")
        else:
            # Analyze top tables
            self.console.print(f"[dim]Analyzing top {top_tables} largest tables by storage[/dim]")
            try:
                tables_analysis = analyzer.get_largest_tables_distribution(top_tables)
                self._display_distribution_summary(tables_analysis)
            except Exception as e:
                self.console.print(f"[red]Error during distribution analysis: {e}[/red]")

    def _display_table_distribution(self, distribution, table_name: str) -> None:
        """Display distribution analysis for a single table"""
        if not distribution:
            self.console.print(f"[yellow]No distribution data found for table: {table_name}[/yellow]")
            return

        # Calculate totals from node distributions
        total_shards = sum(node['total_shards'] for node in distribution.node_distributions.values())
        total_primary_shards = sum(node['primary_shards'] for node in distribution.node_distributions.values())
        total_replica_shards = sum(node['replica_shards'] for node in distribution.node_distributions.values())
        total_size_gb = sum(node['total_size_gb'] for node in distribution.node_distributions.values())
        total_documents = sum(node['total_documents'] for node in distribution.node_distributions.values())
        node_count = len(distribution.node_distributions)

        # Table overview
        overview_table = Table(title=f"Distribution Overview: {distribution.full_table_name}", box=box.ROUNDED)
        overview_table.add_column("Metric", style="cyan")
        overview_table.add_column("Value", style="white")

        overview_table.add_row("Primary Data Size", format_size(distribution.total_primary_size_gb))
        overview_table.add_row("Total Size (with replicas)", format_size(total_size_gb))
        overview_table.add_row("Total Shards", f"{total_shards} ({total_primary_shards}P + {total_replica_shards}R)")
        overview_table.add_row("Total Documents", f"{total_documents:,}")
        overview_table.add_row("Nodes Involved", str(node_count))
        overview_table.add_row("Average Shards/Node", f"{total_shards/max(node_count, 1):.1f}")

        self.console.print(overview_table)
        self.console.print()

        # Node distribution
        node_table = Table(title="Per-Node Distribution", box=box.ROUNDED)
        node_table.add_column("Node", style="cyan")
        node_table.add_column("Primary", justify="right", style="magenta")
        node_table.add_column("Replica", justify="right", style="yellow")
        node_table.add_column("Total", justify="right", style="blue")
        node_table.add_column("Size", justify="right", style="green")
        node_table.add_column("Documents", justify="right", style="bright_blue")

        for node_name in sorted(distribution.node_distributions.keys()):
            node_data = distribution.node_distributions[node_name]

            node_table.add_row(
                node_name,
                str(node_data['primary_shards']),
                str(node_data['replica_shards']),
                str(node_data['total_shards']),
                format_size(node_data['total_size_gb']),
                f"{node_data['total_documents']:,}"
            )

        self.console.print(node_table)
        self.console.print()
        self.console.print("[green]✅ Table distribution analysis complete[/green]")

    def _display_distribution_summary(self, tables_analysis: List) -> None:
        """Display summary of distribution analysis for multiple tables"""
        if not tables_analysis:
            self.console.print("[yellow]No tables found for analysis[/yellow]")
            return

        # Group by base table name to count partitions and aggregate data
        tables_grouped = {}
        for table_dist in tables_analysis:
            # Create base table name (schema.table without partition info)
            base_name = f"{table_dist.schema_name}.{table_dist.table_name}" if table_dist.schema_name != "doc" else table_dist.table_name

            if base_name not in tables_grouped:
                tables_grouped[base_name] = {
                    'partitions': [],
                    'total_shards': 0,
                    'all_nodes': set(),
                    'total_primary_size_gb': 0.0
                }

            # Add this partition/table to the group
            tables_grouped[base_name]['partitions'].append(table_dist)
            tables_grouped[base_name]['total_shards'] += sum(node['total_shards'] for node in table_dist.node_distributions.values())
            tables_grouped[base_name]['all_nodes'].update(table_dist.node_distributions.keys())
            tables_grouped[base_name]['total_primary_size_gb'] += table_dist.total_primary_size_gb

        # Summary table
        summary_table = Table(title="Table Distribution Summary", box=box.ROUNDED)
        summary_table.add_column("Table", style="cyan")
        summary_table.add_column("Shards", justify="right", style="magenta")
        summary_table.add_column("Nodes", justify="right", style="blue")
        summary_table.add_column("Primary Size", justify="right", style="green")
        summary_table.add_column("Status", style="white")

        for base_name, group_data in tables_grouped.items():
            partition_count = len(group_data['partitions'])
            total_shards = group_data['total_shards']
            node_count = len(group_data['all_nodes'])
            total_primary_size_gb = group_data['total_primary_size_gb']

            # Create display name with partition info
            if partition_count > 1:
                display_name = f"{base_name} ({partition_count} partitions)"
            elif partition_count == 1 and group_data['partitions'][0].partition_ident:
                # Single partition (partitioned table with only one partition shown)
                display_name = f"{base_name} (partitioned)"
            else:
                # Non-partitioned table
                display_name = base_name

            # Simple status based on node count and shard distribution
            if node_count == 0:
                status = "[red]🔴 NO DATA[/red]"
            elif node_count < 2:
                status = "[yellow]🟡 SINGLE NODE[/yellow]"
            else:
                # Check for balance across all partitions - basic heuristic
                # For now, just mark as OK if distributed across multiple nodes
                status = "[green]✅ OK[/green]"

            summary_table.add_row(
                display_name,
                str(total_shards),
                str(node_count),
                format_size(total_primary_size_gb),  # Already in GB
                status
            )

        self.console.print(summary_table)

        # Overall summary
        self.console.print()
        total_tables = len(tables_grouped)
        total_partitions = len(tables_analysis)
        if total_partitions > total_tables:
            self.console.print(f"[green]✅ Analyzed {total_tables} tables ({total_partitions} partitions)[/green]")
        else:
            self.console.print(f"[green]✅ Analyzed {total_tables} largest tables[/green]")
        self.console.print("[dim]💡 Use --table <table_name> for detailed analysis of specific tables[/dim]")

    def problematic_translogs(self, sizemb: int, execute: bool, autoexec: bool = False, 
                             dry_run: bool = False, percentage: int = 200, 
                             max_wait: int = 720, log_format: str = "console") -> None:
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
        """
        if not self.validate_connection():
            return

        self.console.print(Panel.fit(f"[bold blue]Problematic Translog Analysis[/bold blue]"))
        self.console.print(f"[dim]Using adaptive thresholds based on table flush_threshold_size settings (≥ {sizemb} MB baseline)[/dim]")

        if autoexec:
            mode_desc = "DRY RUN" if dry_run else "AUTOEXEC"
            self.console.print(f"[red]🤖 {mode_desc} MODE - automatically executing replica reset operations[/red]")
        elif execute:
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
            self._display_individual_problematic_shards(individual_shards, sizemb)

            # Display summary by table
            self._display_table_summary(summary_rows)

            if autoexec:
                # Execute automatic replica reset
                success = self._execute_autoexec(summary_rows, dry_run, percentage, max_wait, log_format)
                if not success:
                    sys.exit(self._get_autoexec_exit_code())
            elif execute:
                self._generate_comprehensive_commands(individual_shards, summary_rows)
            else:
                self.console.print()
                self.console.print("[dim]💡 Use --execute flag to generate comprehensive shard management commands for display[/dim]")
                self.console.print("[dim]💡 Use --autoexec flag to automatically execute replica reset operations[/dim]")

        except Exception as e:
            self.console.print(f"[red]Error analyzing problematic translogs: {e}[/red]")
            if autoexec:
                sys.exit(1)

    def _get_problematic_translogs(self, min_size_mb: int) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Get individual shards and table summaries with problematic translog sizes using adaptive thresholds"""

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
        """Get initial problematic shards using basic threshold"""

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
        """Get flush threshold settings for tables that have problematic shards"""

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
        """Apply adaptive thresholds to filter shards"""

        adaptive_shards = []
        adaptive_summary = []

        # Filter individual shards using adaptive thresholds
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

            # Keep shard if it exceeds the adaptive threshold
            if translog_mb > threshold_mb:
                shard['adaptive_config_mb'] = config_mb
                shard['adaptive_threshold_mb'] = threshold_mb
                adaptive_shards.append(shard)

        # Filter summary data - only keep tables that still have problematic shards after adaptive filtering
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

    def _display_individual_problematic_shards(self, individual_shards: List[Dict[str, Any]], min_size_mb: int) -> None:
        """Display individual problematic shards for REROUTE CANCEL commands"""
        self.console.print(f"[bold]Problematic Replica Shards (adaptive thresholds)[/bold]")

        # Display threshold information
        if individual_shards and any(shard.get('adaptive_threshold_mb') for shard in individual_shards):
            self.console.print("[dim]Threshold Analysis:[/dim]")
            unique_thresholds = {}
            for shard in individual_shards:
                schema = shard['schema_name']
                table = shard['table_name']
                partition = shard.get('partition_values', '')
                config_mb = shard.get('adaptive_config_mb', min_size_mb)
                threshold_mb = shard.get('adaptive_threshold_mb', min_size_mb)
                
                if partition:
                    key = f"{schema}.{table} {partition}"
                else:
                    key = f"{schema}.{table}"
                unique_thresholds[key] = (config_mb, threshold_mb)

            for table_key, (config_mb, threshold_mb) in sorted(unique_thresholds.items()):
                self.console.print(f"[dim]├─ {table_key}: {config_mb:.0f}MB/{threshold_mb:.0f}MB config/threshold[/dim]")
            self.console.print()

        individual_table = Table(box=box.ROUNDED)
        individual_table.add_column("Schema", style="cyan")
        individual_table.add_column("Table", style="blue")
        individual_table.add_column("Partition", style="magenta")
        individual_table.add_column("Shard ID", justify="right", style="yellow")
        individual_table.add_column("Node", style="green")
        individual_table.add_column("Translog MB", justify="right", style="red")
        individual_table.add_column("Threshold MB", justify="right", style="dim")

        for shard in individual_shards:
            schema_name = shard['schema_name']
            table_name = shard['table_name']
            partition_values = shard.get('partition_values', '')
            shard_id = shard['shard_id']
            node_name = shard['node_name']
            translog_mb = shard['translog_size_mb']
            threshold_mb = shard.get('adaptive_threshold_mb', min_size_mb)

            # Format partition values for display
            partition_display = partition_values if partition_values else 'N/A'

            individual_table.add_row(
                schema_name,
                table_name,
                partition_display,
                str(shard_id),
                node_name,
                f"{translog_mb:.1f}",
                f"{threshold_mb:.0f}"
            )

        self.console.print(individual_table)
        self.console.print()

    def _display_table_summary(self, summary_rows: List[Dict[str, Any]]) -> None:
        """Display summary of tables with problematic translogs"""
        self.console.print(f"Found {len(summary_rows)} table/partition(s) with problematic translogs:")
        self.console.print()

        # Display summary table
        results_table = Table(title=f"Tables with Problematic Replicas", box=box.ROUNDED)
        results_table.add_column("Schema", style="cyan")
        results_table.add_column("Table", style="blue")
        results_table.add_column("Partition", style="magenta")
        results_table.add_column("Problematic Replicas", justify="right", style="yellow")
        results_table.add_column("Max Translog MB", justify="right", style="red")
        results_table.add_column("Shards (P/R)", justify="right", style="blue")
        results_table.add_column("Size GB (P/R)", justify="right", style="bright_blue")
        results_table.add_column("Current Replicas", justify="right", style="green")

        for row in summary_rows:
            schema_name = row['schema_name']
            table_name = row['table_name']
            partition_values = row['partition_values']
            problematic_replica_shards = row['problematic_replica_shards']
            max_translog_mb = row['max_translog_uncommitted_mb']
            total_primary_shards = row['total_primary_shards']
            total_replica_shards = row['total_replica_shards']
            total_primary_size_gb = row['total_primary_size_gb']
            total_replica_size_gb = row['total_replica_size_gb']

            partition_display = partition_values if partition_values and partition_values != 'NULL' else "[dim]none[/dim]"

            # Look up current replica count
            partition_ident = row.get('partition_ident')
            current_replicas = self._get_current_replica_count(schema_name, table_name, partition_ident, partition_values)
            if current_replicas == "unknown":
                current_replicas = "?"

            results_table.add_row(
                schema_name,
                table_name,
                partition_display,
                str(problematic_replica_shards),
                f"{max_translog_mb:.1f}",
                f"{total_primary_shards}P/{total_replica_shards}R",
                f"{total_primary_size_gb:.1f}/{total_replica_size_gb:.1f}",
                str(current_replicas)
            )

        self.console.print(results_table)
        self.console.print()

    def _generate_comprehensive_commands(self, individual_shards: List[Dict[str, Any]], summary_rows: List[Dict[str, Any]]) -> None:
        """Generate comprehensive shard management commands with full 6-step process, grouped by table/partition"""
        self.console.print()
        self.console.print("[bold]Generated Comprehensive Shard Management Commands:[/bold]")
        self.console.print()

        # Prepare table info with current replica counts
        valid_table_info = []
        for row in summary_rows:
            schema_name = row['schema_name']
            table_name = row['table_name']
            partition_values = row['partition_values']
            partition_ident = row['partition_ident']

            # Look up current replica count
            current_replicas = self._get_current_replica_count(schema_name, table_name, partition_ident, partition_values)

            if current_replicas == "unknown" or current_replicas == 0:
                continue

            # Add current replicas to the row data for later use
            row['current_replicas'] = current_replicas
            valid_table_info.append(row)

        # 1. Stop automatic shard rebalancing
        self.console.print("[bold cyan]1. Stop Automatic Shard Rebalancing:[/bold cyan]")
        rebalance_disable_cmd = 'SET GLOBAL PERSISTENT "cluster.routing.rebalance.enable"=\'none\';'
        self.console.print(rebalance_disable_cmd)
        self.console.print()

        # 2. Generate REROUTE CANCEL SHARD commands for individual shards
        self.console.print("[bold cyan]2. REROUTE CANCEL Commands:[/bold cyan]")
        self.console.print("[yellow]⚠️  Note: CANCEL SHARDS is deprecated on certain CrateDB cluster versions[/yellow]")
        reroute_commands = []
        for shard in individual_shards:
            schema_name = shard['schema_name']
            table_name = shard['table_name']
            partition_values = shard.get('partition_values')
            shard_id = shard['shard_id']
            node_name = shard['node_name']

            # Include partition clause if this is a partitioned table
            partition_clause = f' PARTITION {partition_values}' if partition_values else ''
            cmd = f'ALTER TABLE "{schema_name}"."{table_name}"{partition_clause} REROUTE CANCEL SHARD {shard_id} on \'{node_name}\' WITH (allow_primary=False);'
            reroute_commands.append(cmd)
            self.console.print(cmd)
        self.console.print()

        # Group remaining commands by table/partition for convenience
        for row in valid_table_info:
            schema_name = row['schema_name']
            table_name = row['table_name']
            partition_values = row['partition_values']
            partition_ident = row['partition_ident']
            current_replicas = row['current_replicas']

            table_display = f"{schema_name}.{table_name}"
            if partition_values and partition_values != 'NULL':
                table_display += f" PARTITION {partition_values}"

            self.console.print(f"[bold green]-- For {table_display}:[/bold green]")
            self.console.print()

            # 3. Set replicas to 0
            self.console.print("[dim]3. Set replicas to 0:[/dim]")
            if partition_values and partition_values != 'NULL':
                cmd_set_zero = f'ALTER TABLE "{schema_name}"."{table_name}" PARTITION {partition_values} SET ("number_of_replicas" = 0);'
            else:
                cmd_set_zero = f'ALTER TABLE "{schema_name}"."{table_name}" SET ("number_of_replicas" = 0);'
            self.console.print(cmd_set_zero)
            self.console.print()

            # 4. Retention lease monitoring query
            self.console.print("[dim]4. Monitor retention leases:[/dim]")
            if partition_values and partition_values != 'NULL':
                retention_query = f"""SELECT array_length(retention_leases['leases'], 1) as cnt_leases, id
FROM sys.shards
WHERE table_name = '{table_name}'
  AND schema_name = '{schema_name}'
  AND partition_ident = '{partition_ident}'
ORDER BY array_length(retention_leases['leases'], 1);"""
            else:
                retention_query = f"""SELECT array_length(retention_leases['leases'], 1) as cnt_leases, id
FROM sys.shards
WHERE table_name = '{table_name}'
  AND schema_name = '{schema_name}'
ORDER BY array_length(retention_leases['leases'], 1);"""
            self.console.print(retention_query)
            self.console.print()

            # 5. Restore replicas to original values
            self.console.print("[dim]5. Restore replicas to original value:[/dim]")
            if partition_values and partition_values != 'NULL':
                cmd_restore = f'ALTER TABLE "{schema_name}"."{table_name}" PARTITION {partition_values} SET ("number_of_replicas" = {current_replicas});'
            else:
                cmd_restore = f'ALTER TABLE "{schema_name}"."{table_name}" SET ("number_of_replicas" = {current_replicas});'
            self.console.print(cmd_restore)
            self.console.print()
            self.console.print("[dim]" + "─" * 80 + "[/dim]")  # Visual separator between tables
            self.console.print()

        # 6. Re-enable automatic shard rebalancing
        self.console.print("[bold cyan]6. Re-enable Automatic Shard Rebalancing:[/bold cyan]")
        rebalance_enable_cmd = 'SET GLOBAL PERSISTENT "cluster.routing.rebalance.enable"=\'all\';'
        self.console.print(rebalance_enable_cmd)
        self.console.print()

        # Summary
        self.console.print(f"[bold]Total Commands:[/bold]")
        self.console.print(f"  • 1 rebalancing disable command")
        self.console.print(f"  • {len(reroute_commands)} REROUTE CANCEL commands")
        self.console.print(f"  • {len(valid_table_info)} set replicas to 0 commands")
        self.console.print(f"  • {len(valid_table_info)} retention lease queries (for monitoring)")
        self.console.print(f"  • {len(valid_table_info)} restore replicas commands")
        self.console.print(f"  • 1 rebalancing enable command")

    def _execute_autoexec(self, summary_rows: List[Dict[str, Any]], dry_run: bool, 
                         percentage: int, max_wait: int, log_format: str) -> bool:
        """Execute automatic replica reset for all problematic tables"""
        
        # Filter tables based on percentage threshold
        filtered_tables = self._filter_tables_by_percentage(summary_rows, percentage)
        
        if not filtered_tables:
            self.console.print(f"[green]✅ No tables exceed {percentage}% of their threshold[/green]")
            self._autoexec_exit_code = 0
            return True
        
        self.console.print(f"[yellow]Processing {len(filtered_tables)} table(s) exceeding {percentage}% threshold[/yellow]")
        
        # Setup JSON logging if requested
        if log_format == "json":
            # Configure loguru for JSON output
            logger.remove()  # Remove default handler
            logger.add(
                sys.stderr,
                format="{time:YYYY-MM-DDTHH:mm:ss.sssZ} | {level} | {message}",
                serialize=True,  # Enable JSON serialization
                level="INFO"
            )
        
        success_count = 0
        failure_count = 0
        failed_tables = []
        
        start_time = time.time()
        
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
    
    def _filter_tables_by_percentage(self, summary_rows: List[Dict[str, Any]], percentage: int) -> List[Dict[str, Any]]:
        """Filter tables that exceed the percentage threshold"""
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
                    current_replicas = self._get_current_replica_count(schema, table, partition_ident, partition_values)
                    table_info['current_replicas'] = current_replicas
                    filtered.append(table_info)
        
        return filtered
    
    def _get_autoexec_exit_code(self) -> int:
        """Get the appropriate exit code for autoexec operations"""
        return getattr(self, '_autoexec_exit_code', 1)

    def _get_current_replica_count(self, schema_name: str, table_name: str, partition_ident: Optional[str] = None, partition_values: Optional[str] = None) -> str:
        """Look up current replica count for table or partition"""
        try:
            if partition_values and partition_values != 'NULL':
                # Partitioned table query
                replica_query = """
                    SELECT number_of_replicas
                    FROM information_schema.table_partitions
                    WHERE table_name = ? AND table_schema = ? AND partition_ident = ?
                """
                replica_result = self.client.execute_query(replica_query, [table_name, schema_name, partition_ident])
            else:
                # Non-partitioned table query
                replica_query = """
                    SELECT number_of_replicas
                    FROM information_schema.tables
                    WHERE table_name = ? AND table_schema = ?
                """
                replica_result = self.client.execute_query(replica_query, [table_name, schema_name])

            replica_rows = replica_result.get('rows', [])
            if replica_rows and replica_rows[0] and replica_rows[0][0] is not None:
                replica_value = replica_rows[0][0]
                # CrateDB returns replica counts as strings like "0-1", we need to parse the range
                if isinstance(replica_value, str):
                    if '-' in replica_value:
                        # Handle range format like "0-1"
                        parts = replica_value.split('-')
                        try:
                            return int(parts[-1])  # Return the max value
                        except (ValueError, IndexError):
                            return replica_value  # Return as-is if parsing fails
                    else:
                        try:
                            return int(replica_value)
                        except ValueError:
                            return replica_value
                elif isinstance(replica_value, (int, float)):
                    return int(replica_value)
                else:
                    return replica_value
            else:
                return "unknown"
        except Exception as e:
            self.console.print(f"[yellow]Warning: Could not retrieve replica count for {schema_name}.{table_name}: {e}[/yellow]")
            return "unknown"

    def check_maintenance(self, node: str, min_availability: str, short: bool = False):
        """Check whether a node could be decommissioned and analyze shard movement requirements

        Args:
            node: Target node to analyze for decommissioning
            min_availability: Minimum availability level - 'full' (move all shards) or 'primaries' (move only primaries without replicas)
            short: Display only essential information without detailed tables and recommendations
        """
        if not self.validate_connection():
            return

        # Get cluster name for display
        cluster_name = self.client.get_cluster_name()

        if not short:
            cluster_display = cluster_name or "Unknown"
            self.print_header(f"Pre-Flight Check {cluster_display}: {node}", f"Min-availability: {min_availability.title()}")

        try:
            # Get cluster recovery settings
            recovery_settings = self._get_cluster_recovery_settings()

            # Get node information and validate node exists
            nodes_info = self.client.get_nodes_info()
            target_node = None
            for n in nodes_info:
                if n.name == node:
                    target_node = n
                    break

            if not target_node:
                self.console.print(f"[red]❌ Node '{node}' not found in cluster[/red]")
                available_nodes = [n.name for n in nodes_info]
                self.console.print(f"Available nodes: {', '.join(available_nodes)}")
                return

            # Get all shards on the target node
            target_shards = self._get_node_shards(node)
            if not target_shards:
                self.console.print(f"[green]✅ Node '{node}' has no shards - safe to decommission[/green]")
                return

            # Analyze shards based on min-availability level
            if min_availability == "full":
                analysis = self._analyze_full_maintenance(target_shards, nodes_info, target_node)
            else:  # primaries
                analysis = self._analyze_primaries_maintenance(target_shards, nodes_info, target_node)

            # Display results
            if short:
                self._display_short_maintenance_analysis(analysis, recovery_settings, cluster_name)
            else:
                self._display_maintenance_analysis(analysis, recovery_settings, target_node)

        except Exception as e:
            self.handle_error(e, "analyzing node maintenance requirements")

    def _get_cluster_recovery_settings(self) -> dict:
        """Get cluster recovery settings and max shards per node from sys.cluster"""
        try:
            # Query recovery settings and max shards per node
            recovery_query = """
            SELECT
                settings['indices']['recovery']['max_bytes_per_sec'] as max_bytes_per_sec,
                settings['cluster']['routing']['allocation']['node_concurrent_recoveries'] as node_concurrent_recoveries,
                settings['cluster']['max_shards_per_node'] as max_shards_per_node
            FROM sys.cluster
            """

            result = self.client.execute_query(recovery_query)
            if result.get('rows'):
                row = result['rows'][0]
                max_bytes_per_sec = row[0] or "20mb"  # CrateDB default
                node_concurrent_recoveries = row[1] or 2  # CrateDB default
                max_shards_per_node = row[2] or 1000  # CrateDB default

                # Parse max_bytes_per_sec (could be "20mb", "100mb", etc.)
                if isinstance(max_bytes_per_sec, str):
                    if max_bytes_per_sec.lower().endswith('mb'):
                        bytes_per_sec = int(max_bytes_per_sec[:-2]) * 1024 * 1024
                    elif max_bytes_per_sec.lower().endswith('gb'):
                        bytes_per_sec = int(max_bytes_per_sec[:-2]) * 1024 * 1024 * 1024
                    else:
                        bytes_per_sec = int(max_bytes_per_sec)
                else:
                    bytes_per_sec = int(max_bytes_per_sec)

                return {
                    'max_bytes_per_sec': bytes_per_sec,
                    'node_concurrent_recoveries': int(node_concurrent_recoveries),
                    'max_shards_per_node': int(max_shards_per_node)
                }
        except Exception:
            pass

        # Return defaults if query fails
        return {
            'max_bytes_per_sec': 20 * 1024 * 1024,  # 20MB default
            'node_concurrent_recoveries': 2,  # Default
            'max_shards_per_node': 1000  # CrateDB default
        }

    def _get_node_shards(self, node_name: str) -> list:
        """Get all shards on a specific node with retention lease information"""
        query = """
        SELECT
            s.schema_name,
            s.table_name,
            s.partition_ident,
            s.id as shard_id,
            s."primary" as is_primary,
            s.size / 1024.0^3 as size_gb,
            s.retention_leases,
            n.attributes['zone'] as zone,
            s.state,
            s.routing_state
        FROM sys.shards s
        JOIN sys.nodes n ON s.node['id'] = n.id
        WHERE COALESCE(s.node['name'], 'unknown-' || COALESCE(s.node['id'], 'corrupted')) = ?
            AND s.routing_state = 'STARTED'
        ORDER BY s.size DESC
        """

        result = self.client.execute_query(query, [node_name])
        shards = []

        for row in result.get('rows', []):
            schema_name, table_name, partition_ident, shard_id, is_primary, size_gb, retention_leases, zone, state, routing_state = row

            # Count replicas from retention_leases
            replica_count = 0
            if retention_leases and isinstance(retention_leases, dict):
                leases = retention_leases.get('leases', [])
                replica_count = len(leases) if leases else 0

            shards.append({
                'schema_name': schema_name,
                'table_name': table_name,
                'partition_ident': partition_ident,
                'shard_id': shard_id,
                'is_primary': is_primary,
                'size_gb': size_gb,
                'replica_count': replica_count,
                'has_replicas': replica_count > 1,  # More than 1 lease indicates replicas
                'zone': zone
            })

        return shards

    def _get_node_shard_count(self, node_name: str) -> int:
        """Get current shard count for a specific node"""
        query = """
        SELECT COUNT(*) as shard_count
        FROM sys.shards s
        WHERE COALESCE(s.node['name'], 'unknown-' || COALESCE(s.node['id'], 'corrupted')) = ?
            AND s.routing_state = 'STARTED'
        """

        try:
            result = self.client.execute_query(query, [node_name])
            if result.get('rows'):
                count = result['rows'][0][0]
                return int(count)  # Ensure it's always returned as int
            return 0
        except Exception:
            return 0

    def _analyze_full_maintenance(self, target_shards: list, nodes_info: list, target_node) -> dict:
        """Analyze requirements for full node maintenance (move all shards)"""
        from ..utils import calculate_watermark_remaining_space

        # Get cluster watermark config and recovery settings (includes max_shards_per_node)
        watermark_config = self.client.get_cluster_watermark_config()
        recovery_settings = self._get_cluster_recovery_settings()
        max_shards_per_node = recovery_settings['max_shards_per_node']

        # Calculate target nodes capacity (same AZ)
        target_zone = target_node.zone
        candidate_nodes = []

        for node in nodes_info:
            if node.name != target_node.name and node.zone == target_zone and not node.name.startswith('master-'):
                watermark_info = calculate_watermark_remaining_space(node.fs_total, node.fs_used, watermark_config)

                # Get current shard count on this node
                current_shards = self._get_node_shard_count(node.name)
                remaining_shard_capacity = int(max_shards_per_node) - current_shards

                candidate_nodes.append({
                    'name': node.name,
                    'zone': node.zone,
                    'remaining_capacity_gb': watermark_info['remaining_to_low_gb'],
                    'disk_usage_percent': node.disk_usage_percent,
                    'available_below_watermark_gb': watermark_info['remaining_to_low_gb'],
                    'current_shards': current_shards,
                    'remaining_shard_capacity': remaining_shard_capacity,
                    'max_shards_per_node': int(max_shards_per_node)
                })

        # Sort by available capacity
        candidate_nodes.sort(key=lambda x: x['remaining_capacity_gb'], reverse=True)

        # Categorize shards
        primary_shards = [s for s in target_shards if s['is_primary']]
        replica_shards = [s for s in target_shards if not s['is_primary']]

        primary_without_replicas = [s for s in primary_shards if not s['has_replicas']]
        primary_with_replicas = [s for s in primary_shards if s['has_replicas']]

        # Calculate totals
        total_data_to_move = sum(s['size_gb'] for s in target_shards)
        total_available_capacity = sum(node['remaining_capacity_gb'] for node in candidate_nodes)

        # Check if there's sufficient shard capacity across all candidate nodes
        total_shard_capacity = sum(node['remaining_shard_capacity'] for node in candidate_nodes)
        shards_sufficient = total_shard_capacity >= len(target_shards)

        return {
            'min_availability': 'full',
            'target_node': target_node.name,
            'target_zone': target_zone,
            'candidate_nodes': candidate_nodes,
            'total_shards': len(target_shards),
            'primary_shards': len(primary_shards),
            'replica_shards': len(replica_shards),
            'primary_without_replicas': primary_without_replicas,
            'primary_with_replicas': primary_with_replicas,
            'total_data_to_move_gb': total_data_to_move,
            'total_available_capacity_gb': total_available_capacity,
            'capacity_sufficient': total_available_capacity >= total_data_to_move and shards_sufficient,
            'all_shards': target_shards,
            'total_shard_capacity': total_shard_capacity,
            'shards_sufficient': shards_sufficient
        }

    def _analyze_primaries_maintenance(self, target_shards: list, nodes_info: list, target_node) -> dict:
        """Analyze requirements for primaries-only maintenance"""
        from ..utils import calculate_watermark_remaining_space

        # Get cluster watermark config and recovery settings (includes max_shards_per_node)
        watermark_config = self.client.get_cluster_watermark_config()
        recovery_settings = self._get_cluster_recovery_settings()
        max_shards_per_node = recovery_settings['max_shards_per_node']

        # Calculate target nodes capacity (same AZ)
        target_zone = target_node.zone
        candidate_nodes = []

        for node in nodes_info:
            if node.name != target_node.name and node.zone == target_zone and not node.name.startswith('master-'):
                watermark_info = calculate_watermark_remaining_space(node.fs_total, node.fs_used, watermark_config)

                # Get current shard count on this node
                current_shards = self._get_node_shard_count(node.name)
                remaining_shard_capacity = int(max_shards_per_node) - current_shards

                candidate_nodes.append({
                    'name': node.name,
                    'zone': node.zone,
                    'remaining_capacity_gb': watermark_info['remaining_to_low_gb'],
                    'disk_usage_percent': node.disk_usage_percent,
                    'available_below_watermark_gb': watermark_info['remaining_to_low_gb'],
                    'current_shards': current_shards,
                    'remaining_shard_capacity': remaining_shard_capacity,
                    'max_shards_per_node': int(max_shards_per_node)
                })

        # Sort by available capacity
        candidate_nodes.sort(key=lambda x: x['remaining_capacity_gb'], reverse=True)

        # Categorize primary shards only
        primary_shards = [s for s in target_shards if s['is_primary']]
        replica_shards = [s for s in target_shards if not s['is_primary']]

        primary_without_replicas = [s for s in primary_shards if not s['has_replicas']]
        primary_with_replicas = [s for s in primary_shards if s['has_replicas']]

        # For primaries maintenance, only primaries without replicas need to be moved
        # Primaries with replicas can be demoted (fast operation)
        data_to_move_gb = sum(s['size_gb'] for s in primary_without_replicas)
        total_available_capacity = sum(node['remaining_capacity_gb'] for node in candidate_nodes)

        # Check if there's sufficient shard capacity for primaries that need to be moved
        total_shard_capacity = sum(node['remaining_shard_capacity'] for node in candidate_nodes)
        shards_sufficient = total_shard_capacity >= len(primary_without_replicas)

        return {
            'min_availability': 'primaries',
            'target_node': target_node.name,
            'target_zone': target_zone,
            'candidate_nodes': candidate_nodes,
            'total_shards': len(target_shards),
            'primary_shards': len(primary_shards),
            'replica_shards': len(replica_shards),
            'primary_without_replicas': primary_without_replicas,
            'primary_with_replicas': primary_with_replicas,
            'data_to_move_gb': data_to_move_gb,  # Only primaries without replicas
            'total_available_capacity_gb': total_available_capacity,
            'capacity_sufficient': total_available_capacity >= data_to_move_gb and shards_sufficient,
            'fast_operations': len(primary_with_replicas),  # Primary->replica conversions
            'slow_operations': len(primary_without_replicas),  # Actual data moves
            'all_shards': target_shards,
            'total_shard_capacity': total_shard_capacity,
            'shards_sufficient': shards_sufficient
        }

    def _display_maintenance_analysis(self, analysis: dict, recovery_settings: dict, target_node):
        """Display the maintenance analysis results"""
        from ..utils import format_size

        # Summary panel
        summary_lines = [
            f"[bold]Target Node:[/bold] {analysis['target_node']} (Zone: {analysis['target_zone']})",
            f"[bold]Min-availability:[/bold] {analysis['min_availability'].title()}",
            f"[bold]Total Shards on Node:[/bold] {analysis['total_shards']} ({analysis['primary_shards']} primaries, {analysis['replica_shards']} replicas)"
        ]

        if analysis['min_availability'] == 'full':
            summary_lines.extend([
                f"[bold]Data to Move:[/bold] {format_size(analysis['total_data_to_move_gb'])}",
                f"[bold]Available Capacity:[/bold] {format_size(analysis['total_available_capacity_gb'])}",
                f"[bold]Capacity Check:[/bold] {'✅ Sufficient' if analysis['capacity_sufficient'] else '❌ Insufficient'}"
            ])
        else:  # primaries
            summary_lines.extend([
                f"[bold]Fast Operations:[/bold] {analysis['fast_operations']} (primary→replica conversions)",
                f"[bold]Slow Operations:[/bold] {analysis['slow_operations']} (data moves)",
                f"[bold]Data to Move:[/bold] {format_size(analysis['data_to_move_gb'])}",
                f"[bold]Available Capacity:[/bold] {format_size(analysis['total_available_capacity_gb'])}",
                f"[bold]Capacity Check:[/bold] {'✅ Sufficient' if analysis['capacity_sufficient'] else '❌ Insufficient'}"
            ])

        self.console.print(Panel("\n".join(summary_lines), title="📊 Maintenance Analysis Summary", border_style="blue"))
        self.console.print()

        # Shard breakdown table
        from rich.table import Table
        from rich import box

        shard_table = Table(title="Shard Analysis by Type", box=box.ROUNDED)
        shard_table.add_column("Shard Type", style="cyan")
        shard_table.add_column("Count", justify="right")
        shard_table.add_column("Total Size", justify="right")
        shard_table.add_column("Action Required")

        if analysis['min_availability'] == 'full':
            shard_table.add_row(
                "Primary Shards (with replicas)",
                str(len(analysis['primary_with_replicas'])),
                format_size(sum(s['size_gb'] for s in analysis['primary_with_replicas'])),
                "Move data"
            )
            shard_table.add_row(
                "Primary Shards (without replicas)",
                str(len(analysis['primary_without_replicas'])),
                format_size(sum(s['size_gb'] for s in analysis['primary_without_replicas'])),
                "Move data"
            )
            shard_table.add_row(
                "Replica Shards",
                str(analysis['replica_shards']),
                format_size(sum(s['size_gb'] for s in [s for s in analysis['all_shards'] if not s['is_primary']])),
                "Move data"
            )
        else:  # primaries
            shard_table.add_row(
                "Primary Shards (with replicas)",
                str(len(analysis['primary_with_replicas'])),
                format_size(sum(s['size_gb'] for s in analysis['primary_with_replicas'])),
                "Convert to replica (fast)"
            )
            shard_table.add_row(
                "Primary Shards (without replicas)",
                str(len(analysis['primary_without_replicas'])),
                format_size(sum(s['size_gb'] for s in analysis['primary_without_replicas'])),
                "Move data (slow)"
            )
            replica_size = sum(s['size_gb'] for s in analysis['all_shards'] if not s['is_primary'])
            shard_table.add_row(
                "Replica Shards",
                str(analysis['replica_shards']),
                format_size(replica_size),
                "No action needed"
            )

        self.console.print(shard_table)
        self.console.print()

        # Target nodes capacity table
        if analysis['candidate_nodes']:
            capacity_table = Table(title=f"Target Nodes Capacity (Zone: {analysis['target_zone']})", box=box.ROUNDED)
            capacity_table.add_column("Node", style="cyan")
            capacity_table.add_column("Space Below Low WM", justify="right")
            capacity_table.add_column("Shard Capacity", justify="right")
            capacity_table.add_column("Disk Usage", justify="right")
            capacity_table.add_column("Status")

            for node in analysis['candidate_nodes']:
                # Check both space and shard capacity constraints
                space_ok = node['remaining_capacity_gb'] > 0
                shards_ok = node['remaining_shard_capacity'] > 0
                disk_high = node['disk_usage_percent'] > 90

                if space_ok and shards_ok and not disk_high:
                    status = "✅ Available"
                elif not space_ok:
                    status = "❌ No space"
                elif not shards_ok:
                    status = "❌ Max shards"
                elif disk_high:
                    status = "⚠️ High usage"
                else:
                    status = "❌ At capacity"

                # Format shard capacity display
                shard_display = f"{node['remaining_shard_capacity']} / {node['max_shards_per_node']}"

                capacity_table.add_row(
                    node['name'],
                    format_size(node['available_below_watermark_gb']),
                    shard_display,
                    f"{node['disk_usage_percent']:.1f}%",
                    status
                )

            self.console.print(capacity_table)
            self.console.print()
        else:
            self.console.print("[red]❌ CRITICAL: Data cannot be moved - no target nodes in same availability zone[/red]")
            self.console.print("[yellow]  • Target node is isolated in zone '{}'[/yellow]".format(analysis['target_zone']))
            self.console.print("[yellow]  • CrateDB requires data movement within the same availability zone[/yellow]")
            self.console.print("[yellow]  • Consider adding nodes to this zone or adjusting zone configuration[/yellow]")
            self.console.print()

        # Time estimation
        self._display_recovery_time_estimation(analysis, recovery_settings)

        # Recommendations
        self._display_maintenance_recommendations(analysis)

    def _display_recovery_time_estimation(self, analysis: dict, recovery_settings: dict):
        """Display estimated recovery time based on cluster settings"""
        from ..utils import format_size

        max_bytes_per_sec = recovery_settings['max_bytes_per_sec']
        concurrent_recoveries = recovery_settings['node_concurrent_recoveries']

        if analysis['min_availability'] == 'full':
            total_bytes = analysis['total_data_to_move_gb'] * 1024**3
            estimated_seconds = total_bytes / max_bytes_per_sec
        else:  # primaries
            total_bytes = analysis['data_to_move_gb'] * 1024**3
            estimated_seconds = total_bytes / max_bytes_per_sec

        # Convert to human readable time
        hours = int(estimated_seconds // 3600)
        minutes = int((estimated_seconds % 3600) // 60)

        # Format throughput display (fix units)
        throughput_mb_per_sec = max_bytes_per_sec / (1024 * 1024)

        time_lines = [
            f"[bold]Recovery Settings:[/bold]",
            f"  • Max bytes/sec: {throughput_mb_per_sec:.0f}MB/sec",
            f"  • Concurrent recoveries: {concurrent_recoveries}",
            f"",
            f"[bold]Estimated Time:[/bold] {hours}h {minutes}m"
        ]

        if analysis['min_availability'] == 'primaries' and analysis['fast_operations'] > 0:
            time_lines.extend([
                f"",
                f"[dim]Note: {analysis['fast_operations']} primary→replica conversions are fast (seconds)[/dim]",
                f"[dim]Time estimate only applies to {analysis['slow_operations']} data moves[/dim]"
            ])

        self.console.print(Panel("\n".join(time_lines), title="⏱️ Recovery Time Estimation", border_style="green"))
        self.console.print()

    def _display_maintenance_recommendations(self, analysis: dict):
        """Display maintenance recommendations"""
        recommendations = []

        if not analysis['capacity_sufficient']:
            # Check if it's a space issue or shard count issue
            space_sufficient = analysis['total_available_capacity_gb'] >= analysis.get('total_data_to_move_gb', analysis.get('data_to_move_gb', 0))
            shards_sufficient = analysis.get('shards_sufficient', True)

            recommendations.extend([
                "[red]❌ CRITICAL: Insufficient capacity in target zone[/red]"
            ])

            if not space_sufficient:
                recommendations.append(f"  • Need {analysis.get('total_data_to_move_gb', analysis.get('data_to_move_gb', 0)):.1f}GB but only {analysis['total_available_capacity_gb']:.1f}GB available")

            if not shards_sufficient:
                total_shards_needed = len(analysis.get('all_shards', [])) if analysis['min_availability'] == 'full' else len(analysis.get('primary_without_replicas', []))
                recommendations.append(f"  • Need capacity for {total_shards_needed} shards but only {analysis.get('total_shard_capacity', 0)} shard slots available")

            recommendations.extend([
                "  • Consider adding nodes or freeing space before maintenance",
                ""
            ])

        if len(analysis['candidate_nodes']) == 0:
            recommendations.extend([
                "[red]❌ CRITICAL: Node is isolated in its availability zone[/red]",
                f"  • No other nodes available in zone '{analysis['target_zone']}'",
                "  • Data movement is impossible due to zone constraints",
                "  • Solutions:",
                "    - Add nodes to the same availability zone",
                "    - Reconfigure zone allocation if appropriate for your setup",
                "    - Consider cross-zone data movement (requires cluster configuration changes)",
                ""
            ])
        elif len(analysis['candidate_nodes']) < 2:
            recommendations.extend([
                "[yellow]⚠️  Warning: Limited target nodes in availability zone[/yellow]",
                f"  • Only {len(analysis['candidate_nodes'])} candidate node(s) available",
                "  • Consider maintenance window timing to avoid single points of failure",
                ""
            ])

        if analysis['min_availability'] == 'primaries':
            if analysis['fast_operations'] > 0:
                recommendations.extend([
                    f"[green]✅ {analysis['fast_operations']} primary shards can be quickly converted to replicas[/green]",
                    "  • These operations complete in seconds",
                    ""
                ])

            if analysis['slow_operations'] > 0:
                recommendations.extend([
                    f"[yellow]⚠️  {analysis['slow_operations']} primary shards need data movement[/yellow]",
                    "  • These require full shard recovery and take significant time",
                    "  • Consider adding replicas before maintenance to reduce this number",
                    ""
                ])

        recommendations.extend([
            "[bold]Next Steps:[/bold]",
            "1. Verify cluster health before starting maintenance",
            "2. Consider maintenance window timing for minimal impact",
            "3. Monitor recovery progress during maintenance",
            "4. Use: [cyan]xmover monitor-recovery --watch[/cyan] during operations"
        ])

        if analysis['capacity_sufficient']:
            status_color = "green"
            status_title = "✅ Maintenance Feasible"
        else:
            status_color = "red"
            status_title = "❌ Maintenance Blocked"

        self.console.print(Panel("\n".join(recommendations), title=status_title, border_style=status_color))

    def _display_short_maintenance_analysis(self, analysis: dict, recovery_settings: dict, cluster_name: str = None):
        """Display compact maintenance analysis with only essential information"""
        from ..utils import format_size

        # Calculate time estimation
        max_bytes_per_sec = recovery_settings['max_bytes_per_sec']
        throughput_mb_per_sec = max_bytes_per_sec / (1024 * 1024)

        if analysis['min_availability'] == 'full':
            total_bytes = analysis['total_data_to_move_gb'] * 1024**3
            data_to_move = analysis['total_data_to_move_gb']
        else:  # primaries
            total_bytes = analysis['data_to_move_gb'] * 1024**3
            data_to_move = analysis['data_to_move_gb']

        estimated_seconds = total_bytes / max_bytes_per_sec
        hours = int(estimated_seconds // 3600)
        minutes = int((estimated_seconds % 3600) // 60)
        seconds = int(estimated_seconds % 60)

        # Build shard summary
        if analysis['min_availability'] == 'full':
            shard_summary = f"{analysis['total_shards']} total ({analysis['primary_shards']} primaries, {analysis['replica_shards']} replicas)"
        else:  # primaries
            fast_ops = analysis['fast_operations']
            slow_ops = analysis['slow_operations']
            replica_count = analysis['replica_shards']
            if fast_ops > 0:
                shard_summary = f"{analysis['primary_shards']} primaries ({slow_ops} move, {fast_ops} fast-convert), {replica_count} replicas (no action)"
            else:
                shard_summary = f"{analysis['primary_shards']} primaries, {replica_count} replicas (no action)"

        # Target nodes summary
        if len(analysis['candidate_nodes']) == 0:
            target_summary = "No nodes available (zone isolated)"
            status_icon = "❌"
            status_text = "BLOCKED - Zone Isolation"
        elif not analysis['capacity_sufficient']:
            available_count = len([n for n in analysis['candidate_nodes']
                                 if n['remaining_capacity_gb'] > 0 and n['remaining_shard_capacity'] > 0])
            at_capacity_count = len(analysis['candidate_nodes']) - available_count
            target_summary = f"{available_count} available, {at_capacity_count} at capacity"
            status_icon = "❌"
            status_text = "BLOCKED - Insufficient Capacity"
        else:
            available_count = len([n for n in analysis['candidate_nodes']
                                 if n['remaining_capacity_gb'] > 0 and n['remaining_shard_capacity'] > 0])
            at_capacity_count = len(analysis['candidate_nodes']) - available_count
            if at_capacity_count > 0:
                target_summary = f"{available_count} available, {at_capacity_count} at capacity"
            else:
                target_summary = f"{available_count} available"
            status_icon = "✅"
            status_text = "Feasible"

        # Display compact summary with cluster name
        cluster_display = cluster_name or "Unknown"
        self.console.print(f"📊 Pre-Flight Check {cluster_display}: {analysis['target_node']} (Zone: {analysis['target_zone']})")
        self.console.print(f"• Shards to move: {shard_summary}")
        self.console.print(f"• Data to move: {format_size(data_to_move)}")
        self.console.print(f"• Target nodes: {target_summary}")
        # Format time display with seconds if under 1 minute
        if hours == 0 and minutes == 0:
            time_display = f"{seconds}s"
        elif hours == 0:
            time_display = f"{minutes}m {seconds}s"
        else:
            time_display = f"{hours}h {minutes}m"

        self.console.print(f"• Estimated time: {time_display} ({throughput_mb_per_sec:.0f}MB/sec)")
        self.console.print(f"{status_icon} Status: {status_text}")

        # Add critical warnings for blocked scenarios
        if len(analysis['candidate_nodes']) == 0:
            self.console.print(f"[red]⚠️ CRITICAL: Node isolated in zone '{analysis['target_zone']}' - add nodes or reconfigure zones[/red]")
        elif not analysis['capacity_sufficient']:
            self.console.print("[red]⚠️ CRITICAL: Insufficient capacity - add nodes or free space before maintenance[/red]")


def create_maintenance_commands(main_cli):
    """Register maintenance commands with the main CLI"""

    @main_cli.command()
    @click.option('--top-tables', default=10, help='Number of largest tables to analyze (default: 10)')
    @click.option('--table', help='Analyze specific table only (e.g., "my_table" or "schema.table")')
    @click.pass_context
    def shard_distribution(ctx, top_tables: int, table: Optional[str]):
        """Analyze shard distribution anomalies across cluster nodes

        This command analyzes the largest tables in your cluster to detect:
        • Uneven shard count distribution between nodes
        • Storage imbalances across nodes
        • Missing node coverage for tables
        • Document count anomalies within tables

        The coefficient of variation (CV%) indicates distribution uniformity:
        • CV < 20%: Well distributed
        • CV 20-40%: Moderately uneven
        • CV > 40%: Highly uneven, consider rebalancing

        Examples:
            xmover shard-distribution                    # Analyze top 10 tables
            xmover shard-distribution --top-tables 25   # Analyze top 25 tables
            xmover shard-distribution --table my_table   # Analyze specific table
        """
        client = ctx.obj['client']
        maintenance = MaintenanceCommands(client)
        maintenance.shard_distribution(top_tables, table)

    @main_cli.command()
    @click.option('--sizeMB', default=512, help='Minimum translog uncommitted size in MB (default: 512)')
    @click.option('--execute', is_flag=True, help='Generate SQL commands for display (does not execute against database)')
    @click.option('--autoexec', is_flag=True, help='Automatically execute replica reset operations')
    @click.option('--dry-run', is_flag=True, help='Simulate operations without actual database changes')
    @click.option('--percentage', default=200, help='Only process tables exceeding this percentage of threshold (default: 200)')
    @click.option('--max-wait', default=720, help='Maximum seconds to wait for retention leases (default: 720)')
    @click.option('--log-format', type=click.Choice(['console', 'json']), default='console', help='Logging format for container environments')
    @click.pass_context
    def problematic_translogs(ctx, sizemb: int, execute: bool, autoexec: bool, dry_run: bool, 
                             percentage: int, max_wait: int, log_format: str):
        """Find tables with problematic translog sizes and optionally execute automatic replica reset
        
        This command can operate in three modes:
        
        1. ANALYSIS MODE (default): Shows problematic shards only
        2. COMMAND GENERATION MODE (--execute): Generates SQL commands for manual execution  
        3. AUTOEXEC MODE (--autoexec): Automatically executes replica reset operations
        
        AUTOEXEC MODE performs these operations for each problematic table:
        • Set number_of_replicas to 0
        • Monitor retention leases until cleared (with incremental backoff)
        • Restore original replica count
        
        Use --dry-run with --autoexec to simulate operations without database changes.
        Use --log-format json for structured logging in container environments.
        
        Examples:
            xmover problematic-translogs                                    # Analysis only
            xmover problematic-translogs --execute                         # Generate SQL commands
            xmover problematic-translogs --autoexec                        # Execute operations
            xmover problematic-translogs --autoexec --dry-run              # Simulate execution
            xmover problematic-translogs --autoexec --percentage 150       # Process tables >150% of threshold
            xmover problematic-translogs --autoexec --log-format json      # Container-friendly logging
        """
        # Validation
        if autoexec and execute:
            click.echo("Error: --autoexec and --execute flags are mutually exclusive", err=True)
            ctx.exit(1)
            
        if dry_run and not autoexec:
            click.echo("Error: --dry-run can only be used with --autoexec", err=True)
            ctx.exit(1)
            
        client = ctx.obj['client']
        maintenance = MaintenanceCommands(client)
        maintenance.problematic_translogs(sizemb, execute, autoexec, dry_run, percentage, max_wait, log_format)

    @main_cli.command()
    @click.option('--node', required=True, help='Target node to analyze for decommissioning')
    @click.option('--min-availability',
                  type=click.Choice(['full', 'primaries'], case_sensitive=False),
                  required=True,
                  help='Minimum availability level: "full" (move all shards) or "primaries" (move only primaries without replicas)')
    @click.option('--short', is_flag=True,
                  help='Display only essential information: shard count, data size, target nodes, and ETA')
    @click.pass_context
    def check_maintenance(ctx, node: str, min_availability: str, short: bool):
        """Check whether a node could be decommissioned and analyze shard movement requirements

        This command analyzes if a node can be safely decommissioned by checking:
        • Available capacity on other nodes (considering disk watermarks)
        • Shard types and replica availability
        • Estimated recovery time based on cluster settings

        Use --short for a brief summary with only essential information:
        • Amount of shards to move
        • Amount of data to move
        • Possible target nodes
        • ETA for moving data (including recovery rate)

        Minimum availability levels:
        • "full": All shards need to be moved away from the node
        • "primaries": Only primaries without replicas need data movement.
          Primary shards with replicas can be quickly converted to replicas.

        The analysis considers:
        • Low watermark thresholds for target node capacity
        • Max number of shards per node limits
        • Availability zone constraints (capacity must be in same AZ)
        • Recovery bandwidth settings for time estimation

        Reports:
        • Primary shards without replicas (slow data movement required)
        • Primary shards with replicas (fast primary→replica conversion)
        • Replica shards (no action needed for "primaries" type)
        • Estimated time based on recovery.max_bytes_per_sec and routing.node_concurrent_recoveries

        Examples:
            xmover check-maintenance --node data-hot-4 --min-availability full        # Check full decommission
            xmover check-maintenance --node data-hot-4 --min-availability primaries   # Check primaries maintenance
            xmover check-maintenance --node data-hot-4 --min-availability full --short # Brief summary only
        """
        client = ctx.obj['client']
        maintenance = MaintenanceCommands(client)
        maintenance.check_maintenance(node, min_availability.lower(), short)


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
    
    def __init__(self, table_info: Dict[str, Any], client, dry_run: bool = False, 
                 max_wait: int = 720, log_format: str = "console"):
        self.table_info = table_info
        self.client = client
        self.dry_run = dry_run
        self.max_wait = max_wait
        self.log_format = log_format
        
        self.schema_name = table_info['schema_name']
        self.table_name = table_info['table_name']
        self.partition_values = table_info.get('partition_values', '')
        self.partition_ident = table_info.get('partition_ident', '')
        self.original_replicas = table_info.get('current_replicas', 0)
        
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
        name = f"{self.schema_name}.{self.table_name}"
        if self.partition_values and self.partition_values != 'NULL':
            name += f" PARTITION {self.partition_values}"
        return name
        
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
            if self.partition_values and self.partition_values != 'NULL':
                sql = f'ALTER TABLE "{self.schema_name}"."{self.table_name}" PARTITION {self.partition_values} SET ("number_of_replicas" = 0);'
            else:
                sql = f'ALTER TABLE "{self.schema_name}"."{self.table_name}" SET ("number_of_replicas" = 0);'
            
            self._log_info(f"Setting replicas to 0 (original: {self.original_replicas})")
            self._log_info(f"Executing: {sql}")
            
            if not self.dry_run:
                result = self.client.execute_query(sql)
                if 'error' in result:
                    self._handle_failure(f"Failed to set replicas to 0: {result.get('error', 'Unknown error')}")
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
                expected_count = self.table_info.get('total_primary_shards', 1)
                
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
            if self.partition_values and self.partition_values != 'NULL':
                sql = f'ALTER TABLE "{self.schema_name}"."{self.table_name}" PARTITION {self.partition_values} SET ("number_of_replicas" = {self.original_replicas});'
            else:
                sql = f'ALTER TABLE "{self.schema_name}"."{self.table_name}" SET ("number_of_replicas" = {self.original_replicas});'
            
            self._log_info(f"Restoring replicas to {self.original_replicas}")
            self._log_info(f"Executing: {sql}")
            
            if not self.dry_run:
                result = self.client.execute_query(sql)
                if 'error' in result:
                    self._handle_failure(f"CRITICAL: Failed to restore replicas: {result.get('error', 'Unknown error')}")
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
            if self.partition_values and self.partition_values != 'NULL':
                sql = f"""
                SELECT array_length(retention_leases['leases'], 1) as cnt_leases
                FROM sys.shards
                WHERE table_name = '{self.table_name}'
                  AND schema_name = '{self.schema_name}'
                  AND partition_ident = '{self.partition_ident}'
                """
            else:
                sql = f"""
                SELECT array_length(retention_leases['leases'], 1) as cnt_leases
                FROM sys.shards
                WHERE table_name = '{self.table_name}'
                  AND schema_name = '{self.schema_name}'
                """
            
            result = self.client.execute_query(sql)
            rows = result.get('rows', [])
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
        self._transition_to_state(TableResetState.FAILED)
        self._log_error(error_msg)
        
        # Attempt rollback if we were in monitoring or restoring phase
        if self.state in [TableResetState.MONITORING_LEASES, TableResetState.RESTORING_REPLICAS]:
            self._attempt_rollback()
    
    def _attempt_rollback(self) -> None:
        """Attempt to rollback by restoring original replica count"""
        if self.dry_run:
            self._log_info("DRY RUN: Would attempt rollback to original replica count")
            return
            
        try:
            self._log_info(f"Attempting rollback: restoring {self.original_replicas} replicas")
            
            if self.partition_values and self.partition_values != 'NULL':
                sql = f'ALTER TABLE "{self.schema_name}"."{self.table_name}" PARTITION {self.partition_values} SET ("number_of_replicas" = {self.original_replicas});'
            else:
                sql = f'ALTER TABLE "{self.schema_name}"."{self.table_name}" SET ("number_of_replicas" = {self.original_replicas});'
            
            self._log_info(f"Rollback executing: {sql}")
            result = self.client.execute_query(sql)
            if 'error' not in result:
                self._log_info("Rollback successful")
            else:
                self._log_error(f"MANUAL INTERVENTION REQUIRED: Rollback failed - {result.get('error', 'Unknown error')}")
                
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
