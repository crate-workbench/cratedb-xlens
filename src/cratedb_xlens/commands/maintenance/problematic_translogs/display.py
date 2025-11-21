"""
Display utilities for problematic translog analysis

This module contains the ProblematicTranslogsDisplay class for rendering
problematic translog information in rich formatted tables.
"""

from typing import List, Dict, Any, Union

from rich.console import Console
from rich.table import Table
from rich import box


class ProblematicTranslogsDisplay:
    """Display handler for problematic translog analysis results"""

    def __init__(self, console):
        """Initialize the display handler

        Args:
            console: Rich console for output
        """
        self.console = console

    def display_individual_problematic_shards(self, individual_shards: List[Dict[str, Any]],
                                             min_size_mb: int) -> None:
        """Display individual problematic shards for REROUTE CANCEL commands

        Args:
            individual_shards: List of individual problematic shards
            min_size_mb: Minimum size threshold in MB
        """
        self.console.print(f"[bold]Problematic Replica Shards (exceeding {min_size_mb}MB threshold)[/bold]")

        # Display table-specific threshold information
        if individual_shards and any(shard.get('adaptive_threshold_mb') for shard in individual_shards):
            self.console.print("[dim]Table-specific flush_threshold_size settings (for reference):[/dim]")
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
                self.console.print(f"[dim]├─ {table_key}: {config_mb:.0f}MB config, {threshold_mb:.0f}MB+10% threshold[/dim]")
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

    def display_table_summary(self, summary_rows: List[Dict[str, Any]],
                             get_current_replica_count_fn) -> None:
        """Display summary of tables with problematic translogs

        Args:
            summary_rows: List of table summary data
            get_current_replica_count_fn: Function to get current replica count
        """
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
            current_replicas = get_current_replica_count_fn(
                schema_name, table_name, partition_ident, partition_values
            )
            if current_replicas == "unknown":
                current_replicas = "?"

            results_table.add_row(
                schema_name,
                table_name,
                partition_display,
                str(problematic_replica_shards),
                f"{max_translog_mb:.1f}",
                f"{total_primary_shards}/{total_replica_shards}",
                f"{total_primary_size_gb:.1f}/{total_replica_size_gb:.1f}",
                str(current_replicas)
            )

        self.console.print(results_table)
        self.console.print()
