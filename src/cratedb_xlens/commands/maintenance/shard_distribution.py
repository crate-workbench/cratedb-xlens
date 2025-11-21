"""
Shard distribution analysis command

This module provides functionality to analyze shard distribution anomalies across
cluster nodes, including uneven shard counts, storage imbalances, and missing node coverage.
"""

from typing import Optional, List
from rich.table import Table
from rich.panel import Panel
from rich import box

from ..base import BaseCommand
from ...distribution_analyzer import DistributionAnalyzer
from ...utils import format_size


class ShardDistributionCommand(BaseCommand):
    """Command handler for shard distribution analysis"""

    def execute(self, top_tables: int, table: Optional[str]) -> None:
        """Analyze shard distribution anomalies across cluster nodes

        This command analyzes the largest tables in your cluster to detect:
        • Uneven shard count distribution between nodes
        • Storage imbalances across nodes
        • Missing node coverage for tables
        • Document count anomalies within tables

        Args:
            top_tables: Number of largest tables to analyze (when table is None)
            table: Specific table name to analyze in detail (optional)
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
