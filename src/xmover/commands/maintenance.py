"""
Maintenance command handlers for XMover

This module contains commands related to cluster maintenance operations:
- shard_distribution: Analyze shard distribution anomalies across cluster nodes
- problematic_translogs: Find tables with problematic translog sizes and generate shard management commands
"""

import sys
import time
from typing import Optional, List, Dict, Any, Union
import click
from rich.table import Table
from rich.panel import Panel
from rich import box
from rich.console import Console

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

        # Summary table
        summary_table = Table(title="Table Distribution Summary", box=box.ROUNDED)
        summary_table.add_column("Table", style="cyan")
        summary_table.add_column("Shards", justify="right", style="magenta")
        summary_table.add_column("Nodes", justify="right", style="blue")
        summary_table.add_column("Primary Size", justify="right", style="green")
        summary_table.add_column("Status", style="white")

        for table_dist in tables_analysis:
            # Calculate totals from node distributions
            total_shards = sum(node['total_shards'] for node in table_dist.node_distributions.values())
            node_count = len(table_dist.node_distributions)

            # Simple status based on node count and shard distribution
            if node_count == 0:
                status = "[red]🔴 NO DATA[/red]"
            elif node_count < 2:
                status = "[yellow]🟡 SINGLE NODE[/yellow]"
            else:
                # Check for balance - basic heuristic
                shard_counts = [node['total_shards'] for node in table_dist.node_distributions.values()]
                max_shards = max(shard_counts)
                min_shards = min(shard_counts)
                if max_shards > min_shards * 2:
                    status = "[yellow]🟡 IMBALANCED[/yellow]"
                else:
                    status = "[green]✅ OK[/green]"

            summary_table.add_row(
                table_dist.full_table_name,
                str(total_shards),
                str(node_count),
                format_size(table_dist.total_primary_size_gb),  # Already in GB
                status
            )

        self.console.print(summary_table)

        # Overall summary
        self.console.print()
        self.console.print(f"[green]✅ Analyzed {len(tables_analysis)} largest tables[/green]")
        self.console.print("[dim]💡 Use --table <table_name> for detailed analysis of specific tables[/dim]")

    def problematic_translogs(self, sizemb: int, execute: bool) -> None:
        """Find tables with problematic translog sizes and generate comprehensive shard management commands

        This command identifies tables with replica shards that have large uncommitted translog sizes
        indicating replication issues. It generates a complete sequence including:
        1. Stop automatic shard rebalancing
        2. REROUTE CANCEL commands for problematic shards
        3. REROUTE ALLOCATE commands to recreate replicas
        4. Re-enable automatic shard rebalancing
        """
        if not self.validate_connection():
            return

        self.console.print(Panel.fit(f"[bold blue]Problematic Translog Analysis[/bold blue]"))
        self.console.print(f"[dim]Analyzing shards with translog uncommitted size ≥ {sizemb} MB[/dim]")

        if execute:
            self.console.print("[yellow]⚠️  COMMAND GENERATION MODE - SQL commands will be generated for display[/yellow]")
        else:
            self.console.print("[green]🔍 ANALYSIS MODE - showing problematic shards only[/green]")

        self.console.print()

        try:
            # Get both individual shards and table summaries
            individual_shards, summary_rows = self._get_problematic_translogs(sizemb)

            if not individual_shards:
                self.console.print("[green]✅ No shards found with problematic translog sizes[/green]")
                self.console.print(f"[dim]All shards have translog uncommitted size < {sizemb} MB[/dim]")
                return

            # Display individual problematic shards
            self._display_individual_problematic_shards(individual_shards, sizemb)

            # Display summary by table
            self._display_table_summary(summary_rows)

            if execute:
                self._generate_comprehensive_commands(individual_shards, summary_rows)
            else:
                self.console.print()
                self.console.print("[dim]💡 Use --execute flag to generate comprehensive shard management commands for display[/dim]")

        except Exception as e:
            self.console.print(f"[red]Error analyzing problematic translogs: {e}[/red]")

    def _get_problematic_translogs(self, min_size_mb: int) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Get individual shards and table summaries with problematic translog sizes"""

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

    def _display_individual_problematic_shards(self, individual_shards: List[Dict[str, Any]], min_size_mb: int) -> None:
        """Display individual problematic shards for REROUTE CANCEL commands"""
        self.console.print(f"[bold]Problematic Replica Shards (translog > {min_size_mb}MB)[/bold]")

        individual_table = Table(box=box.ROUNDED)
        individual_table.add_column("Schema", style="cyan")
        individual_table.add_column("Table", style="blue")
        individual_table.add_column("Partition", style="magenta")
        individual_table.add_column("Shard ID", justify="right", style="yellow")
        individual_table.add_column("Node", style="green")
        individual_table.add_column("Translog MB", justify="right", style="red")

        for shard in individual_shards:
            schema_name = shard['schema_name']
            table_name = shard['table_name']
            partition_values = shard['partition_values']
            shard_id = shard['shard_id']
            node_name = shard['node_name']
            translog_mb = shard['translog_size_mb']

            partition_display = partition_values if partition_values and partition_values != 'NULL' else "none"

            individual_table.add_row(
                schema_name,
                table_name,
                partition_display,
                str(shard_id),
                node_name,
                f"{translog_mb:.1f}"
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
            shard_id = shard['shard_id']
            node_name = shard['node_name']

            cmd = f'ALTER TABLE "{schema_name}"."{table_name}" REROUTE CANCEL SHARD {shard_id} on \'{node_name}\' WITH (allow_primary=False);'
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

    def _get_current_replica_count(self, schema_name: str, table_name: str, partition_ident: Optional[str], partition_values: Optional[str]) -> Union[int, str]:
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
    @click.option('--sizeMB', default=300, help='Minimum translog uncommitted size in MB (default: 300)')
    @click.option('--execute', is_flag=True, help='Generate SQL commands for display (does not execute against database)')
    @click.pass_context
    def problematic_translogs(ctx, sizemb: int, execute: bool):
        """Find tables with problematic translog sizes and generate comprehensive shard management commands

        This command identifies tables with replica shards that have large uncommitted translog sizes
        indicating replication issues. It generates a complete sequence including:
        1. Stop automatic shard rebalancing
        2. REROUTE CANCEL commands for problematic shards
        3. REROUTE ALLOCATE commands to recreate replicas
        4. Re-enable automatic shard rebalancing

        Large translog sizes typically indicate:
        • Network issues between nodes
        • Storage performance problems
        • Node overload or memory pressure
        • Replication lag or failures

        WARNING: This command only cancels REPLICA shards for safety.
        Primary shards with large translogs require manual investigation.

        Examples:
            xmover problematic-translogs                     # Find shards with ≥300MB translogs
            xmover problematic-translogs --sizeMB 500        # Custom size threshold
            xmover problematic-translogs --execute           # Generate management commands
        """
        client = ctx.obj['client']
        maintenance = MaintenanceCommands(client)
        maintenance.problematic_translogs(sizemb, execute)
