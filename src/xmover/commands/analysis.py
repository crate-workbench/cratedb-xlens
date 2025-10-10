"""
Analysis Commands Module

This module contains commands for analyzing shard distribution and performing
deep analysis of the CrateDB cluster.
"""

import sys
from typing import Optional

import click
from rich.table import Table
from rich.panel import Panel
from rich import box

from .base import BaseCommand
from ..analyzer import ShardAnalyzer
from ..shard_size_monitor import ShardSizeMonitor, validate_rules_file
from ..utils import format_size, format_percentage


class AnalysisCommands(BaseCommand):
    """Commands for cluster analysis operations"""

    def execute(self, command: str, **kwargs) -> None:
        """Execute an analysis command by name"""
        if command == 'analyze':
            self.analyze(**kwargs)
        elif command == 'deep_analyze':
            self.deep_analyze(**kwargs)
        else:
            raise ValueError(f"Unknown analysis command: {command}")

    def analyze(self, ctx, table: Optional[str], largest: Optional[int], 
                smallest: Optional[int], no_zero_size: bool):
        """Analyze current shard distribution across nodes and zones
        
        Use --largest N to show the N largest tables/partitions by total size.
        Use --smallest N to show the N smallest tables/partitions by total size.
        Use --no-zero-size with --smallest to exclude zero-sized tables from results.
        Both options properly handle partitioned tables and show detailed size breakdowns.
        """
        client = ctx.obj['client']
        analyzer = ShardAnalyzer(client)

        self.console.print(Panel.fit("[bold blue]CrateDB Cluster Analysis[/bold blue]"))

        # Get cluster overview (includes all shards for complete analysis)
        overview = analyzer.get_cluster_overview()

        # Cluster summary table
        summary_table = Table(title="Cluster Summary", box=box.ROUNDED)
        summary_table.add_column("Metric", style="cyan")
        summary_table.add_column("Value", style="magenta")

        summary_table.add_row("Nodes", str(overview['nodes']))
        summary_table.add_row("Availability Zones", str(overview['zones']))
        summary_table.add_row("Total Shards", str(overview['total_shards']))
        summary_table.add_row("Primary Shards", str(overview['primary_shards']))
        summary_table.add_row("Replica Shards", str(overview['replica_shards']))
        summary_table.add_row("Total Size", format_size(overview['total_size_gb']))

        self.console.print(summary_table)
        self.console.print()

        # Disk watermarks table
        if overview.get('watermarks'):
            watermarks_table = Table(title="Disk Allocation Watermarks", box=box.ROUNDED)
            watermarks_table.add_column("Setting", style="cyan")
            watermarks_table.add_column("Value", style="magenta")

            watermarks = overview['watermarks']
            watermarks_table.add_row("Low Watermark", str(watermarks.get('low', 'Not set')))
            watermarks_table.add_row("High Watermark", str(watermarks.get('high', 'Not set')))
            watermarks_table.add_row("Flood Stage", str(watermarks.get('flood_stage', 'Not set')))
            watermarks_table.add_row("Enable for Single Node", str(watermarks.get('enable_for_single_data_node', 'Not set')))

            self.console.print(watermarks_table)
            self.console.print()

        # Zone distribution table
        zone_table = Table(title="Zone Distribution", box=box.ROUNDED)
        zone_table.add_column("Zone", style="cyan")
        zone_table.add_column("Shards", justify="right", style="magenta")
        zone_table.add_column("Percentage", justify="right", style="green")

        total_shards = overview['total_shards']
        for zone, count in overview['zone_distribution'].items():
            percentage = (count / total_shards * 100) if total_shards > 0 else 0
            zone_table.add_row(zone, str(count), f"{percentage:.1f}%")

        self.console.print(zone_table)
        self.console.print()

        # Node health table
        node_table = Table(title="Node Health", box=box.ROUNDED)
        node_table.add_column("Node", style="cyan")
        node_table.add_column("Zone", style="blue")
        node_table.add_column("Shards", justify="right", style="magenta")
        node_table.add_column("Size", justify="right", style="green")
        node_table.add_column("Disk Usage", justify="right")
        node_table.add_column("Available Space", justify="right", style="green")
        node_table.add_column("Until Low WM", justify="right", style="yellow")
        node_table.add_column("Until High WM", justify="right", style="red")

        for node_info in overview['node_health']:
            # Format watermark remaining capacity
            low_wm_remaining = format_size(node_info['remaining_to_low_watermark_gb']) if node_info['remaining_to_low_watermark_gb'] > 0 else "[red]Exceeded[/red]"
            high_wm_remaining = format_size(node_info['remaining_to_high_watermark_gb']) if node_info['remaining_to_high_watermark_gb'] > 0 else "[red]Exceeded[/red]"

            node_table.add_row(
                node_info['name'],
                node_info['zone'],
                str(node_info['shards']),
                format_size(node_info['size_gb']),
                format_percentage(node_info['disk_usage_percent']),
                format_size(node_info['available_space_gb']),
                low_wm_remaining,
                high_wm_remaining
            )

        self.console.print(node_table)
        self.console.print()

        # Shard Size Overview
        size_overview = analyzer.get_shard_size_overview()
        
        size_table = Table(title="Shard Size Distribution", box=box.ROUNDED)
        size_table.add_column("Size Range", style="cyan")
        size_table.add_column("Count", justify="right", style="magenta")
        size_table.add_column("Percentage", justify="right", style="green")
        size_table.add_column("Avg Size", justify="right", style="blue")
        size_table.add_column("Max Size", justify="right", style="red")
        size_table.add_column("Total Size", justify="right", style="yellow")

        total_shards = size_overview['total_shards']
        
        # Define color coding thresholds
        large_shards_threshold = 0   # warn if ANY shards >=50GB (red flag)
        small_shards_percentage_threshold = 40  # warn if >40% of shards are small (<1GB)
        
        for bucket_name, bucket_data in size_overview['size_buckets'].items():
            count = bucket_data['count']
            avg_size = bucket_data['avg_size_gb']
            total_size = bucket_data['total_size']
            percentage = (count / total_shards * 100) if total_shards > 0 else 0
            
            # Apply color coding
            count_str = str(count)
            percentage_str = f"{percentage:.1f}%"
            
            # Color code large shards (>=50GB) - ANY large shard is a red flag
            if bucket_name == '>=50GB' and count > large_shards_threshold:
                count_str = f"[red]{count}[/red]"
                percentage_str = f"[red]{percentage:.1f}%[/red]"
            
            # Color code if too many very small shards (<1GB)
            if bucket_name == '<1GB' and percentage > small_shards_percentage_threshold:
                count_str = f"[yellow]{count}[/yellow]"
                percentage_str = f"[yellow]{percentage:.1f}%[/yellow]"
            
            size_table.add_row(
                bucket_name,
                count_str,
                percentage_str,
                f"{avg_size:.2f}GB" if avg_size > 0 else "0GB",
                f"{bucket_data['max_size']:.2f}GB" if bucket_data['max_size'] > 0 else "0GB",
                format_size(total_size)
            )
        
        self.console.print(size_table)
        
        # Add footer showing total number of tables/partitions
        all_tables = analyzer.get_table_size_breakdown(limit=None)
        total_tables_partitions = len(all_tables)
        self.console.print(f"[dim]📊 Total: {total_tables_partitions} table/partition(s) in cluster[/dim]")
        
        # Add schema breakdown table
        schema_stats = {}
        for table_info in all_tables:
            # Extract schema from table name (format: "schema.table" or just "table")
            table_name = table_info['table_name']
            if '.' in table_name:
                schema = table_name.split('.')[0]
            else:
                schema = 'doc'  # Default schema
                
            partition = table_info['partition']
            has_partition = partition != 'N/A'
            
            if schema not in schema_stats:
                schema_stats[schema] = {
                    'tables': 0,
                    'partitioned_tables': set(),
                    'total_partitions': 0
                }
            
            if has_partition:
                # This is a partitioned table
                base_table_name = table_name
                schema_stats[schema]['partitioned_tables'].add(base_table_name)
                schema_stats[schema]['total_partitions'] += 1
            else:
                # This is a regular table
                schema_stats[schema]['tables'] += 1
        
        # Create schema breakdown table
        self.console.print()
        schema_table = Table(title="Schema Breakdown", box=box.ROUNDED)
        schema_table.add_column("Schema", style="cyan")
        schema_table.add_column("Tables", justify="right", style="green")
        schema_table.add_column("Partitioned Tables", justify="right", style="magenta")
        schema_table.add_column("Total Partitions", justify="right", style="yellow")
        
        # Sort schemas alphabetically (case-insensitive)
        for schema in sorted(schema_stats.keys(), key=str.lower):
            stats = schema_stats[schema]
            tables_count = stats['tables']
            partitioned_tables_count = len(stats['partitioned_tables'])
            total_partitions = stats['total_partitions']
            
            schema_table.add_row(
                schema,
                str(tables_count),
                str(partitioned_tables_count),
                str(total_partitions)
            )
        
        self.console.print(schema_table)
        
        # Add warnings if thresholds are exceeded
        warnings = []
        if size_overview['large_shards_count'] > large_shards_threshold:
            warnings.append(f"[red]🔥 CRITICAL: {size_overview['large_shards_count']} large shards (>=50GB) detected - IMMEDIATE ACTION REQUIRED![/red]")
            warnings.append(f"[red]   Large shards cause slow recovery, memory pressure, and performance issues[/red]")
        
        # Calculate percentage of very small shards (<1GB)
        very_small_count = size_overview['size_buckets']['<1GB']['count']
        very_small_percentage = (very_small_count / total_shards * 100) if total_shards > 0 else 0
        
        if very_small_percentage > small_shards_percentage_threshold:
            warnings.append(f"[yellow]⚠️  {very_small_percentage:.1f}% of shards are very small (<1GB) - consider optimizing shard allocation[/yellow]")
            warnings.append(f"[yellow]   Too many small shards create metadata overhead and reduce efficiency[/yellow]")
        
        if warnings:
            self.console.print()
            for warning in warnings:
                self.console.print(warning)
        
        # Show compact table/partition breakdown of large shards if any exist
        if size_overview['large_shards_count'] > 0:
            self.console.print()
            large_shards_details = analyzer.get_large_shards_details()
            
            # Aggregate by table/partition
            table_partition_stats = {}
            for shard in large_shards_details:
                # Create table key with schema
                table_display = shard['table_name']
                if shard['schema_name'] and shard['schema_name'] != 'doc':
                    table_display = f"{shard['schema_name']}.{shard['table_name']}"
                
                # Create partition key
                partition_key = shard['partition_values'] or "N/A"
                
                # Create combined key
                key = (table_display, partition_key)
                
                if key not in table_partition_stats:
                    table_partition_stats[key] = {
                        'sizes': [],
                        'primary_count': 0,
                        'replica_count': 0,
                        'total_size': 0.0
                    }
                
                # Aggregate stats
                stats = table_partition_stats[key]
                stats['sizes'].append(shard['size_gb'])
                stats['total_size'] += shard['size_gb']
                if shard['is_primary']:
                    stats['primary_count'] += 1
                else:
                    stats['replica_count'] += 1
            
            # Create compact table
            large_shards_table = Table(title=f"Large Shards Breakdown by Table/Partition (>=50GB)", box=box.ROUNDED)
            large_shards_table.add_column("Table", style="cyan")
            large_shards_table.add_column("Partition", style="blue")
            large_shards_table.add_column("Shards", justify="right", style="magenta")
            large_shards_table.add_column("P/R", justify="center", style="yellow") 
            large_shards_table.add_column("Min Size", justify="right", style="green")
            large_shards_table.add_column("Avg Size", justify="right", style="red")
            large_shards_table.add_column("Max Size", justify="right", style="red")
            large_shards_table.add_column("Total Size", justify="right", style="red")
            
            # Sort by total size descending (most problematic first)
            sorted_stats = sorted(table_partition_stats.items(), key=lambda x: x[1]['total_size'], reverse=True)
            
            for (table_name, partition_key), stats in sorted_stats:
                # Format partition display
                partition_display = partition_key
                if partition_display != "N/A" and len(partition_display) > 25:
                    partition_display = partition_display[:22] + "..."
                
                # Calculate size stats
                sizes = stats['sizes']
                min_size = min(sizes)
                avg_size = sum(sizes) / len(sizes)
                max_size = max(sizes)
                total_size = stats['total_size']
                total_shards = len(sizes)
                
                # Format primary/replica ratio
                p_r_display = f"{stats['primary_count']}P/{stats['replica_count']}R"
                
                large_shards_table.add_row(
                    table_name,
                    partition_display,
                    str(total_shards),
                    p_r_display,
                    f"{min_size:.1f}GB",
                    f"{avg_size:.1f}GB", 
                    f"{max_size:.1f}GB",
                    f"{total_size:.1f}GB"
                )
            
            self.console.print(large_shards_table)
            
            # Add summary stats
            total_primary = sum(stats['primary_count'] for stats in table_partition_stats.values())
            total_replica = sum(stats['replica_count'] for stats in table_partition_stats.values())
            affected_table_partitions = len(table_partition_stats)
            
            self.console.print()
            self.console.print(f"[dim]📊 Summary: {total_primary} primary, {total_replica} replica shards across {affected_table_partitions} table/partition(s)[/dim]")
        
        # Show compact table/partition breakdown of smallest shards (top 10)
        self.console.print()
        small_shards_details = analyzer.get_small_shards_details(limit=10)
        
        if small_shards_details:
            # Create compact table
            small_shards_table = Table(title=f"Smallest Shards Breakdown by Table/Partition (Top 10)", box=box.ROUNDED)
            small_shards_table.add_column("Table", style="cyan")
            small_shards_table.add_column("Partition", style="blue")
            small_shards_table.add_column("Shards", justify="right", style="magenta")
            small_shards_table.add_column("P/R", justify="center", style="yellow") 
            small_shards_table.add_column("Min Size", justify="right", style="green")
            small_shards_table.add_column("Avg Size", justify="right", style="red")
            small_shards_table.add_column("Max Size", justify="right", style="red")
            small_shards_table.add_column("Total Size", justify="right", style="red")
            
            for entry in small_shards_details:
                table_name = entry['table_name']
                partition_key = entry['partition_key']
                stats = entry['stats']
                
                # Format partition display
                partition_display = partition_key
                if partition_display != "N/A" and len(partition_display) > 25:
                    partition_display = partition_display[:22] + "..."
                
                # Calculate size stats
                sizes = stats['sizes']
                min_size = min(sizes)
                avg_size = sum(sizes) / len(sizes)
                max_size = max(sizes)
                total_size = stats['total_size']
                total_shards = len(sizes)
                
                # Format primary/replica ratio
                p_r_display = f"{stats['primary_count']}P/{stats['replica_count']}R"
                
                small_shards_table.add_row(
                    table_name,
                    partition_display,
                    str(total_shards),
                    p_r_display,
                    f"{min_size:.1f}GB",
                    f"{avg_size:.1f}GB", 
                    f"{max_size:.1f}GB",
                    f"{total_size:.1f}GB"
                )
            
            self.console.print(small_shards_table)
            
            # Add summary stats for smallest shards
            total_small_primary = sum(entry['stats']['primary_count'] for entry in small_shards_details)
            total_small_replica = sum(entry['stats']['replica_count'] for entry in small_shards_details)
            small_table_partitions = len(small_shards_details)
            
            self.console.print()
            self.console.print(f"[dim]📊 Summary: {total_small_primary} primary, {total_small_replica} replica shards across {small_table_partitions} table/partition(s) with smallest average sizes[/dim]")
        
        self.console.print()

        # Table-specific analysis if requested
        if table:
            self.console.print()
            self.console.print(Panel.fit(f"[bold blue]Analysis for table: {table}[/bold blue]"))

            stats = analyzer.analyze_distribution(table)

            table_summary = Table(title=f"Table {table} Distribution", box=box.ROUNDED)
            table_summary.add_column("Metric", style="cyan")
            table_summary.add_column("Value", style="magenta")

            table_summary.add_row("Total Shards", str(stats.total_shards))
            table_summary.add_row("Total Size", format_size(stats.total_size_gb))
            table_summary.add_row("Zone Balance Score", f"{stats.zone_balance_score:.1f}/100")
            table_summary.add_row("Node Balance Score", f"{stats.node_balance_score:.1f}/100")

            self.console.print(table_summary)

        # Show largest tables if requested
        if largest:
            self.console.print()
            largest_tables = analyzer.get_table_size_breakdown(limit=largest, order='largest')
            
            largest_table = Table(title=f"Largest Tables/Partitions by Size (Top {largest})", box=box.ROUNDED)
            largest_table.add_column("Table", style="cyan")
            largest_table.add_column("Partition", style="magenta")
            largest_table.add_column("Shards", justify="right", style="yellow")
            largest_table.add_column("P/R", justify="right", style="blue")
            largest_table.add_column("Min Size", justify="right", style="green")
            largest_table.add_column("Avg Size", justify="right", style="bright_green")
            largest_table.add_column("Max Size", justify="right", style="red")
            largest_table.add_column("Total Size", justify="right", style="bright_red")
            
            for entry in largest_tables:
                table_name = entry['table_name']
                partition = entry['partition']
                total_shards = entry['total_shards']
                primary_count = entry['primary_count']
                replica_count = entry['replica_count']
                min_size = entry['min_size']
                avg_size = entry['avg_size']
                max_size = entry['max_size']
                total_size = entry['total_size']
                
                largest_table.add_row(
                    table_name,
                    partition,
                    str(total_shards),
                    f"{primary_count}P/{replica_count}R",
                    f"{min_size:.1f}GB",
                    f"{avg_size:.1f}GB", 
                    f"{max_size:.1f}GB",
                    f"{total_size:.1f}GB"
                )
            
            self.console.print(largest_table)
            
            # Add summary stats
            total_largest_size = sum(entry['total_size'] for entry in largest_tables)
            total_largest_shards = sum(entry['total_shards'] for entry in largest_tables)
            
            self.console.print()
            self.console.print(f"[dim]📊 Summary: {total_largest_shards} total shards using {total_largest_size:.1f}GB across {len(largest_tables)} largest table/partition(s)[/dim]")

        # Show smallest tables if requested
        if smallest:
            self.console.print()
            all_smallest = analyzer.get_table_size_breakdown(limit=None, order='smallest')
            
            # Filter based on no_zero_size flag
            if no_zero_size:
                # Use tolerance for effectively zero-sized tables (handles display formatting)
                # Since display uses {size:.1f}GB format, anything < 0.05GB displays as 0.0GB
                zero_tolerance = 0.05  # Consider anything that displays as 0.0GB as effectively zero
                
                # Count effectively zero-sized tables
                zero_sized_count = len([t for t in all_smallest if t['total_size'] < zero_tolerance])
                # Filter out effectively zero-sized tables and take the requested number
                non_zero_tables = [t for t in all_smallest if t['total_size'] >= zero_tolerance]
                smallest_tables = non_zero_tables[:smallest]
                
                if zero_sized_count > 0:
                    self.console.print(f"[dim]ℹ️  Found {zero_sized_count} table/partition(s) with 0.0GB size (excluded from results)[/dim]")
                    self.console.print()
            else:
                smallest_tables = all_smallest[:smallest]
            
            smallest_table = Table(title=f"Smallest Tables/Partitions by Size (Top {len(smallest_tables)})", box=box.ROUNDED)
            smallest_table.add_column("Table", style="cyan")
            smallest_table.add_column("Partition", style="magenta")
            smallest_table.add_column("Shards", justify="right", style="yellow")
            smallest_table.add_column("P/R", justify="right", style="blue")
            smallest_table.add_column("Min Size", justify="right", style="green")
            smallest_table.add_column("Avg Size", justify="right", style="bright_green")
            smallest_table.add_column("Max Size", justify="right", style="red")
            smallest_table.add_column("Total Size", justify="right", style="bright_red")
            
            for entry in smallest_tables:
                table_name = entry['table_name']
                partition = entry['partition']
                total_shards = entry['total_shards']
                primary_count = entry['primary_count']
                replica_count = entry['replica_count']
                min_size = entry['min_size']
                avg_size = entry['avg_size']
                max_size = entry['max_size']
                total_size = entry['total_size']
                
                smallest_table.add_row(
                    table_name,
                    partition,
                    str(total_shards),
                    f"{primary_count}P/{replica_count}R",
                    f"{min_size:.1f}GB",
                    f"{avg_size:.1f}GB", 
                    f"{max_size:.1f}GB",
                    f"{total_size:.1f}GB"
                )
            
            self.console.print(smallest_table)
            
            # Add summary stats
            total_smallest_size = sum(entry['total_size'] for entry in smallest_tables)
            total_smallest_shards = sum(entry['total_shards'] for entry in smallest_tables)
            
            self.console.print()
            if no_zero_size and len([t for t in all_smallest if t['total_size'] < 0.05]) > 0:
                self.console.print(f"[dim]📊 Summary: {total_smallest_shards} total shards using {total_smallest_size:.3f}GB across {len(smallest_tables)} smallest non-zero table/partition(s)[/dim]")
            else:
                self.console.print(f"[dim]📊 Summary: {total_smallest_shards} total shards using {total_smallest_size:.1f}GB across {len(smallest_tables)} smallest table/partition(s)[/dim]")

    def deep_analyze(self, ctx, rules_file: Optional[str], schema: Optional[str], 
                     severity: Optional[str], export_csv: Optional[str],
                     validate_rules: Optional[str]):
        """Deep analysis of shard sizes with configurable optimization rules
        
        This command analyzes your CrateDB cluster's shard sizes, column counts,
        and distribution patterns, then applies a comprehensive set of rules to
        identify optimization opportunities and performance issues.
        
        Features:
        - Cluster configuration analysis (nodes, CPU, memory, heap)
        - Table and partition shard size analysis
        - Configurable rule-based recommendations
        - CSV export for spreadsheet analysis
        - Custom rules file support
        
        Examples:
        
            # Run full analysis with default rules
            xmover deep-analyze
            
            # Analyze specific schema only
            xmover deep-analyze --schema myschema
            
            # Show only critical issues
            xmover deep-analyze --severity critical
            
            # Export to spreadsheet
            xmover deep-analyze --export-csv shard_analysis.csv
            
            # Use custom rules
            xmover deep-analyze --rules-file custom_rules.yaml
            
            # Validate rules file
            xmover deep-analyze --validate-rules custom_rules.yaml
        """
        if validate_rules:
            if validate_rules_file(validate_rules):
                self.console.print(f"[green]✅ Rules file {validate_rules} is valid[/green]")
                sys.exit(0)
            else:
                sys.exit(1)
        
        try:
            client = ctx.obj['client']
            
            # Initialize monitor with optional custom rules
            monitor = ShardSizeMonitor(client, rules_file)
            
            self.console.print("[bold blue]🔍 XMover Deep Shard Size Analysis[/bold blue]")
            self.console.print("Analyzing cluster configuration and shard distributions...\n")
            
            # Run analysis
            report = monitor.analyze_cluster_shard_sizes(schema_filter=schema)
            
            # Display results
            monitor.display_report(report, severity_filter=severity)
            
            # Export CSV if requested
            if export_csv:
                monitor.export_csv(report, export_csv)
                self.console.print(f"\n[green]📊 Results exported to {export_csv}[/green]")
            
            # Summary footer
            violation_counts = report.total_violations_by_severity
            total_violations = sum(violation_counts.values())
            
            if total_violations > 0:
                self.console.print(f"\n[bold]Analysis completed:[/bold] {total_violations} optimization opportunities identified")
                if violation_counts['critical'] > 0:
                    self.console.print("[red]⚠️  Critical issues require immediate attention[/red]")
            else:
                self.console.print("\n[bold green]🎉 Excellent! No optimization issues detected[/bold green]")
                
        except Exception as e:
            self.handle_error(e, "Error during deep shard size analysis")
            import traceback
            self.console.print(f"[dim]{traceback.format_exc()}[/dim]")


def create_analysis_commands(main_group):
    """Register analysis commands with the main CLI group"""

    @main_group.command()
    @click.option('--table', '-t', help='Analyze specific table only')
    @click.option('--largest', type=int, help='Show N largest tables/partitions by size')
    @click.option('--smallest', type=int, help='Show N smallest tables/partitions by size')
    @click.option('--no-zero-size', is_flag=True, default=False, help='Exclude zero-sized tables from smallest results')
    @click.pass_context
    def analyze(ctx, table: Optional[str], largest: Optional[int], smallest: Optional[int], no_zero_size: bool):
        """Analyze current shard distribution across nodes and zones
        
        Use --largest N to show the N largest tables/partitions by total size.
        Use --smallest N to show the N smallest tables/partitions by total size.
        Use --no-zero-size with --smallest to exclude zero-sized tables from results.
        Both options properly handle partitioned tables and show detailed size breakdowns.
        """
        client = ctx.obj['client']
        commands = AnalysisCommands(client)
        commands.analyze(ctx, table, largest, smallest, no_zero_size)

    @main_group.command("deep-analyze")
    @click.option('--rules-file', '-r', type=click.Path(exists=True), 
                  help='Path to custom rules YAML file')
    @click.option('--schema', '-s', help='Analyze specific schema only')
    @click.option('--severity', type=click.Choice(['critical', 'warning', 'info']),
                  help='Show only violations of specified severity')
    @click.option('--export-csv', type=click.Path(), 
                  help='Export results to CSV file')
    @click.option('--validate-rules', type=click.Path(exists=True),
                  help='Validate rules file and exit')
    @click.pass_context
    def deep_analyze(ctx, rules_file: Optional[str], schema: Optional[str], 
                     severity: Optional[str], export_csv: Optional[str],
                     validate_rules: Optional[str]):
        """Deep analysis of shard sizes with configurable optimization rules
        
        This command analyzes your CrateDB cluster's shard sizes, column counts,
        and distribution patterns, then applies a comprehensive set of rules to
        identify optimization opportunities and performance issues.
        
        Features:
        - Cluster configuration analysis (nodes, CPU, memory, heap)
        - Table and partition shard size analysis
        - Configurable rule-based recommendations
        - CSV export for spreadsheet analysis
        - Custom rules file support
        
        Examples:
        
            # Run full analysis with default rules
            xmover deep-analyze
            
            # Analyze specific schema only
            xmover deep-analyze --schema myschema
            
            # Show only critical issues
            xmover deep-analyze --severity critical
            
            # Export to spreadsheet
            xmover deep-analyze --export-csv shard_analysis.csv
            
            # Use custom rules
            xmover deep-analyze --rules-file custom_rules.yaml
            
            # Validate rules file
            xmover deep-analyze --validate-rules custom_rules.yaml
        """
        client = ctx.obj['client']
        commands = AnalysisCommands(client)
        commands.deep_analyze(ctx, rules_file, schema, severity, export_csv, validate_rules)
