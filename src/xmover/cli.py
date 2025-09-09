"""
Command line interface for XMover - CrateDB Shard Analyzer and Movement Tool
"""

import sys
import time
import os
import json
from typing import Optional
try:
    import click
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.text import Text
    from rich import box
except ImportError as e:
    print(f"Missing required dependency: {e}")
    print("Please install dependencies with: pip install -e .")
    sys.exit(1)

from .database import CrateDBClient
from .analyzer import ShardAnalyzer, RecoveryMonitor, ActiveShardMonitor
from .distribution_analyzer import DistributionAnalyzer
from .shard_size_monitor import ShardSizeMonitor, validate_rules_file


console = Console()


def format_size(size_gb: float) -> str:
    """Format size in GB with appropriate precision"""
    if size_gb >= 1000:
        return f"{size_gb/1000:.1f}TB"
    elif size_gb >= 1:
        return f"{size_gb:.1f}GB"
    else:
        return f"{size_gb*1000:.0f}MB"


def format_percentage(value: float) -> str:
    """Format percentage with color coding"""
    color = "green"
    if value > 80:
        color = "red"
    elif value > 70:
        color = "yellow"
    return f"[{color}]{value:.1f}%[/{color}]"


def format_table_display_with_partition(schema_name: str, table_name: str, partition_values: str = None) -> str:
    """Format table display with partition values if available"""
    # Create base table name
    if schema_name and schema_name != 'doc':
        base_display = f"{schema_name}.{table_name}"
    else:
        base_display = table_name
    
    # Add partition values if available
    if partition_values:
        return f"{base_display} {partition_values}"
    else:
        return base_display


def format_translog_info(recovery_info) -> str:
    """Format translog size information with color coding showing both total and uncommitted sizes"""
    tl_total_bytes = recovery_info.translog_size_bytes
    tl_uncommitted_bytes = recovery_info.translog_uncommitted_bytes
    
    # Only show if significant (>10MB for production) - check uncommitted size primarily
    if tl_uncommitted_bytes < 10 * 1024 * 1024 and tl_total_bytes < 50 * 1024 * 1024:  # 10MB uncommitted or 50MB total
        return ""
    
    tl_total_gb = recovery_info.translog_size_gb
    tl_uncommitted_gb = recovery_info.translog_uncommitted_gb
    uncommitted_percentage = recovery_info.translog_uncommitted_percentage
    
    # Color coding based on uncommitted size and percentage
    # Round percentage to handle floating-point precision issues
    rounded_percentage = round(uncommitted_percentage, 1)
    if tl_uncommitted_gb >= 5.0 or rounded_percentage >= 80.0:
        color = "red"
    elif tl_uncommitted_gb >= 1.0 or rounded_percentage >= 50.0:
        color = "yellow"
    else:
        color = "green"
    
    # Format sizes
    if tl_total_gb >= 1.0:
        total_str = f"{tl_total_gb:.1f}GB"
    else:
        total_str = f"{tl_total_gb*1000:.0f}MB"
        
    if tl_uncommitted_gb >= 1.0:
        uncommitted_str = f"{tl_uncommitted_gb:.1f}GB"
    else:
        uncommitted_str = f"{tl_uncommitted_gb*1000:.0f}MB"
    
    return f" [dim]([{color}]TL:{total_str} / {uncommitted_str} / {uncommitted_percentage:.0f}%[/{color}])[/dim]"


def format_recovery_progress(recovery_info) -> str:
    """Format recovery progress, using sequence number progress for replicas when available"""
    if not recovery_info.is_primary and recovery_info.seq_no_progress is not None:
        # For replica shards, show sequence number progress if available
        seq_progress = recovery_info.seq_no_progress
        traditional_progress = recovery_info.overall_progress
        
        # If sequence progress is significantly different from traditional progress, show both
        if abs(seq_progress - traditional_progress) > 5.0:
            return f"{seq_progress:.1f}% (seq) / {traditional_progress:.1f}% (rec)"
        else:
            return f"{seq_progress:.1f}% (seq)"
    else:
        # For primary shards or when sequence progress unavailable, use traditional progress
        return f"{recovery_info.overall_progress:.1f}%"


@click.group()
@click.version_option()
@click.pass_context
def main(ctx):
    """XMover - CrateDB Shard Analyzer and Movement Tool

    A tool for analyzing CrateDB shard distribution across nodes and availability zones,
    and generating safe SQL commands for shard rebalancing.
    """
    ctx.ensure_object(dict)

    # Test connection on startup
    try:
        client = CrateDBClient()
        if not client.test_connection():
            console.print("[red]Error: Could not connect to CrateDB[/red]")
            console.print("Please check your CRATE_CONNECTION_STRING in .env file")
            sys.exit(1)
        ctx.obj['client'] = client
    except Exception as e:
        console.print(f"[red]Error connecting to CrateDB: {e}[/red]")
        sys.exit(1)


@main.command()
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
    analyzer = ShardAnalyzer(client)

    console.print(Panel.fit("[bold blue]CrateDB Cluster Analysis[/bold blue]"))

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

    console.print(summary_table)
    console.print()

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

        console.print(watermarks_table)
        console.print()

    # Zone distribution table
    zone_table = Table(title="Zone Distribution", box=box.ROUNDED)
    zone_table.add_column("Zone", style="cyan")
    zone_table.add_column("Shards", justify="right", style="magenta")
    zone_table.add_column("Percentage", justify="right", style="green")

    total_shards = overview['total_shards']
    for zone, count in overview['zone_distribution'].items():
        percentage = (count / total_shards * 100) if total_shards > 0 else 0
        zone_table.add_row(zone, str(count), f"{percentage:.1f}%")

    console.print(zone_table)
    console.print()

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

    console.print(node_table)
    console.print()

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
    
    console.print(size_table)
    
    # Add footer showing total number of tables/partitions
    all_tables = analyzer.get_table_size_breakdown(limit=None)
    total_tables_partitions = len(all_tables)
    console.print(f"[dim]📊 Total: {total_tables_partitions} table/partition(s) in cluster[/dim]")
    
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
    console.print()
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
    
    console.print(schema_table)
    
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
        console.print()
        for warning in warnings:
            console.print(warning)
    
    # Show compact table/partition breakdown of large shards if any exist
    if size_overview['large_shards_count'] > 0:
        console.print()
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
        
        console.print(large_shards_table)
        
        # Add summary stats
        total_primary = sum(stats['primary_count'] for stats in table_partition_stats.values())
        total_replica = sum(stats['replica_count'] for stats in table_partition_stats.values())
        affected_table_partitions = len(table_partition_stats)
        
        console.print()
        console.print(f"[dim]📊 Summary: {total_primary} primary, {total_replica} replica shards across {affected_table_partitions} table/partition(s)[/dim]")
    
    # Show compact table/partition breakdown of smallest shards (top 10)
    console.print()
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
        
        console.print(small_shards_table)
        
        # Add summary stats for smallest shards
        total_small_primary = sum(entry['stats']['primary_count'] for entry in small_shards_details)
        total_small_replica = sum(entry['stats']['replica_count'] for entry in small_shards_details)
        small_table_partitions = len(small_shards_details)
        
        console.print()
        console.print(f"[dim]📊 Summary: {total_small_primary} primary, {total_small_replica} replica shards across {small_table_partitions} table/partition(s) with smallest average sizes[/dim]")
    
    console.print()

    # Table-specific analysis if requested
    if table:
        console.print()
        console.print(Panel.fit(f"[bold blue]Analysis for table: {table}[/bold blue]"))

        stats = analyzer.analyze_distribution(table)

        table_summary = Table(title=f"Table {table} Distribution", box=box.ROUNDED)
        table_summary.add_column("Metric", style="cyan")
        table_summary.add_column("Value", style="magenta")

        table_summary.add_row("Total Shards", str(stats.total_shards))
        table_summary.add_row("Total Size", format_size(stats.total_size_gb))
        table_summary.add_row("Zone Balance Score", f"{stats.zone_balance_score:.1f}/100")
        table_summary.add_row("Node Balance Score", f"{stats.node_balance_score:.1f}/100")

        console.print(table_summary)

    # Show largest tables if requested
    if largest:
        console.print()
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
        
        console.print(largest_table)
        
        # Add summary stats
        total_largest_size = sum(entry['total_size'] for entry in largest_tables)
        total_largest_shards = sum(entry['total_shards'] for entry in largest_tables)
        
        console.print()
        console.print(f"[dim]📊 Summary: {total_largest_shards} total shards using {total_largest_size:.1f}GB across {len(largest_tables)} largest table/partition(s)[/dim]")

    # Show smallest tables if requested
    if smallest:
        console.print()
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
                console.print(f"[dim]ℹ️  Found {zero_sized_count} table/partition(s) with 0.0GB size (excluded from results)[/dim]")
                console.print()
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
        
        console.print(smallest_table)
        
        # Add summary stats
        total_smallest_size = sum(entry['total_size'] for entry in smallest_tables)
        total_smallest_shards = sum(entry['total_shards'] for entry in smallest_tables)
        
        console.print()
        if no_zero_size and len([t for t in all_smallest if t['total_size'] < 0.05]) > 0:
            console.print(f"[dim]📊 Summary: {total_smallest_shards} total shards using {total_smallest_size:.3f}GB across {len(smallest_tables)} smallest non-zero table/partition(s)[/dim]")
        else:
            console.print(f"[dim]📊 Summary: {total_smallest_shards} total shards using {total_smallest_size:.3f}GB across {len(smallest_tables)} smallest table/partition(s)[/dim]")


@main.command()
@click.option('--table', '-t', help='Find candidates for specific table only')
@click.option('--min-size', default=40.0, help='Minimum shard size in GB (default: 40)')
@click.option('--max-size', default=60.0, help='Maximum shard size in GB (default: 60)')
@click.option('--limit', default=20, help='Maximum number of candidates to show (default: 20)')
@click.option('--node', help='Only show candidates from this specific source node (e.g., data-hot-4)')
@click.pass_context
def find_candidates(ctx, table: Optional[str], min_size: float, max_size: float, limit: int, node: Optional[str]):
    """Find shard candidates for movement based on size criteria

    Results are sorted by nodes with least available space first,
    then by shard size (smallest first) for easier moves.
    """
    client = ctx.obj['client']
    analyzer = ShardAnalyzer(client)

    console.print(Panel.fit(f"[bold blue]Finding Moveable Shards ({min_size}-{max_size}GB)[/bold blue]"))

    if node:
        console.print(f"[dim]Filtering: Only showing candidates from source node '{node}'[/dim]")

    # Find moveable candidates (only healthy shards suitable for operations)
    candidates = analyzer.find_moveable_shards(min_size, max_size, table)

    # Filter by node if specified
    if node:
        candidates = [c for c in candidates if c.node_name == node]

    if not candidates:
        if node:
            console.print(f"[yellow]No moveable shards found on node '{node}' in the specified size range.[/yellow]")
            console.print(f"[dim]Tip: Try different size ranges or remove --node filter to see all candidates[/dim]")
        else:
            console.print("[yellow]No moveable shards found in the specified size range.[/yellow]")
        return

    # Show limited results
    shown_candidates = candidates[:limit]

    candidates_table = Table(title=f"Moveable Shard Candidates (showing {len(shown_candidates)} of {len(candidates)})", box=box.ROUNDED)
    candidates_table.add_column("Table", style="cyan")
    candidates_table.add_column("Shard ID", justify="right", style="magenta")
    candidates_table.add_column("Type", style="blue")
    candidates_table.add_column("Node", style="green")
    candidates_table.add_column("Zone", style="yellow")
    candidates_table.add_column("Size", justify="right", style="red")
    candidates_table.add_column("Node Free Space", justify="right", style="white")
    candidates_table.add_column("Documents", justify="right", style="dim")

    # Create a mapping of node names to available space for display
    node_space_map = {node.name: node.available_space_gb for node in analyzer.nodes}

    for shard in shown_candidates:
        node_free_space = node_space_map.get(shard.node_name, 0)
        candidates_table.add_row(
            f"{shard.schema_name}.{shard.table_name}",
            str(shard.shard_id),
            shard.shard_type,
            shard.node_name,
            shard.zone,
            format_size(shard.size_gb),
            format_size(node_free_space),
            f"{shard.num_docs:,}"
        )

    console.print(candidates_table)

    if len(candidates) > limit:
        console.print(f"\n[dim]... and {len(candidates) - limit} more candidates[/dim]")


@main.command()
@click.option('--table', '-t', help='Generate recommendations for specific table only')
@click.option('--min-size', default=40.0, help='Minimum shard size in GB (default: 40)')
@click.option('--max-size', default=60.0, help='Maximum shard size in GB (default: 60)')
@click.option('--zone-tolerance', default=10.0, help='Zone balance tolerance percentage (default: 10)')
@click.option('--min-free-space', default=100.0, help='Minimum free space required on target nodes in GB (default: 100)')
@click.option('--max-moves', default=10, help='Maximum number of move recommendations (default: 10)')
@click.option('--max-disk-usage', default=90.0, help='Maximum disk usage percentage for target nodes (default: 90)')

@click.option('--validate/--no-validate', default=True, help='Validate move safety (default: True)')
@click.option('--prioritize-space/--prioritize-zones', default=False, help='Prioritize available space over zone balancing (default: False)')
@click.option('--dry-run/--execute', default=True, help='Show what would be done without generating SQL commands (default: True)')
@click.option('--auto-execute', is_flag=True, default=False, help='DANGER: Automatically execute the SQL commands (requires --execute, asks for confirmation)')
@click.option('--node', help='Only recommend moves from this specific source node (e.g., data-hot-4)')
@click.pass_context
def recommend(ctx, table: Optional[str], min_size: float, max_size: float,
              zone_tolerance: float, min_free_space: float, max_moves: int, max_disk_usage: float, validate: bool, prioritize_space: bool, dry_run: bool, auto_execute: bool, node: Optional[str]):
    """Generate shard movement recommendations for rebalancing"""
    client = ctx.obj['client']
    analyzer = ShardAnalyzer(client)
    
    # Safety check for auto-execute
    if auto_execute and dry_run:
        console.print("[red]❌ Error: --auto-execute requires --execute flag[/red]")
        console.print("[dim]Use: --execute --auto-execute[/dim]")
        return

    mode_text = "DRY RUN - Analysis Only" if dry_run else "EXECUTION MODE"
    console.print(Panel.fit(f"[bold blue]Generating Rebalancing Recommendations[/bold blue] - [bold {'green' if dry_run else 'red'}]{mode_text}[/bold {'green' if dry_run else 'red'}]"))
    console.print("[dim]Note: Only analyzing healthy shards (STARTED + 100% recovered) for safe operations[/dim]")
    console.print("[dim]Zone conflict detection: Prevents moves that would violate CrateDB's zone awareness[/dim]")
    if prioritize_space:
        console.print("[dim]Mode: Prioritizing available space over zone balancing[/dim]")
    else:
        console.print("[dim]Mode: Prioritizing zone balancing over available space[/dim]")

    if node:
        console.print(f"[dim]Filtering: Only showing moves from source node '{node}'[/dim]")

    console.print(f"[dim]Safety thresholds: Max disk usage {max_disk_usage}%, Min free space {min_free_space}GB[/dim]")

    if dry_run:
        console.print("[green]Running in DRY RUN mode - no SQL commands will be generated[/green]")
    else:
        console.print("[red]EXECUTION MODE - SQL commands will be generated for actual moves[/red]")
    console.print()

    recommendations = analyzer.generate_rebalancing_recommendations(
        table_name=table,
        min_size_gb=min_size,
        max_size_gb=max_size,
        zone_tolerance_percent=zone_tolerance,
        min_free_space_gb=min_free_space,
        max_recommendations=max_moves,
        prioritize_space=prioritize_space,
        source_node=node,
        max_disk_usage_percent=max_disk_usage
    )

    if not recommendations:
        if node:
            console.print(f"[yellow]No safe recommendations found for node '{node}'[/yellow]")
            console.print(f"[dim]This could be due to:[/dim]")
            console.print(f"[dim]  • Zone conflicts preventing safe moves[/dim]")
            console.print(f"[dim]  • Target nodes exceeding {max_disk_usage}% disk usage threshold[/dim]")
            console.print(f"[dim]  • Insufficient free space on target nodes (need {min_free_space}GB)[/dim]")
            console.print(f"[dim]  • No shards in size range {min_size}-{max_size}GB[/dim]")
            console.print(f"[dim]Suggestions:[/dim]")
            console.print(f"[dim]  • Try: --max-disk-usage 95 (allow higher disk usage)[/dim]")
            console.print(f"[dim]  • Try: --min-free-space 50 (reduce space requirements)[/dim]")
            console.print(f"[dim]  • Try: different size ranges or remove --node filter[/dim]")
        else:
            console.print("[green]No rebalancing recommendations needed. Cluster appears well balanced![/green]")
        return

    # Show recommendations table
    rec_table = Table(title=f"Rebalancing Recommendations ({len(recommendations)} moves)", box=box.ROUNDED)
    rec_table.add_column("Table", style="cyan")
    rec_table.add_column("Shard", justify="right", style="magenta")
    rec_table.add_column("Type", style="blue")
    rec_table.add_column("From Node", style="red")
    rec_table.add_column("To Node", style="green")
    rec_table.add_column("Target Free Space", justify="right", style="cyan")
    rec_table.add_column("Zone Change", style="yellow")
    rec_table.add_column("Size", justify="right", style="white")
    rec_table.add_column("Reason", style="dim")
    if validate:
        rec_table.add_column("Safety Check", style="bold")

    # Create a mapping of node names to available space for display
    node_space_map = {node.name: node.available_space_gb for node in analyzer.nodes}

    for rec in recommendations:
        zone_change = f"{rec.from_zone} → {rec.to_zone}" if rec.from_zone != rec.to_zone else rec.from_zone
        target_free_space = node_space_map.get(rec.to_node, 0)

        row = [
            f"{rec.schema_name}.{rec.table_name}",
            str(rec.shard_id),
            rec.shard_type,
            rec.from_node,
            rec.to_node,
            format_size(target_free_space),
            zone_change,
            format_size(rec.size_gb),
            rec.reason
        ]

        if validate:
            is_safe, safety_msg = analyzer.validate_move_safety(rec, max_disk_usage_percent=max_disk_usage)
            safety_status = "[green]✓ SAFE[/green]" if is_safe else f"[red]✗ {safety_msg}[/red]"
            row.append(safety_status)

        rec_table.add_row(*row)

    console.print(rec_table)
    console.print()

    # Generate SQL commands or show dry-run analysis
    if dry_run:
        console.print(Panel.fit("[bold yellow]Dry Run Analysis - No Commands Generated[/bold yellow]"))
        console.print("[dim]# This is a dry run - showing what would be recommended[/dim]")
        console.print("[dim]# Use --execute flag to generate actual SQL commands[/dim]")
        console.print()

        safe_moves = 0
        zone_conflicts = 0
        space_issues = 0

        for i, rec in enumerate(recommendations, 1):
            if validate:
                is_safe, safety_msg = analyzer.validate_move_safety(rec, max_disk_usage_percent=max_disk_usage)
                if not is_safe:
                    if "zone conflict" in safety_msg.lower():
                        zone_conflicts += 1
                        console.print(f"[yellow]⚠ Move {i}: WOULD BE SKIPPED - {safety_msg}[/yellow]")
                    elif "space" in safety_msg.lower():
                        space_issues += 1
                        console.print(f"[yellow]⚠ Move {i}: WOULD BE SKIPPED - {safety_msg}[/yellow]")
                    else:
                        console.print(f"[yellow]⚠ Move {i}: WOULD BE SKIPPED - {safety_msg}[/yellow]")
                    continue
                safe_moves += 1

            console.print(f"[green]✓ Move {i}: WOULD EXECUTE - {rec.reason}[/green]")
            console.print(f"[dim]  Target SQL: {rec.to_sql()}[/dim]")

        console.print()
        console.print(f"[bold]Dry Run Summary:[/bold]")
        console.print(f"  • Safe moves that would execute: [green]{safe_moves}[/green]")
        console.print(f"  • Zone conflicts prevented: [yellow]{zone_conflicts}[/yellow]")
        console.print(f"  • Space-related issues: [yellow]{space_issues}[/yellow]")
        if safe_moves > 0:
            console.print(f"\n[green]✓ Ready to execute {safe_moves} safe moves. Use --execute to generate SQL commands.[/green]")
        else:
            console.print(f"\n[yellow]⚠ No safe moves identified. Review cluster balance or adjust parameters.[/yellow]")
    else:
        console.print(Panel.fit("[bold green]Generated SQL Commands[/bold green]"))
        console.print("[dim]# Copy and paste these commands to execute the moves[/dim]")
        console.print("[dim]# ALWAYS test in a non-production environment first![/dim]")
        console.print("[dim]# These commands only operate on healthy shards (STARTED + fully recovered)[/dim]")
        console.print("[dim]# Commands use quoted identifiers for schema and table names[/dim]")
        console.print()

        safe_moves = 0
        zone_conflicts = 0
        for i, rec in enumerate(recommendations, 1):
            if validate:
                is_safe, safety_msg = analyzer.validate_move_safety(rec, max_disk_usage_percent=max_disk_usage)
                if not is_safe:
                    if "Zone conflict" in safety_msg:
                        zone_conflicts += 1
                        console.print(f"-- Move {i}: SKIPPED - {safety_msg}")
                        console.print(f"--   Tip: Try moving to a different zone or check existing shard distribution")
                    else:
                        console.print(f"-- Move {i}: SKIPPED - {safety_msg}")
                    continue
                safe_moves += 1

            console.print(f"-- Move {i}: {rec.reason}")
            console.print(f"{rec.to_sql()}")
        console.print()

        # Auto-execution if requested
        if auto_execute:
            _execute_recommendations_safely(client, recommendations, validate)

    if validate and safe_moves < len(recommendations):
        if zone_conflicts > 0:
            console.print(f"[yellow]Warning: {zone_conflicts} moves skipped due to zone conflicts[/yellow]")
            console.print(f"[yellow]Tip: Use 'find-candidates' to see current shard distribution across zones[/yellow]")
        console.print(f"[yellow]Warning: Only {safe_moves} of {len(recommendations)} moves passed safety validation[/yellow]")


@main.command()
@click.option('--connection-string', help='Override connection string from .env')
@click.pass_context
def test_connection(ctx, connection_string: Optional[str]):
    """Test connection to CrateDB cluster"""
    try:
        if connection_string:
            client = CrateDBClient(connection_string)
        else:
            client = CrateDBClient()

        if client.test_connection():
            console.print("[green]✓ Connection successful![/green]")

            # Get basic cluster info
            nodes = client.get_nodes_info()
            console.print(f"Connected to cluster with {len(nodes)} nodes:")
            for node in nodes:
                console.print(f"  • {node.name} (zone: {node.zone})")
        else:
            console.print("[red]✗ Connection failed[/red]")
            sys.exit(1)

    except Exception as e:
        console.print(f"[red]✗ Connection error: {e}[/red]")
        sys.exit(1)


@main.command()
@click.option('--table', '-t', help='Check balance for specific table only')
@click.option('--tolerance', default=10.0, help='Zone balance tolerance percentage (default: 10)')
@click.pass_context
def check_balance(ctx, table: Optional[str], tolerance: float):
    """Check zone balance for shards"""
    client = ctx.obj['client']
    analyzer = ShardAnalyzer(client)

    console.print(Panel.fit("[bold blue]Zone Balance Check[/bold blue]"))
    console.print("[dim]Note: Analyzing all shards regardless of state for complete cluster view[/dim]")
    console.print()

    zone_stats = analyzer.check_zone_balance(table, tolerance)

    if not zone_stats:
        console.print("[yellow]No shards found for analysis[/yellow]")
        return

    # Calculate totals and targets
    total_shards = sum(stats['TOTAL'] for stats in zone_stats.values())
    zones = list(zone_stats.keys())
    target_per_zone = total_shards // len(zones) if zones else 0
    tolerance_range = (
        target_per_zone * (1 - tolerance / 100),
        target_per_zone * (1 + tolerance / 100)
    )

    balance_table = Table(title=f"Zone Balance Analysis (Target: {target_per_zone} ±{tolerance}%)", box=box.ROUNDED)
    balance_table.add_column("Zone", style="cyan")
    balance_table.add_column("Primary", justify="right", style="blue")
    balance_table.add_column("Replica", justify="right", style="green")
    balance_table.add_column("Total", justify="right", style="magenta")
    balance_table.add_column("Status", style="bold")

    for zone, stats in zone_stats.items():
        total = stats['TOTAL']

        if tolerance_range[0] <= total <= tolerance_range[1]:
            status = "[green]✓ Balanced[/green]"
        elif total < tolerance_range[0]:
            status = f"[yellow]⚠ Under ({total - target_per_zone:+})[/yellow]"
        else:
            status = f"[red]⚠ Over ({total - target_per_zone:+})[/red]"

        balance_table.add_row(
            zone,
            str(stats['PRIMARY']),
            str(stats['REPLICA']),
            str(total),
            status
        )

    console.print(balance_table)


@main.command()
@click.option('--table', '-t', help='Analyze zones for specific table only')
@click.option('--show-shards/--no-show-shards', default=False, help='Show individual shard details (default: False)')
@click.pass_context
def zone_analysis(ctx, table: Optional[str], show_shards: bool):
    """Detailed analysis of zone distribution and potential conflicts"""
    client = ctx.obj['client']

    console.print(Panel.fit("[bold blue]Detailed Zone Analysis[/bold blue]"))
    console.print("[dim]Comprehensive zone distribution analysis for CrateDB cluster[/dim]")
    console.print()

    # Get all shards for analysis
    shards = client.get_shards_info(table_name=table, for_analysis=True)

    if not shards:
        console.print("[yellow]No shards found for analysis[/yellow]")
        return

    # Organize by table and shard
    tables = {}
    for shard in shards:
        table_key = f"{shard.schema_name}.{shard.table_name}"
        if table_key not in tables:
            tables[table_key] = {}

        shard_key = shard.shard_id
        if shard_key not in tables[table_key]:
            tables[table_key][shard_key] = []

        tables[table_key][shard_key].append(shard)

    # Analyze each table
    zone_conflicts = 0
    under_replicated = 0

    for table_name, table_shards in tables.items():
        console.print(f"\n[bold cyan]Table: {table_name}[/bold cyan]")

        # Create analysis table
        analysis_table = Table(title=f"Shard Distribution for {table_name}", box=box.ROUNDED)
        analysis_table.add_column("Shard ID", justify="right", style="magenta")
        analysis_table.add_column("Primary Zone", style="blue")
        analysis_table.add_column("Replica Zones", style="green")
        analysis_table.add_column("Total Copies", justify="right", style="cyan")
        analysis_table.add_column("Status", style="bold")

        for shard_id, shard_copies in sorted(table_shards.items()):
            primary_zone = "Unknown"
            replica_zones = set()
            total_copies = len(shard_copies)
            zones_with_copies = set()

            for shard_copy in shard_copies:
                zones_with_copies.add(shard_copy.zone)
                if shard_copy.is_primary:
                    primary_zone = shard_copy.zone
                else:
                    replica_zones.add(shard_copy.zone)

            # Determine status
            status_parts = []
            if len(zones_with_copies) == 1:
                zone_conflicts += 1
                status_parts.append("[red]⚠ ZONE CONFLICT[/red]")

            if total_copies < 2:  # Assuming we want at least 1 replica
                under_replicated += 1
                status_parts.append("[yellow]⚠ Under-replicated[/yellow]")

            if not status_parts:
                status_parts.append("[green]✓ Good[/green]")

            replica_zones_str = ", ".join(sorted(replica_zones)) if replica_zones else "None"

            analysis_table.add_row(
                str(shard_id),
                primary_zone,
                replica_zones_str,
                str(total_copies),
                " ".join(status_parts)
            )

            # Show individual shard details if requested
            if show_shards:
                for shard_copy in shard_copies:
                    health_indicator = "✓" if shard_copy.routing_state == 'STARTED' else "⚠"
                    console.print(f"    {health_indicator} {shard_copy.shard_type} on {shard_copy.node_name} ({shard_copy.zone}) - {shard_copy.routing_state}")

        console.print(analysis_table)

    # Summary
    console.print(f"\n[bold]Zone Analysis Summary:[/bold]")
    console.print(f"  • Tables analyzed: [cyan]{len(tables)}[/cyan]")
    console.print(f"  • Zone conflicts detected: [red]{zone_conflicts}[/red]")
    console.print(f"  • Under-replicated shards: [yellow]{under_replicated}[/yellow]")

    if zone_conflicts > 0:
        console.print(f"\n[red]⚠ Found {zone_conflicts} zone conflicts that need attention![/red]")
        console.print("[dim]Zone conflicts occur when all copies of a shard are in the same zone.[/dim]")
        console.print("[dim]This violates CrateDB's zone-awareness and creates availability risks.[/dim]")

    if under_replicated > 0:
        console.print(f"\n[yellow]⚠ Found {under_replicated} under-replicated shards.[/yellow]")
        console.print("[dim]Consider increasing replication for better availability.[/dim]")

    if zone_conflicts == 0 and under_replicated == 0:
        console.print("\n[green]✓ No critical zone distribution issues detected![/green]")


# @main.command()
# @click.argument('node_name')
# @click.option('--min-free-space', default=100.0, help='Minimum free space required on target nodes in GB (default: 100)')
# @click.option('--dry-run/--execute', default=True, help='Show decommission plan without generating SQL commands (default: True)')
# @click.pass_context
# def decommission(ctx, node_name: str, min_free_space: float, dry_run: bool):
#     """Plan decommissioning of a node by analyzing required shard moves
#
#     NODE_NAME: Name of the node to decommission
#     """
#     client = ctx.obj['client']
#     analyzer = ShardAnalyzer(client)
#
#     mode_text = "PLANNING MODE" if dry_run else "EXECUTION MODE"
#     console.print(Panel.fit(f"[bold blue]Node Decommission Analysis[/bold blue] - [bold {'green' if dry_run else 'red'}]{mode_text}[/bold {'green' if dry_run else 'red'}]"))
#     console.print(f"[dim]Analyzing decommission plan for node: {node_name}[/dim]")
#     console.print()
#
#     # Generate decommission plan
#     plan = analyzer.plan_node_decommission(node_name, min_free_space)
#
#     if 'error' in plan:
#         console.print(f"[red]Error: {plan['error']}[/red]")
#         return
#
#     # Display plan summary
#     summary_table = Table(title=f"Decommission Plan for {node_name}", box=box.ROUNDED)
#     summary_table.add_column("Metric", style="cyan")
#     summary_table.add_column("Value", style="magenta")
#
#     summary_table.add_row("Node", plan['node'])
#     summary_table.add_row("Zone", plan['zone'])
#     summary_table.add_row("Feasible", "[green]✓ Yes[/green]" if plan['feasible'] else "[red]✗ No[/red]")
#     summary_table.add_row("Shards to Move", str(plan['shards_to_move']))
#     summary_table.add_row("Moveable Shards", str(plan['moveable_shards']))
#     summary_table.add_row("Total Data Size", format_size(plan['total_size_gb']))
#     summary_table.add_row("Estimated Time", f"{plan['estimated_time_hours']:.1f} hours")
#
#     console.print(summary_table)
#     console.print()
#
#     # Show warnings if any
#     if plan['warnings']:
#         console.print("[bold yellow]⚠ Warnings:[/bold yellow]")
#         for warning in plan['warnings']:
#             console.print(f"  • [yellow]{warning}[/yellow]")
#         console.print()
#
#     # Show infeasible moves if any
#     if plan['infeasible_moves']:
#         console.print("[bold red]✗ Cannot Move:[/bold red]")
#         infeasible_table = Table(box=box.ROUNDED)
#         infeasible_table.add_column("Shard", style="cyan")
#         infeasible_table.add_column("Size", style="magenta")
#         infeasible_table.add_column("Reason", style="red")
#
#         for move in plan['infeasible_moves']:
#             infeasible_table.add_row(
#                 move['shard'],
#                 format_size(move['size_gb']),
#                 move['reason']
#             )
#         console.print(infeasible_table)
#         console.print()
#
#     # Show move recommendations
#     if plan['recommendations']:
#         move_table = Table(title="Required Shard Moves", box=box.ROUNDED)
#         move_table.add_column("Table", style="cyan")
#         move_table.add_column("Shard", justify="right", style="magenta")
#         move_table.add_column("Type", style="blue")
#         move_table.add_column("Size", style="green")
#         move_table.add_column("From Zone", style="yellow")
#         move_table.add_column("To Node", style="cyan")
#         move_table.add_column("To Zone", style="yellow")
#
#         for rec in plan['recommendations']:
#             move_table.add_row(
#                 f"{rec.schema_name}.{rec.table_name}",
#                 str(rec.shard_id),
#                 rec.shard_type,
#                 format_size(rec.size_gb),
#                 rec.from_zone,
#                 rec.to_node,
#                 rec.to_zone
#             )
#
#         console.print(move_table)
#         console.print()
#
#         # Generate SQL commands if not in dry-run mode
#         if not dry_run and plan['feasible']:
#             console.print(Panel.fit("[bold green]Decommission SQL Commands[/bold green]"))
#             console.print("[dim]# Execute these commands in order to prepare for node decommission[/dim]")
#             console.print("[dim]# ALWAYS test in a non-production environment first![/dim]")
#             console.print("[dim]# Monitor shard health after each move before proceeding[/dim]")
#             console.print()
#
#             for i, rec in enumerate(plan['recommendations'], 1):
#                 console.print(f"-- Move {i}: {rec.reason}")
#                 console.print(f"{rec.to_sql()}")
#                 console.print()
#
#             console.print(f"-- After all moves complete, the node {node_name} can be safely removed")
#             console.print(f"-- Total moves required: {len(plan['recommendations'])}")
#         elif dry_run:
#             console.print("[green]✓ Decommission plan ready. Use --execute to generate SQL commands.[/green]")
#
#     # Final status
#     if not plan['feasible']:
#         console.print(f"[red]⚠ Node {node_name} cannot be safely decommissioned at this time.[/red]")
#         console.print("[dim]Address the issues above before attempting decommission.[/dim]")
#     elif plan['shards_to_move'] == 0:
#         console.print(f"[green]✓ Node {node_name} is ready for immediate decommission (no shards to move).[/green]")
#     else:
#         console.print(f"[green]✓ Node {node_name} can be safely decommissioned after moving {len(plan['recommendations'])} shards.[/green]")


@main.command()
@click.argument('schema_table')
@click.argument('shard_id', type=int)
@click.argument('from_node')
@click.argument('to_node')
@click.option('--max-disk-usage', default=90.0, help='Maximum disk usage percentage for target node (default: 90)')

@click.pass_context
def validate_move(ctx, schema_table: str, shard_id: int, from_node: str, to_node: str, max_disk_usage: float):
    """Validate a specific shard move before execution

    SCHEMA_TABLE: Schema and table name (format: schema.table)
    SHARD_ID: Shard ID to move
    FROM_NODE: Source node name
    TO_NODE: Target node name

    Example: xmover validate-move CUROV.maddoxxFormfactor 4 data-hot-1 data-hot-3
    """
    client = ctx.obj['client']
    analyzer = ShardAnalyzer(client)

    # Parse schema and table
    if '.' not in schema_table:
        console.print("[red]Error: Schema and table must be in format 'schema.table'[/red]")
        return

    schema_name, table_name = schema_table.split('.', 1)

    console.print(Panel.fit(f"[bold blue]Validating Shard Move[/bold blue]"))
    console.print(f"[dim]Move: {schema_name}.{table_name}[{shard_id}] from {from_node} to {to_node}[/dim]")
    console.print()

    # Find the nodes
    from_node_info = None
    to_node_info = None
    for node in analyzer.nodes:
        if node.name == from_node:
            from_node_info = node
        if node.name == to_node:
            to_node_info = node

    if not from_node_info:
        console.print(f"[red]✗ Source node '{from_node}' not found in cluster[/red]")
        return

    if not to_node_info:
        console.print(f"[red]✗ Target node '{to_node}' not found in cluster[/red]")
        return

    # Find the specific shard
    target_shard = None
    for shard in analyzer.shards:
        if (shard.schema_name == schema_name and
            shard.table_name == table_name and
            shard.shard_id == shard_id and
            shard.node_name == from_node):
            target_shard = shard
            break

    if not target_shard:
        console.print(f"[red]✗ Shard {shard_id} not found on node {from_node}[/red]")
        console.print(f"[dim]Use 'xmover find-candidates' to see available shards[/dim]")
        return

    # Create a move recommendation for validation
    recommendation = MoveRecommendation(
        table_name=table_name,
        schema_name=schema_name,
        shard_id=shard_id,
        from_node=from_node,
        to_node=to_node,
        from_zone=from_node_info.zone,
        to_zone=to_node_info.zone,
        shard_type=target_shard.shard_type,
        size_gb=target_shard.size_gb,
        reason="Manual validation"
    )

    # Display shard details
    details_table = Table(title="Shard Details", box=box.ROUNDED)
    details_table.add_column("Property", style="cyan")
    details_table.add_column("Value", style="magenta")

    details_table.add_row("Table", f"{schema_name}.{table_name}")
    details_table.add_row("Shard ID", str(shard_id))
    details_table.add_row("Type", target_shard.shard_type)
    details_table.add_row("Size", format_size(target_shard.size_gb))
    details_table.add_row("Documents", f"{target_shard.num_docs:,}")
    details_table.add_row("State", target_shard.state)
    details_table.add_row("Routing State", target_shard.routing_state)
    details_table.add_row("From Node", f"{from_node} ({from_node_info.zone})")
    details_table.add_row("To Node", f"{to_node} ({to_node_info.zone})")
    details_table.add_row("Zone Change", "Yes" if from_node_info.zone != to_node_info.zone else "No")

    console.print(details_table)
    console.print()

    # Perform comprehensive validation
    is_safe, safety_msg = analyzer.validate_move_safety(recommendation, max_disk_usage_percent=max_disk_usage)

    if is_safe:
        console.print("[green]✓ VALIDATION PASSED - Move appears safe[/green]")
        console.print(f"[green]✓ {safety_msg}[/green]")
        console.print()

        # Show the SQL command
        console.print(Panel.fit("[bold green]Ready to Execute[/bold green]"))
        console.print("[dim]# Copy and paste this command to execute the move[/dim]")
        console.print()
        console.print(f"{recommendation.to_sql()}")
        console.print()
        console.print("[dim]# Monitor shard health after execution[/dim]")
        console.print("[dim]# Check with: SELECT * FROM sys.shards WHERE table_name = '{table_name}' AND id = {shard_id};[/dim]")
    else:
        console.print("[red]✗ VALIDATION FAILED - Move not safe[/red]")
        console.print(f"[red]✗ {safety_msg}[/red]")
        console.print()

        # Provide troubleshooting guidance
        if "zone conflict" in safety_msg.lower():
            console.print("[yellow]💡 Troubleshooting Zone Conflicts:[/yellow]")
            console.print("  • Check current shard distribution: xmover zone-analysis --show-shards")
            console.print("  • Try moving to a different zone")
            console.print("  • Verify cluster has proper zone-awareness configuration")
        elif "node conflict" in safety_msg.lower():
            console.print("[yellow]💡 Troubleshooting Node Conflicts:[/yellow]")
            console.print("  • The target node already has a copy of this shard")
            console.print("  • Choose a different target node")
            console.print("  • Check shard distribution: xmover analyze")
        elif "space" in safety_msg.lower():
            console.print("[yellow]💡 Troubleshooting Space Issues:[/yellow]")
            console.print("  • Free up space on the target node")
            console.print("  • Choose a node with more available capacity")
            console.print("  • Check node capacity: xmover analyze")
        elif "usage" in safety_msg.lower():
            console.print("[yellow]💡 Troubleshooting High Disk Usage:[/yellow]")
            console.print("  • Wait for target node disk usage to decrease")
            console.print("  • Choose a node with lower disk usage")
            console.print("  • Check cluster health: xmover analyze")
            console.print("  • Consider using --max-disk-usage option for urgent moves")


@main.command()
@click.argument('error_message', required=False)
@click.pass_context
def explain_error(ctx, error_message: Optional[str]):
    """Explain CrateDB allocation error messages and provide solutions

    ERROR_MESSAGE: The CrateDB error message to analyze (optional - can be provided interactively)

    Example: xmover explain-error "NO(a copy of this shard is already allocated to this node)"
    """
    console.print(Panel.fit("[bold blue]CrateDB Error Message Decoder[/bold blue]"))
    console.print("[dim]Helps decode and troubleshoot CrateDB shard allocation errors[/dim]")
    console.print()

    if not error_message:
        console.print("Please paste the CrateDB error message (press Enter twice when done):")
        lines = []
        while True:
            try:
                line = input()
                if line.strip() == "" and lines:
                    break
                lines.append(line)
            except (EOFError, KeyboardInterrupt):
                break
        error_message = "\n".join(lines)

    if not error_message.strip():
        console.print("[yellow]No error message provided[/yellow]")
        return

    console.print(f"[dim]Analyzing error message...[/dim]")
    console.print()

    # Common CrateDB allocation error patterns and solutions
    error_patterns = [
        {
            "pattern": "a copy of this shard is already allocated to this node",
            "title": "Node Already Has Shard Copy",
            "explanation": "The target node already contains a copy (primary or replica) of this shard.",
            "solutions": [
                "Choose a different target node that doesn't have this shard",
                "Use 'xmover zone-analysis --show-shards' to see current distribution",
                "Verify the shard ID and table name are correct"
            ],
            "prevention": "Always check current shard locations before moving"
        },
        {
            "pattern": "there are too many copies of the shard allocated to nodes with attribute",
            "title": "Zone Allocation Limit Exceeded",
            "explanation": "CrateDB's zone awareness prevents too many copies in the same zone.",
            "solutions": [
                "Move the shard to a different availability zone",
                "Check zone balance with 'xmover check-balance'",
                "Ensure target zone doesn't already have copies of this shard"
            ],
            "prevention": "Use 'xmover recommend' which respects zone constraints"
        },
        {
            "pattern": "not enough disk space",
            "title": "Insufficient Disk Space",
            "explanation": "The target node doesn't have enough free disk space for the shard.",
            "solutions": [
                "Free up space on the target node",
                "Choose a node with more available capacity",
                "Check available space with 'xmover analyze'"
            ],
            "prevention": "Use '--min-free-space' parameter in recommendations"
        },
        {
            "pattern": "shard recovery limit",
            "title": "Recovery Limit Exceeded",
            "explanation": "Too many shards are currently being moved/recovered simultaneously.",
            "solutions": [
                "Wait for current recoveries to complete",
                "Check recovery status in CrateDB admin UI",
                "Reduce concurrent recoveries in cluster settings"
            ],
            "prevention": "Move shards gradually, monitor recovery progress"
        },
        {
            "pattern": "allocation is disabled",
            "title": "Allocation Disabled",
            "explanation": "Shard allocation is temporarily disabled in the cluster.",
            "solutions": [
                "Re-enable allocation: PUT /_cluster/settings {\"persistent\":{\"cluster.routing.allocation.enable\":\"all\"}}",
                "Check if allocation was disabled for maintenance",
                "Verify cluster health before re-enabling"
            ],
            "prevention": "Check allocation status before performing moves"
        }
    ]

    # Find matching patterns
    matches = []
    error_lower = error_message.lower()

    for pattern_info in error_patterns:
        if pattern_info["pattern"].lower() in error_lower:
            matches.append(pattern_info)

    if matches:
        for i, match in enumerate(matches):
            if i > 0:
                console.print("\n" + "─" * 60 + "\n")

            console.print(f"[bold red]🚨 {match['title']}[/bold red]")
            console.print(f"[yellow]📝 Explanation:[/yellow] {match['explanation']}")
            console.print()

            console.print("[green]💡 Solutions:[/green]")
            for j, solution in enumerate(match['solutions'], 1):
                console.print(f"  {j}. {solution}")
            console.print()

            console.print(f"[blue]🛡️ Prevention:[/blue] {match['prevention']}")
    else:
        console.print("[yellow]⚠ No specific pattern match found[/yellow]")
        console.print()
        console.print("[bold]General Troubleshooting Steps:[/bold]")
        console.print("1. Check current shard distribution: [cyan]xmover analyze[/cyan]")
        console.print("2. Validate the specific move: [cyan]xmover validate-move schema.table shard_id from_node to_node[/cyan]")
        console.print("3. Check zone conflicts: [cyan]xmover zone-analysis --show-shards[/cyan]")
        console.print("4. Verify node capacity: [cyan]xmover analyze[/cyan]")
        console.print("5. Review CrateDB documentation on shard allocation")

    console.print()
    console.print("[dim]💡 Tip: Use 'xmover validate-move' to check moves before execution[/dim]")
    console.print("[dim]📚 For more help: https://crate.io/docs/crate/reference/en/latest/admin/system-information.html[/dim]")


@main.command()
@click.option('--table', '-t', help='Monitor recovery for specific table only')
@click.option('--node', '-n', help='Monitor recovery on specific node only')
@click.option('--watch', '-w', is_flag=True, help='Continuously monitor (refresh every 10s)')
@click.option('--refresh-interval', default=10, help='Refresh interval for watch mode (seconds)')
@click.option('--recovery-type', type=click.Choice(['PEER', 'DISK', 'all']), default='all', help='Filter by recovery type')
@click.option('--include-transitioning', is_flag=True, help='Include completed recoveries still in transitioning state')
@click.pass_context
def monitor_recovery(ctx, table: str, node: str, watch: bool, refresh_interval: int, recovery_type: str, include_transitioning: bool):
    """Monitor active shard recovery operations on the cluster

    This command monitors ongoing shard recoveries by querying sys.allocations
    and sys.shards tables. It shows recovery progress, type (PEER/DISK), and timing.

    By default, only shows actively progressing recoveries. Use --include-transitioning
    to also see completed recoveries that haven't fully transitioned to STARTED state.

    Examples:
        xmover monitor-recovery                        # Show active recoveries only
        xmover monitor-recovery --include-transitioning # Show active + transitioning
        xmover monitor-recovery --table myTable       # Monitor specific table
        xmover monitor-recovery --watch                # Continuous monitoring
        xmover monitor-recovery --recovery-type PEER  # Only PEER recoveries
    """
    try:
        client = ctx.obj['client']
        recovery_monitor = RecoveryMonitor(client)

        if watch:

            console.print(f"🔄 Monitoring shard recoveries (refreshing every {refresh_interval}s)")
            console.print("Press Ctrl+C to stop")
            console.print()

            try:
                # Show header once
                console.print("📊 Recovery Progress Monitor")
                console.print("=" * 80)

                # Track previous state for change detection
                previous_recoveries = {}
                previous_timestamp = None
                last_transitioning_display = None
                first_run = True

                while True:
                    # Get current recovery status
                    recoveries = recovery_monitor.get_cluster_recovery_status(
                        table_name=table,
                        node_name=node,
                        recovery_type_filter=recovery_type,
                        include_transitioning=include_transitioning
                    )

                    # Display current time
                    from datetime import datetime
                    current_time = datetime.now().strftime("%H:%M:%S")

                    # Check for any changes
                    changes = []
                    active_count = 0
                    completed_count = 0

                    for recovery in recoveries:
                        recovery_key = f"{recovery.schema_name}.{recovery.table_name}.{recovery.shard_id}.{recovery.node_name}"

                        # Create complete table name
                        table_display = format_table_display_with_partition(
                            recovery.schema_name, recovery.table_name, recovery.partition_values
                        )

                        # Count active vs completed
                        if recovery.stage == "DONE" and recovery.overall_progress >= 100.0:
                            completed_count += 1
                        else:
                            active_count += 1

                        # Check for changes since last update
                        if recovery_key in previous_recoveries:
                            prev = previous_recoveries[recovery_key]
                            if prev['progress'] != recovery.overall_progress:
                                diff = recovery.overall_progress - prev['progress']
                                # Create node route display
                                node_route = ""
                                if recovery.recovery_type == "PEER" and recovery.source_node_name:
                                    node_route = f" {recovery.source_node_name} → {recovery.node_name}"
                                elif recovery.recovery_type == "DISK":
                                    node_route = f" disk → {recovery.node_name}"

                                # Add translog info
                                translog_info = format_translog_info(recovery)
                                
                                if diff > 0:
                                    table_display = format_table_display_with_partition(
                                        recovery.schema_name, recovery.table_name, recovery.partition_values
                                    )
                                    progress_info = format_recovery_progress(recovery)
                                    changes.append(f"[green]📈[/green] {table_display} S{recovery.shard_id} {recovery.recovery_type} {progress_info} (+{diff:.1f}%) {recovery.size_gb:.1f}GB{translog_info}{node_route}")
                                else:
                                    table_display = format_table_display_with_partition(
                                        recovery.schema_name, recovery.table_name, recovery.partition_values
                                    )
                                    progress_info = format_recovery_progress(recovery)
                                    changes.append(f"[yellow]📉[/yellow] {table_display} S{recovery.shard_id} {recovery.recovery_type} {progress_info} ({diff:.1f}%) {recovery.size_gb:.1f}GB{translog_info}{node_route}")
                            elif prev['stage'] != recovery.stage:
                                # Create node route display
                                node_route = ""
                                if recovery.recovery_type == "PEER" and recovery.source_node_name:
                                    node_route = f" {recovery.source_node_name} → {recovery.node_name}"
                                elif recovery.recovery_type == "DISK":
                                    node_route = f" disk → {recovery.node_name}"

                                # Add translog info
                                translog_info = format_translog_info(recovery)
                                
                                table_display = format_table_display_with_partition(
                                    recovery.schema_name, recovery.table_name, recovery.partition_values
                                )
                                progress_info = format_recovery_progress(recovery)
                                changes.append(f"[blue]🔄[/blue] {table_display} S{recovery.shard_id} {recovery.recovery_type} {prev['stage']}→{recovery.stage} {progress_info} {recovery.size_gb:.1f}GB{translog_info}{node_route}")
                        else:
                            # New recovery - show based on include_transitioning flag or first run
                            if first_run or include_transitioning or (recovery.overall_progress < 100.0 or recovery.stage != "DONE"):
                                # Create node route display
                                node_route = ""
                                if recovery.recovery_type == "PEER" and recovery.source_node_name:
                                    node_route = f" {recovery.source_node_name} → {recovery.node_name}"
                                elif recovery.recovery_type == "DISK":
                                    node_route = f" disk → {recovery.node_name}"

                                status_icon = "[cyan]🆕[/cyan]" if not first_run else "[blue]📋[/blue]"
                                # Add translog info
                                translog_info = format_translog_info(recovery)
                                
                                table_display = format_table_display_with_partition(
                                    recovery.schema_name, recovery.table_name, recovery.partition_values
                                )
                                progress_info = format_recovery_progress(recovery)
                                changes.append(f"{status_icon} {table_display} S{recovery.shard_id} {recovery.recovery_type} {recovery.stage} {progress_info} {recovery.size_gb:.1f}GB{translog_info}{node_route}")

                        # Store current state for next comparison
                        previous_recoveries[recovery_key] = {
                            'progress': recovery.overall_progress,
                            'stage': recovery.stage
                        }

                    # Get problematic shards for comprehensive status
                    problematic_shards = recovery_monitor.get_problematic_shards(table, node)
                    
                    # Filter out shards that are already being recovered
                    non_recovering_shards = []
                    if problematic_shards:
                        for shard in problematic_shards:
                            # Check if this shard is already in our recoveries list
                            is_recovering = any(
                                r.shard_id == shard['shard_id'] and 
                                r.table_name == shard['table_name'] and 
                                r.schema_name == shard['schema_name']
                                for r in recoveries
                            )
                            if not is_recovering:
                                non_recovering_shards.append(shard)
                    
                    # Always show a comprehensive status line
                    if not recoveries and not non_recovering_shards:
                        console.print(f"{current_time} | [green]No issues - cluster stable[/green]")
                        previous_recoveries.clear()
                    elif not recoveries and non_recovering_shards:
                        console.print(f"{current_time} | [yellow]{len(non_recovering_shards)} shards need attention (not recovering)[/yellow]")
                        # Show first few problematic shards
                        for shard in non_recovering_shards[:5]:
                            table_display = format_table_display_with_partition(
                                shard['schema_name'], shard['table_name'], shard.get('partition_values')
                            )
                            primary_indicator = "P" if shard.get('primary') else "R"
                            console.print(f"         | [red]⚠[/red] {table_display} S{shard['shard_id']}{primary_indicator} {shard['state']}")
                        if len(non_recovering_shards) > 5:
                            console.print(f"         | [dim]... and {len(non_recovering_shards) - 5} more[/dim]")
                        previous_recoveries.clear()
                    else:
                        # Build status message for active recoveries
                        status_parts = []
                        if active_count > 0:
                            status_parts.append(f"{active_count} recovering")
                        if completed_count > 0:
                            status_parts.append(f"{completed_count} done")
                        if non_recovering_shards:
                            status_parts.append(f"[yellow]{len(non_recovering_shards)} awaiting recovery[/yellow]")
                        
                        status = " | ".join(status_parts)

                        # Show status line with changes or periodic update
                        if changes:
                            console.print(f"{current_time} | {status}")
                            for change in changes:
                                console.print(f"         | {change}")
                            # Show some problematic shards if there are any
                            if non_recovering_shards and len(changes) < 3:  # Don't overwhelm the output
                                for shard in non_recovering_shards[:2]:
                                    table_display = format_table_display_with_partition(
                                        shard['schema_name'], shard['table_name'], shard.get('partition_values')
                                    )
                                    primary_indicator = "P" if shard.get('primary') else "R"
                                    console.print(f"         | [red]⚠[/red] {table_display} S{shard['shard_id']}{primary_indicator} {shard['state']}")
                        else:
                            # Show periodic status even without changes
                            if include_transitioning and completed_count > 0:
                                from datetime import datetime, timedelta
                                current_dt = datetime.now()
                                
                                # Show transitioning details every 30 seconds or first time
                                should_show_details = (
                                    last_transitioning_display is None or 
                                    (current_dt - last_transitioning_display).total_seconds() >= 30
                                )
                                
                                if should_show_details:
                                    console.print(f"{current_time} | {status} (transitioning)")
                                    # Show details of transitioning recoveries
                                    transitioning_recoveries = [r for r in recoveries if r.stage == "DONE" and r.overall_progress >= 100.0]
                                    for recovery in transitioning_recoveries[:5]:  # Limit to first 5 to avoid spam
                                        # Create node route display
                                        node_route = ""
                                        if recovery.recovery_type == "PEER" and recovery.source_node_name:
                                            node_route = f" {recovery.source_node_name} → {recovery.node_name}"
                                        elif recovery.recovery_type == "DISK":
                                            node_route = f" disk → {recovery.node_name}"
                                        
                                        # Add translog info
                                        translog_info = format_translog_info(recovery)
                                        
                                        table_display = format_table_display_with_partition(
                                            recovery.schema_name, recovery.table_name, recovery.partition_values
                                        )
                                        progress_info = format_recovery_progress(recovery)
                                        primary_indicator = "P" if recovery.is_primary else "R"
                                        console.print(f"         | [cyan]🔄[/cyan] {table_display} S{recovery.shard_id}{primary_indicator} {recovery.recovery_type} {recovery.stage} {progress_info} {recovery.size_gb:.1f}GB{translog_info}{node_route}")
                                    
                                    if len(transitioning_recoveries) > 5:
                                        console.print(f"         | [dim]... and {len(transitioning_recoveries) - 5} more transitioning[/dim]")
                                    
                                    last_transitioning_display = current_dt
                                else:
                                    console.print(f"{current_time} | {status} (transitioning)")
                            elif active_count > 0:
                                console.print(f"{current_time} | {status} (no changes)")
                            elif non_recovering_shards:
                                console.print(f"{current_time} | {status} (issues persist)")

                    previous_timestamp = current_time
                    first_run = False
                    time.sleep(refresh_interval)

            except KeyboardInterrupt:
                console.print("\n\n[yellow]⏹  Monitoring stopped by user[/yellow]")

                # Show final summary
                final_recoveries = recovery_monitor.get_cluster_recovery_status(
                    table_name=table,
                    node_name=node,
                    recovery_type_filter=recovery_type,
                    include_transitioning=include_transitioning
                )
                
                final_problematic_shards = recovery_monitor.get_problematic_shards(table, node)
                
                # Filter out shards that are already being recovered
                final_non_recovering_shards = []
                if final_problematic_shards:
                    for shard in final_problematic_shards:
                        is_recovering = any(
                            r.shard_id == shard['shard_id'] and 
                            r.table_name == shard['table_name'] and 
                            r.schema_name == shard['schema_name']
                            for r in final_recoveries
                        )
                        if not is_recovering:
                            final_non_recovering_shards.append(shard)

                if final_recoveries or final_non_recovering_shards:
                    console.print("\n📊 [bold]Final Cluster Status Summary:[/bold]")
                    
                    if final_recoveries:
                        summary = recovery_monitor.get_recovery_summary(final_recoveries)
                        # Count active vs completed
                        active_count = len([r for r in final_recoveries if r.overall_progress < 100.0 or r.stage != "DONE"])
                        completed_count = len(final_recoveries) - active_count

                        console.print(f"   Total recoveries: {summary['total_recoveries']}")
                        console.print(f"   Active: {active_count}, Completed: {completed_count}")
                        console.print(f"   Total size: {summary['total_size_gb']:.1f} GB")
                        console.print(f"   Average progress: {summary['avg_progress']:.1f}%")

                        if summary['by_type']:
                            console.print(f"   By recovery type:")
                            for rec_type, stats in summary['by_type'].items():
                                console.print(f"     {rec_type}: {stats['count']} recoveries, {stats['avg_progress']:.1f}% avg progress")
                    
                    if final_non_recovering_shards:
                        console.print(f"   [yellow]Problematic shards needing attention: {len(final_non_recovering_shards)}[/yellow]")
                        # Group by state for summary
                        by_state = {}
                        for shard in final_non_recovering_shards:
                            state = shard['state']
                            if state not in by_state:
                                by_state[state] = 0
                            by_state[state] += 1
                        
                        for state, count in by_state.items():
                            console.print(f"     {state}: {count} shards")
                else:
                    console.print("\n[green]✅ Cluster stable - no issues detected[/green]")

                return

        else:
            # Single status check
            recoveries = recovery_monitor.get_cluster_recovery_status(
                table_name=table,
                node_name=node,
                recovery_type_filter=recovery_type,
                include_transitioning=include_transitioning
            )

            display_output = recovery_monitor.format_recovery_display(recoveries)
            console.print(display_output)

            # Get problematic shards for comprehensive status
            problematic_shards = recovery_monitor.get_problematic_shards(table, node)
            
            # Filter out shards that are already being recovered
            non_recovering_shards = []
            if problematic_shards:
                for shard in problematic_shards:
                    is_recovering = any(
                        r.shard_id == shard['shard_id'] and 
                        r.table_name == shard['table_name'] and 
                        r.schema_name == shard['schema_name']
                        for r in recoveries
                    )
                    if not is_recovering:
                        non_recovering_shards.append(shard)

            if not recoveries and not non_recovering_shards:
                if include_transitioning:
                    console.print("\n[green]✅ No issues found - cluster stable[/green]")
                else:
                    console.print("\n[green]✅ No active recoveries found[/green]")
                    console.print("[dim]💡 Use --include-transitioning to see completed recoveries still transitioning[/dim]")
            elif not recoveries and non_recovering_shards:
                console.print(f"\n[yellow]⚠️ {len(non_recovering_shards)} shards need attention (not recovering)[/yellow]")
                # Group by state for summary
                by_state = {}
                for shard in non_recovering_shards:
                    state = shard['state']
                    if state not in by_state:
                        by_state[state] = 0
                    by_state[state] += 1
                
                for state, count in by_state.items():
                    console.print(f"   {state}: {count} shards")
                    
                # Show first few examples
                console.print(f"\nExamples:")
                for shard in non_recovering_shards[:5]:
                    table_display = format_table_display_with_partition(
                        shard['schema_name'], shard['table_name'], shard.get('partition_values')
                    )
                    primary_indicator = "P" if shard.get('primary') else "R"
                    console.print(f"   [red]⚠[/red] {table_display} S{shard['shard_id']}{primary_indicator} {shard['state']}")
                    
                if len(non_recovering_shards) > 5:
                    console.print(f"   [dim]... and {len(non_recovering_shards) - 5} more[/dim]")
            else:
                # Show recovery summary
                summary = recovery_monitor.get_recovery_summary(recoveries)
                console.print(f"\n📊 [bold]Cluster Status Summary:[/bold]")
                console.print(f"   Active recoveries: {summary['total_recoveries']}")
                console.print(f"   Total recovery size: {summary['total_size_gb']:.1f} GB")
                console.print(f"   Average progress: {summary['avg_progress']:.1f}%")

                # Show breakdown by type
                if summary['by_type']:
                    console.print(f"\n   By recovery type:")
                    for rec_type, stats in summary['by_type'].items():
                        console.print(f"     {rec_type}: {stats['count']} recoveries, {stats['avg_progress']:.1f}% avg progress")

                # Show problematic shards if any
                if non_recovering_shards:
                    console.print(f"\n   [yellow]Problematic shards needing attention: {len(non_recovering_shards)}[/yellow]")
                    by_state = {}
                    for shard in non_recovering_shards:
                        state = shard['state']
                        if state not in by_state:
                            by_state[state] = 0
                        by_state[state] += 1
                    
                    for state, count in by_state.items():
                        console.print(f"     {state}: {count} shards")

                console.print(f"\n[dim]💡 Use --watch flag for continuous monitoring[/dim]")

    except Exception as e:
        console.print(f"[red]❌ Error monitoring recoveries: {e}[/red]")
        if ctx.obj.get('debug'):
            raise


def _wait_for_recovery_capacity(client, max_concurrent_recoveries: int = 5):
    """Wait until active recovery count is below threshold"""
    from xmover.analyzer import RecoveryMonitor
    from time import sleep
    
    recovery_monitor = RecoveryMonitor(client)
    wait_time = 0
    
    while True:
        # Check active recoveries (including transitioning)
        recoveries = recovery_monitor.get_cluster_recovery_status(include_transitioning=True)
        active_count = len([r for r in recoveries if r.overall_progress < 100.0 or r.stage != "DONE"])
        
        if active_count < max_concurrent_recoveries:
            if wait_time > 0:
                console.print(f"    [green]✓ Recovery capacity available ({active_count}/{max_concurrent_recoveries} active)[/green]")
            break
        else:
            if wait_time == 0:
                console.print(f"    [yellow]⏳ Waiting for recovery capacity... ({active_count}/{max_concurrent_recoveries} active)[/yellow]")
            elif wait_time % 30 == 0:  # Update every 30 seconds
                console.print(f"    [yellow]⏳ Still waiting... ({active_count}/{max_concurrent_recoveries} active)[/yellow]")
            
            sleep(10)  # Check every 10 seconds
            wait_time += 10


def _execute_recommendations_safely(client, recommendations, validate: bool):
    """Execute recommendations with extensive safety measures"""
    from time import sleep
    import sys
    from xmover.analyzer import ShardAnalyzer
    
    # Filter to only safe recommendations
    safe_recommendations = []
    if validate:
        analyzer = ShardAnalyzer(client)
        for rec in recommendations:
            is_safe, safety_msg = analyzer.validate_move_safety(rec, max_disk_usage_percent=95.0)
            if is_safe:
                safe_recommendations.append(rec)
    else:
        safe_recommendations = recommendations
    
    if not safe_recommendations:
        console.print("[yellow]⚠ No safe recommendations to execute[/yellow]")
        return
    
    console.print(f"\n[bold red]🚨 AUTO-EXECUTION MODE 🚨[/bold red]")
    console.print(f"About to execute {len(safe_recommendations)} shard moves automatically:")
    console.print()
    
    # Show what will be executed
    for i, rec in enumerate(safe_recommendations, 1):
        table_display = f"{rec.schema_name}.{rec.table_name}" if rec.schema_name != "doc" else rec.table_name
        console.print(f"  {i}. {table_display} S{rec.shard_id} ({rec.size_gb:.1f}GB) {rec.from_node} → {rec.to_node}")
    
    console.print()
    console.print("[bold yellow]⚠ SAFETY WARNINGS:[/bold yellow]")
    console.print("  • These commands will immediately start shard movements")
    console.print("  • Each move will temporarily impact cluster performance")
    console.print("  • Recovery time depends on shard size and network speed")
    console.print("  • You should monitor progress with: xmover monitor-recovery --watch")
    console.print()
    
    # Double confirmation
    try:
        response1 = input("Type 'EXECUTE' to proceed with automatic execution: ").strip()
        if response1 != "EXECUTE":
            console.print("[yellow]❌ Execution cancelled[/yellow]")
            return
        
        response2 = input(f"Confirm: Execute {len(safe_recommendations)} shard moves? (yes/no): ").strip().lower()
        if response2 not in ['yes', 'y']:
            console.print("[yellow]❌ Execution cancelled[/yellow]")
            return
            
    except KeyboardInterrupt:
        console.print("\n[yellow]❌ Execution cancelled by user[/yellow]")
        return
    
    console.print(f"\n🚀 [bold green]Executing {len(safe_recommendations)} shard moves...[/bold green]")
    console.print()
    
    successful_moves = 0
    failed_moves = 0
    
    for i, rec in enumerate(safe_recommendations, 1):
        table_display = f"{rec.schema_name}.{rec.table_name}" if rec.schema_name != "doc" else rec.table_name
        sql_command = rec.to_sql()
        
        console.print(f"[{i}/{len(safe_recommendations)}] Executing: {table_display} S{rec.shard_id} ({rec.size_gb:.1f}GB)")
        console.print(f"    {rec.from_node} → {rec.to_node}")
        
        try:
            # Execute the SQL command
            result = client.execute_query(sql_command)
            
            if result.get('rowcount', 0) >= 0:  # Success indicator for ALTER statements
                console.print(f"    [green]✅ SUCCESS[/green] - Move initiated")
                successful_moves += 1
                
                # Smart delay: check active recoveries before next move
                if i < len(safe_recommendations):
                    _wait_for_recovery_capacity(client, max_concurrent_recoveries=5)
            else:
                console.print(f"    [red]❌ FAILED[/red] - Unexpected result: {result}")
                failed_moves += 1
                
        except Exception as e:
            console.print(f"    [red]❌ FAILED[/red] - Error: {e}")
            failed_moves += 1
            
            # Ask whether to continue after a failure
            if i < len(safe_recommendations):
                try:
                    continue_response = input(f"    Continue with remaining {len(safe_recommendations) - i} moves? (yes/no): ").strip().lower()
                    if continue_response not in ['yes', 'y']:
                        console.print("[yellow]⏹ Execution stopped by user[/yellow]")
                        break
                except KeyboardInterrupt:
                    console.print("\n[yellow]⏹ Execution stopped by user[/yellow]")
                    break
        
        console.print()
    
    # Final summary
    console.print(f"📊 [bold]Execution Summary:[/bold]")
    console.print(f"   Successful moves: [green]{successful_moves}[/green]")
    console.print(f"   Failed moves: [red]{failed_moves}[/red]")
    console.print(f"   Total attempted: {successful_moves + failed_moves}")
    
    if successful_moves > 0:
        console.print()
        console.print("[green]✅ Shard moves initiated successfully![/green]")
        console.print("[dim]💡 Monitor progress with:[/dim]")
        console.print("[dim]   xmover monitor-recovery --watch[/dim]")
        console.print("[dim]💡 Check cluster status with:[/dim]")
        console.print("[dim]   xmover analyze[/dim]")
    
    if failed_moves > 0:
        console.print()
        console.print(f"[yellow]⚠ {failed_moves} moves failed - check cluster status and retry if needed[/yellow]")


@main.command()
@click.option('--top-tables', default=10, help='Number of largest tables to analyze (default: 10)')
@click.option('--table', help='Analyze specific table only (e.g., "my_table" or "schema.table")')
@click.pass_context
def shard_distribution(ctx, top_tables: int, table: Optional[str]):
    """Analyze shard distribution anomalies across cluster nodes
    
    This command analyzes the largest tables in your cluster to detect:
    • Uneven shard count distribution between nodes
    • Storage imbalances across nodes
    • Missing node coverage for tables
    • Document count imbalances indicating data skew
    
    Results are ranked by impact and severity to help prioritize fixes.
    
    Examples:
        xmover shard-distribution                    # Analyze top 10 tables
        xmover shard-distribution --top-tables 20   # Analyze top 20 tables
        xmover shard-distribution --table my_table  # Detailed report for specific table
    """
    try:
        client = ctx.obj['client']
        analyzer = DistributionAnalyzer(client)
        
        if table:
            # Focused table analysis mode
            console.print(f"[blue]🔍 Analyzing table: {table}...[/blue]")
            
            # Find table (handles schema auto-detection)
            table_identifier = analyzer.find_table_by_name(table)
            if not table_identifier:
                console.print(f"[red]❌ Table '{table}' not found[/red]")
                return
            
            # Get detailed distribution
            table_dist = analyzer.get_table_distribution_detailed(table_identifier)
            if not table_dist:
                console.print(f"[red]❌ No shard data found for table '{table_identifier}'[/red]")
                return
            
            # Display comprehensive health report
            analyzer.format_table_health_report(table_dist)
            
        else:
            # General anomaly detection mode
            console.print(f"[blue]🔍 Analyzing shard distribution for top {top_tables} tables...[/blue]")
            console.print()
            
            # Perform analysis
            anomalies, tables_analyzed = analyzer.analyze_distribution(top_tables)
            
            # Display results
            analyzer.format_distribution_report(anomalies, tables_analyzed)
        
    except KeyboardInterrupt:
        console.print("\n[yellow]Analysis interrupted by user[/yellow]")
    except Exception as e:
        console.print(f"[red]Error during distribution analysis: {e}[/red]")
        import traceback
        console.print(f"[dim]{traceback.format_exc()}[/dim]")


@main.command()
@click.option('--count', default=10, help='Number of most active shards to show (default: 10)')
@click.option('--interval', default=30, help='Observation interval in seconds (default: 30)')
@click.option('--min-checkpoint-delta', default=1000, help='Minimum checkpoint progression between snapshots to show shard (default: 1000)')
@click.option('--table', '-t', help='Monitor specific table only')
@click.option('--node', '-n', help='Monitor specific node only')
@click.option('--watch', '-w', is_flag=True, help='Continuously monitor (refresh every interval)')
@click.option('--exclude-system', is_flag=True, help='Exclude system tables (gc.*, information_schema.*)')
@click.option('--min-rate', type=float, help='Minimum activity rate (changes/sec) to show')
@click.option('--show-replicas/--hide-replicas', default=True, help='Show replica shards (default: True)')
@click.pass_context
def active_shards(ctx, count: int, interval: int, min_checkpoint_delta: int, 
                 table: Optional[str], node: Optional[str], watch: bool,
                 exclude_system: bool, min_rate: Optional[float], show_replicas: bool):
    """Monitor most active shards by checkpoint progression
    
    This command takes two snapshots of ALL started shards separated by the
    observation interval, then shows the shards with the highest checkpoint
    progression (activity) between the snapshots.
    
    Unlike other commands, this tracks ALL shards and filters based on actual
    activity between snapshots, not current state. This captures shards that
    become active during the observation period.
    
    Useful for identifying which shards are receiving the most write activity
    in your cluster and understanding write patterns.
    
    Examples:
        xmover active-shards --count 20 --interval 60        # Top 20 over 60 seconds
        xmover active-shards --watch --interval 30           # Continuous monitoring
        xmover active-shards --table my_table --watch        # Monitor specific table
        xmover active-shards --node data-hot-1 --count 5     # Top 5 on specific node
        xmover active-shards --min-checkpoint-delta 500      # Lower activity threshold
        xmover active-shards --exclude-system --min-rate 50  # Skip system tables, min 50/sec
        xmover active-shards --hide-replicas --count 20      # Only primary shards
    """
    client = ctx.obj['client']
    monitor = ActiveShardMonitor(client)
    
    def get_filtered_snapshot():
        """Get snapshot with optional filtering"""
        snapshots = client.get_active_shards_snapshot(min_checkpoint_delta=min_checkpoint_delta)
        
        # Apply table filter if specified
        if table:
            snapshots = [s for s in snapshots if s.table_name == table or 
                        f"{s.schema_name}.{s.table_name}" == table]
        
        # Apply node filter if specified  
        if node:
            snapshots = [s for s in snapshots if s.node_name == node]
        
        # Exclude system tables if requested
        if exclude_system:
            snapshots = [s for s in snapshots if not (
                s.schema_name.startswith('gc.') or 
                s.schema_name == 'information_schema' or
                s.schema_name == 'sys' or
                s.table_name.endswith('_events') or
                s.table_name.endswith('_log')
            )]
            
        return snapshots
    
    def run_single_analysis():
        """Run a single analysis cycle"""
        if not watch:
            console.print(Panel.fit("[bold blue]Active Shards Monitor[/bold blue]"))
        
        # Show configuration - simplified for watch mode
        if watch:
            config_parts = [f"{interval}s interval", f"threshold: {min_checkpoint_delta:,}", f"top {count}"]
            if table:
                config_parts.append(f"table: {table}")
            if node:
                config_parts.append(f"node: {node}")
            console.print(f"[dim]{' | '.join(config_parts)}[/dim]")
        else:
            config_info = [
                f"Observation interval: {interval}s",
                f"Min checkpoint delta: {min_checkpoint_delta:,}",
                f"Show count: {count}"
            ]
            if table:
                config_info.append(f"Table filter: {table}")
            if node:
                config_info.append(f"Node filter: {node}")
            if exclude_system:
                config_info.append("Excluding system tables")
            if min_rate:
                config_info.append(f"Min rate: {min_rate}/sec")
            if not show_replicas:
                config_info.append("Primary shards only")
                
            console.print("[dim]" + " | ".join(config_info) + "[/dim]")
        console.print()
        
        # Take first snapshot
        if not watch:
            console.print("📷 Taking first snapshot...")
        snapshot1 = get_filtered_snapshot()
        
        if not snapshot1:
            console.print("[yellow]No started shards found matching criteria[/yellow]")
            return
            
        if not watch:
            console.print(f"   Tracking {len(snapshot1)} started shards for activity")
            console.print(f"⏱️  Waiting {interval} seconds for activity...")
        
        # Wait for observation interval
        if watch:
            # Simplified countdown for watch mode
            for remaining in range(interval, 0, -1):
                if remaining % 5 == 0 or remaining <= 3:  # Show fewer updates
                    console.print(f"[dim]⏱️  {remaining}s...[/dim]", end="\r")
                time.sleep(1)
            console.print(" " * 15, end="\r")  # Clear countdown
        else:
            time.sleep(interval)
        
        # Take second snapshot
        if not watch:
            console.print("📷 Taking second snapshot...")
        snapshot2 = get_filtered_snapshot()
        
        if not snapshot2:
            console.print("[yellow]No started shards found in second snapshot[/yellow]")
            return
            
        if not watch:
            console.print(f"   Tracking {len(snapshot2)} started shards for activity")
        
        # Compare snapshots and show results
        activities = monitor.compare_snapshots(snapshot1, snapshot2, min_activity_threshold=min_checkpoint_delta)
        
        # Apply additional filters
        if not show_replicas:
            activities = [a for a in activities if a.is_primary]
        
        if min_rate:
            activities = [a for a in activities if a.activity_rate >= min_rate]
        
        if not activities:
            console.print(f"[green]✅ No shards exceeded activity threshold ({min_checkpoint_delta:,} checkpoint changes)[/green]")
            if min_rate:
                console.print(f"[dim]Also filtered by minimum rate: {min_rate}/sec[/dim]")
        else:
            if not watch:
                overlap_count = len(set(s.shard_identifier for s in snapshot1) & 
                               set(s.shard_identifier for s in snapshot2))
                console.print(f"[dim]Analyzed {overlap_count} shards present in both snapshots[/dim]")
            console.print(monitor.format_activity_display(activities, show_count=count, watch_mode=watch))
    
    try:
        if watch:
            console.print("[dim]Press Ctrl+C to stop monitoring[/dim]")
            console.print()
            
            while True:
                run_single_analysis()
                if watch:
                    console.print(f"\n[dim]━━━ Next update in {interval}s ━━━[/dim]\n")
                time.sleep(interval)
        else:
            run_single_analysis()
            
    except KeyboardInterrupt:
        console.print("\n[yellow]Monitoring stopped by user[/yellow]")
    except Exception as e:
        console.print(f"[red]Error during active shards monitoring: {e}[/red]")
        import traceback
        console.print(f"[dim]{traceback.format_exc()}[/dim]")


@main.command()
@click.option('--sizeMB', default=300, help='Minimum translog uncommitted size in MB (default: 300)')
@click.option('--execute', is_flag=True, help='Execute the replica commands after confirmation')
@click.pass_context
def problematic_translogs(ctx, sizemb: int, execute: bool):
    """Find tables with problematic translog sizes and generate comprehensive shard management commands
    
    This command identifies tables with replica shards that have large uncommitted translog sizes
    indicating replication issues. It generates a complete sequence including:
    1. Stop automatic shard rebalancing
    2. REROUTE CANCEL commands for problematic shards  
    3. Set replicas to 0 commands
    4. Retention lease queries for monitoring
    5. Set replicas to 1 commands (restored from original values)
    6. Re-enable automatic shard rebalancing
    With --execute, it runs them after confirmation.
    """
    client = ctx.obj['client']
    
    console.print(Panel.fit("[bold blue]Problematic Translog Analysis[/bold blue]"))
    console.print(f"[dim]Looking for tables with replica shards having translog uncommitted size > {sizemb}MB[/dim]")
    console.print()
    
    # First query to get individual problematic shards for REROUTE CANCEL commands
    individual_shards_query = """
        SELECT
            sh.schema_name,
            sh.table_name,
            translate(p.values::text, ':{}', '=()') as partition_values,
            sh.id AS shard_id,
            node['name'] AS node_name,
            sh.translog_stats['uncommitted_size'] / 1024^2 AS translog_uncommitted_mb
        FROM
            sys.shards AS sh
        LEFT JOIN information_schema.table_partitions p
            ON sh.table_name = p.table_name
            AND sh.schema_name = p.table_schema
            AND sh.partition_ident = p.partition_ident
        WHERE
            sh.state = 'STARTED'
            AND sh.translog_stats['uncommitted_size'] > ? * 1024^2
            AND primary=FALSE
        ORDER BY
            translog_uncommitted_mb DESC
    """
    
    # Query to find tables with problematic replica shards, grouped by table/partition
    summary_query = """
        SELECT
            all_shards.schema_name,
            all_shards.table_name,
            translate(p.values::text, ':{}', '=()') as partition_values,
            p.partition_ident,
            COUNT(CASE WHEN all_shards.primary=FALSE AND all_shards.translog_stats['uncommitted_size'] > ? * 1024^2 THEN 1 END) as problematic_replica_shards,
            MAX(CASE WHEN all_shards.primary=FALSE AND all_shards.translog_stats['uncommitted_size'] > ? * 1024^2 THEN all_shards.translog_stats['uncommitted_size'] / 1024^2 END) AS max_translog_uncommitted_mb,
            COUNT(CASE WHEN all_shards.primary=TRUE THEN 1 END) as total_primary_shards,
            COUNT(CASE WHEN all_shards.primary=FALSE THEN 1 END) as total_replica_shards,
            SUM(CASE WHEN all_shards.primary=TRUE THEN all_shards.size / 1024^3 ELSE 0 END) as total_primary_size_gb,
            SUM(CASE WHEN all_shards.primary=FALSE THEN all_shards.size / 1024^3 ELSE 0 END) as total_replica_size_gb
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
                AND sh.translog_stats['uncommitted_size'] > ? * 1024^2
                AND sh.primary=FALSE
            )
        GROUP BY
            all_shards.schema_name, all_shards.table_name, partition_values, p.partition_ident
        ORDER BY
            max_translog_uncommitted_mb DESC
    """
    
    try:
        # Get individual shards first
        individual_result = client.execute_query(individual_shards_query, [sizemb])
        individual_shards = individual_result.get('rows', [])
        
        # Get summary data
        summary_result = client.execute_query(summary_query, [sizemb, sizemb, sizemb])
        summary_rows = summary_result.get('rows', [])
        
        if not individual_shards:
            console.print(f"[green]✓ No tables found with replica shards having translog uncommitted size > {sizemb}MB[/green]")
            return
        
        # Display individual problematic shards first
        console.print(f"[bold]Problematic Replica Shards (translog > {sizemb}MB)[/bold]")
        from rich.table import Table
        individual_table = Table(box=box.ROUNDED)
        individual_table.add_column("Schema", style="cyan")
        individual_table.add_column("Table", style="blue")
        individual_table.add_column("Partition", style="magenta")
        individual_table.add_column("Shard ID", justify="right", style="yellow")
        individual_table.add_column("Node", style="green")
        individual_table.add_column("Translog MB", justify="right", style="red")
        
        for row in individual_shards:
            schema_name, table_name, partition_values, shard_id, node_name, translog_mb = row
            partition_display = partition_values if partition_values and partition_values != 'NULL' else "none"
            
            individual_table.add_row(
                schema_name,
                table_name,
                partition_display,
                str(shard_id),
                node_name,
                f"{translog_mb:.1f}"
            )
        
        console.print(individual_table)
        console.print()
        
        console.print(f"Found {len(summary_rows)} table/partition(s) with problematic translogs:")
        console.print()
        
        # Display summary table
        results_table = Table(title=f"Tables with Problematic Replicas (translog > {sizemb}MB)", box=box.ROUNDED)
        results_table.add_column("Schema", style="cyan")
        results_table.add_column("Table", style="blue")  
        results_table.add_column("Partition", style="magenta")
        results_table.add_column("Problematic Replicas", justify="right", style="yellow")
        results_table.add_column("Max Translog MB", justify="right", style="red")
        results_table.add_column("Shards (P/R)", justify="right", style="blue")
        results_table.add_column("Size GB (P/R)", justify="right", style="bright_blue")
        results_table.add_column("Current Replicas", justify="right", style="green")
        
        # Collect table/partition info and look up current replica counts
        table_replica_info = []
        for row in summary_rows:
            schema_name, table_name, partition_values, partition_ident, problematic_replica_shards, max_translog_mb, total_primary_shards, total_replica_shards, total_primary_size_gb, total_replica_size_gb = row
            partition_display = partition_values if partition_values and partition_values != 'NULL' else "[dim]none[/dim]"
            
            # Look up current replica count
            current_replicas = 0
            try:
                if partition_values and partition_values != 'NULL':
                    # Partitioned table query
                    replica_query = """
                        SELECT number_of_replicas
                        FROM information_schema.table_partitions
                        WHERE table_name = ? AND table_schema = ? AND partition_ident = ?
                    """
                    replica_result = client.execute_query(replica_query, [table_name, schema_name, partition_ident])
                else:
                    # Non-partitioned table query
                    replica_query = """
                        SELECT number_of_replicas
                        FROM information_schema.tables
                        WHERE table_name = ? AND table_schema = ?
                    """
                    replica_result = client.execute_query(replica_query, [table_name, schema_name])
                
                replica_rows = replica_result.get('rows', [])
                if replica_rows:
                    current_replicas = replica_rows[0][0]
            except Exception as e:
                console.print(f"[yellow]Warning: Could not retrieve replica count for {schema_name}.{table_name}: {e}[/yellow]")
                current_replicas = "unknown"
            
            table_replica_info.append((
                schema_name, table_name, partition_values, partition_ident, 
                problematic_replica_shards, max_translog_mb, total_primary_shards, total_replica_shards, 
                total_primary_size_gb, total_replica_size_gb, current_replicas
            ))
            
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
        
        console.print(results_table)
        console.print()
        console.print("[bold]Generated Comprehensive Shard Management Commands:[/bold]")
        console.print()
        
        # 1. Stop automatic shard rebalancing
        console.print("[bold cyan]1. Stop Automatic Shard Rebalancing:[/bold cyan]")
        rebalance_disable_cmd = 'SET GLOBAL PERSISTENT "cluster.routing.rebalance.enable"=\'none\';'
        console.print(rebalance_disable_cmd)
        console.print()
        
        # 2. Generate REROUTE CANCEL SHARD commands for individual shards (unchanged)
        console.print("[bold cyan]2. REROUTE CANCEL Commands (unchanged from original):[/bold cyan]")
        reroute_commands = []
        for row in individual_shards:
            schema_name, table_name, partition_values, shard_id, node_name, translog_mb = row
            cmd = f'ALTER TABLE "{schema_name}"."{table_name}" REROUTE CANCEL SHARD {shard_id} on \'{node_name}\' WITH (allow_primary=False);'
            reroute_commands.append(cmd)
            console.print(cmd)
        
        if reroute_commands:
            console.print()
        
        # 3. Generate ALTER commands to set replicas to 0
        console.print("[bold cyan]3. Set Replicas to 0:[/bold cyan]")
        set_zero_commands = []
        valid_table_info = []
        
        for info in table_replica_info:
            schema_name, table_name, partition_values, partition_ident, problematic_replica_shards, max_translog_mb, total_primary_shards, total_replica_shards, total_primary_size_gb, total_replica_size_gb, current_replicas = info
            
            if current_replicas == "unknown":
                console.print(f"[yellow]-- Skipping {schema_name}.{table_name} (unknown replica count)[/yellow]")
                continue
                
            if current_replicas == 0:
                console.print(f"[yellow]-- Skipping {schema_name}.{table_name} (already has 0 replicas)[/yellow]")
                continue
            
            valid_table_info.append(info)
            
            # Build the ALTER command to set replicas to 0
            if partition_values and partition_values != 'NULL':
                # Partitioned table commands
                cmd_set_zero = f'ALTER TABLE "{schema_name}"."{table_name}" PARTITION {partition_values} SET ("number_of_replicas" = 0);'
            else:
                # Non-partitioned table commands
                cmd_set_zero = f'ALTER TABLE "{schema_name}"."{table_name}" SET ("number_of_replicas" = 0);'
            
            set_zero_commands.append(cmd_set_zero)
            console.print(cmd_set_zero)
        
        console.print()
        
        # 4. Generate retention lease queries for monitoring
        console.print("[bold cyan]4. Retention Lease Monitoring Queries:[/bold cyan]")
        retention_queries = []
        
        for info in valid_table_info:
            schema_name, table_name, partition_values, partition_ident, problematic_replica_shards, max_translog_mb, total_primary_shards, total_replica_shards, total_primary_size_gb, total_replica_size_gb, current_replicas = info
            
            if partition_values and partition_values != 'NULL':
                # For partitioned tables, we need to resolve the partition_ident
                # First, get all partition_idents for this table
                partition_query = f"""SELECT array_length(retention_leases['leases'], 1) as cnt_leases, id 
FROM sys.shards 
WHERE table_name = '{table_name}' 
  AND schema_name = '{schema_name}'
  AND partition_ident = '{partition_ident}' 
ORDER BY array_length(retention_leases['leases'], 1);"""
            else:
                # For non-partitioned tables
                partition_query = f"""SELECT array_length(retention_leases['leases'], 1) as cnt_leases, id 
FROM sys.shards 
WHERE table_name = '{table_name}' 
  AND schema_name = '{schema_name}'
ORDER BY array_length(retention_leases['leases'], 1);"""
            
            retention_queries.append(partition_query)
            console.print(f"-- For {schema_name}.{table_name}:")
            console.print(partition_query)
            console.print()
        
        # 5. Generate ALTER commands to set replicas to 1 (or original value)
        console.print("[bold cyan]5. Restore Replicas to Original Values:[/bold cyan]")
        restore_commands = []
        
        for info in valid_table_info:
            schema_name, table_name, partition_values, partition_ident, problematic_replica_shards, max_translog_mb, total_primary_shards, total_replica_shards, total_primary_size_gb, total_replica_size_gb, current_replicas = info
            
            # Build the ALTER command to restore replicas
            if partition_values and partition_values != 'NULL':
                # Partitioned table commands
                cmd_restore = f'ALTER TABLE "{schema_name}"."{table_name}" PARTITION {partition_values} SET ("number_of_replicas" = {current_replicas});'
            else:
                # Non-partitioned table commands
                cmd_restore = f'ALTER TABLE "{schema_name}"."{table_name}" SET ("number_of_replicas" = {current_replicas});'
            
            restore_commands.append(cmd_restore)
            console.print(cmd_restore)
        
        console.print()
        
        # 6. Re-enable automatic shard rebalancing
        console.print("[bold cyan]6. Re-enable Automatic Shard Rebalancing:[/bold cyan]")
        rebalance_enable_cmd = 'SET GLOBAL PERSISTENT "cluster.routing.rebalance.enable"=\'all\';'
        console.print(rebalance_enable_cmd)
        console.print()
        
        # Collect all commands for execution
        all_commands = [rebalance_disable_cmd] + reroute_commands + set_zero_commands + restore_commands + [rebalance_enable_cmd]
        
        if not all_commands:
            console.print("[yellow]No ALTER commands generated[/yellow]")
            return
            
        console.print(f"[bold]Total Commands:[/bold]")
        console.print(f"  • 1 rebalancing disable command")
        console.print(f"  • {len(reroute_commands)} REROUTE CANCEL commands")
        console.print(f"  • {len(set_zero_commands)} set replicas to 0 commands")
        console.print(f"  • {len(retention_queries)} retention lease queries (for monitoring)")
        console.print(f"  • {len(restore_commands)} restore replicas commands")
        console.print(f"  • 1 rebalancing enable command")
        
        if execute and all_commands:
            console.print()
            console.print("[yellow]⚠️  WARNING: This will execute the complete shard management sequence![/yellow]")
            console.print("[yellow]This includes disabling rebalancing, canceling problematic shards,")
            console.print("setting replicas to 0, restoring replicas, and re-enabling rebalancing.[/yellow]")
            console.print("[yellow]Retention lease queries will be displayed but not executed.[/yellow]")
            console.print()
            
            if click.confirm("Execute all commands with individual confirmation for each?"):
                console.print()
                console.print("[bold blue]Executing comprehensive shard management sequence...[/bold blue]")
                
                executed = 0
                failed = 0
                cmd_num = 0
                
                # 1. Execute rebalancing disable command
                cmd_num += 1
                console.print(f"[bold]Step 1: Disable Rebalancing[/bold]")
                console.print(f"[dim]Command {cmd_num}: {rebalance_disable_cmd}[/dim]")
                if click.confirm(f"Execute rebalancing disable command?"):
                    try:
                        client.execute_query(rebalance_disable_cmd)
                        console.print(f"[green]✓ Command {cmd_num} executed successfully[/green]")
                        executed += 1
                    except Exception as e:
                        console.print(f"[red]✗ Command {cmd_num} failed: {e}[/red]")
                        failed += 1
                else:
                    console.print(f"[yellow]Command {cmd_num} skipped[/yellow]")
                console.print()
                
                # 2. Execute REROUTE CANCEL commands
                if reroute_commands:
                    console.print(f"[bold]Step 2: Execute REROUTE CANCEL Commands[/bold]")
                    for cmd in reroute_commands:
                        cmd_num += 1
                        console.print(f"[dim]Command {cmd_num}: {cmd}[/dim]")
                        if click.confirm(f"Execute this REROUTE CANCEL command?"):
                            try:
                                client.execute_query(cmd)
                                console.print(f"[green]✓ Command {cmd_num} executed successfully[/green]")
                                executed += 1
                            except Exception as e:
                                console.print(f"[red]✗ Command {cmd_num} failed: {e}[/red]")
                                failed += 1
                        else:
                            console.print(f"[yellow]Command {cmd_num} skipped[/yellow]")
                    console.print()
                
                # 3. Execute set replicas to 0 commands
                if set_zero_commands:
                    console.print(f"[bold]Step 3: Set Replicas to 0[/bold]")
                    for cmd in set_zero_commands:
                        cmd_num += 1
                        console.print(f"[dim]Command {cmd_num}: {cmd}[/dim]")
                        if click.confirm(f"Execute this SET replicas to 0 command?"):
                            try:
                                client.execute_query(cmd)
                                console.print(f"[green]✓ Command {cmd_num} executed successfully[/green]")
                                executed += 1
                            except Exception as e:
                                console.print(f"[red]✗ Command {cmd_num} failed: {e}[/red]")
                                failed += 1
                        else:
                            console.print(f"[yellow]Command {cmd_num} skipped[/yellow]")
                    console.print()
                
                # 4. Display retention lease queries (not executed)
                if retention_queries:
                    console.print(f"[bold]Step 4: Retention Lease Monitoring Queries (for reference)[/bold]")
                    console.print("[dim]These queries are for monitoring purposes and will not be executed:[/dim]")
                    for i, query in enumerate(retention_queries, 1):
                        console.print(f"[dim]Query {i}:[/dim]")
                        console.print(f"[dim]{query}[/dim]")
                    console.print()
                
                # 5. Execute restore replicas commands
                if restore_commands:
                    console.print(f"[bold]Step 5: Restore Replicas to Original Values[/bold]")
                    for cmd in restore_commands:
                        cmd_num += 1
                        console.print(f"[dim]Command {cmd_num}: {cmd}[/dim]")
                        if click.confirm(f"Execute this RESTORE replicas command?"):
                            try:
                                client.execute_query(cmd)
                                console.print(f"[green]✓ Command {cmd_num} executed successfully[/green]")
                                executed += 1
                            except Exception as e:
                                console.print(f"[red]✗ Command {cmd_num} failed: {e}[/red]")
                                failed += 1
                        else:
                            console.print(f"[yellow]Command {cmd_num} skipped[/yellow]")
                    console.print()
                
                # 6. Execute rebalancing enable command
                cmd_num += 1
                console.print(f"[bold]Step 6: Re-enable Rebalancing[/bold]")
                console.print(f"[dim]Command {cmd_num}: {rebalance_enable_cmd}[/dim]")
                if click.confirm(f"Execute rebalancing enable command?"):
                    try:
                        client.execute_query(rebalance_enable_cmd)
                        console.print(f"[green]✓ Command {cmd_num} executed successfully[/green]")
                        executed += 1
                    except Exception as e:
                        console.print(f"[red]✗ Command {cmd_num} failed: {e}[/red]")
                        failed += 1
                else:
                    console.print(f"[yellow]Command {cmd_num} skipped[/yellow]")
                console.print()
                
                console.print(f"[bold]Execution Summary:[/bold]")
                console.print(f"[green]✓ Successful: {executed}[/green]")
                if failed > 0:
                    console.print(f"[red]✗ Failed: {failed}[/red]")
            else:
                console.print("[yellow]Operation cancelled by user[/yellow]")
                
    except Exception as e:
        console.print(f"[red]Error analyzing problematic translogs: {e}[/red]")
        import traceback
        console.print(f"[dim]{traceback.format_exc()}[/dim]")


@main.command()
@click.option('--translogsize', default=500, help='Minimum translog uncommitted size threshold in MB (default: 500)')
@click.option('--interval', default=60, help='Monitoring interval in seconds for watch mode (default: 60)')
@click.option('--watch', '-w', is_flag=True, help='Continuously monitor (refresh every interval)')
@click.option('--table', '-t', help='Monitor specific table only')
@click.option('--node', '-n', help='Monitor specific node only')
@click.option('--count', default=50, help='Maximum number of shards with large translogs to show (default: 50)')
@click.pass_context
def large_translogs(ctx, translogsize: int, interval: int, watch: bool, table: Optional[str], node: Optional[str], count: int):
    """Monitor shards with large translog uncommitted sizes that do not flush
    
    This command identifies shards (both primary and replica) that have large
    translog uncommitted sizes, indicating they are not flushing properly.
    Useful for monitoring translog growth and identifying problematic shards.
    
    Examples:
        xmover large-translogs --translogsize 1000            # Shards with >1GB translog
        xmover large-translogs --watch --interval 30          # Continuous monitoring every 30s
        xmover large-translogs --table my_table --watch       # Monitor specific table
        xmover large-translogs --node data-hot-1 --count 20   # Top 20 on specific node
    """
    client = ctx.obj['client']
    
    def get_large_translog_shards():
        """Get shards with large translog uncommitted sizes"""
        query = """
            SELECT
                sh.schema_name,
                sh.table_name,
                translate(p.values::text, ':{}', '=()') as partition_values,
                sh.id AS shard_id,
                node['name'] AS node_name,
                COALESCE(sh.translog_stats['uncommitted_size'] / 1024^2, 0) AS translog_uncommitted_mb,
                sh.primary,
                sh.size / 1024^2 AS shard_size_mb
            FROM
                sys.shards AS sh
            LEFT JOIN information_schema.table_partitions p
                ON sh.table_name = p.table_name
                AND sh.schema_name = p.table_schema
                AND sh.partition_ident = p.partition_ident
            WHERE
                sh.state = 'STARTED'
                AND COALESCE(sh.translog_stats['uncommitted_size'], 0) > ? * 1024^2
        """
        
        params = [translogsize]
        
        # Add table filter if specified
        if table:
            if '.' in table:
                schema_name, table_name = table.split('.', 1)
                query += " AND sh.schema_name = ? AND sh.table_name = ?"
                params.extend([schema_name, table_name])
            else:
                query += " AND sh.table_name = ?"
                params.append(table)
        
        # Add node filter if specified
        if node:
            query += " AND node['name'] = ?"
            params.append(node)
        
        query += """
            ORDER BY
                COALESCE(sh.translog_stats['uncommitted_size'], 0) DESC
            LIMIT ?
        """
        params.append(count)
        
        try:
            result = client.execute_query(query, params)
            return result.get('rows', [])
        except Exception as e:
            console.print(f"[red]Error querying shards with large translogs: {e}[/red]")
            return []
    
    def display_large_translog_shards(shards_data, show_header=True):
        """Display the shards with large translogs in a table"""
        if not shards_data:
            threshold_display = f"{translogsize}MB" if translogsize < 1000 else f"{translogsize/1000:.1f}GB"
            console.print(f"[green]✅ No shards found with translog uncommitted size over {threshold_display}[/green]")
            return
        
        # Get current timestamp
        import datetime
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")
        
        # Create condensed table
        from rich.table import Table
        results_table = Table(show_header=show_header, box=box.SIMPLE if watch else box.ROUNDED)
        if show_header:
            results_table.add_column("Schema.Table", style="cyan", max_width=50)
            results_table.add_column("Partition", style="magenta", max_width=30)
            results_table.add_column("Shard", justify="right", style="yellow", width=5)
            results_table.add_column("Node", style="green", max_width=12)
            results_table.add_column("TL MB", justify="right", style="red", width=6)
            results_table.add_column("Type", justify="center", style="bright_white", width=4)
        else:
            results_table.add_column("", style="cyan", max_width=50)
            results_table.add_column("", style="magenta", max_width=30)
            results_table.add_column("", justify="right", style="yellow", width=5)
            results_table.add_column("", style="green", max_width=12)
            results_table.add_column("", justify="right", style="red", width=6)
            results_table.add_column("", justify="center", style="bright_white", width=4)
        
        for row in shards_data:
            schema_name, table_name, partition_values, shard_id, node_name, translog_mb, is_primary, shard_size_mb = row
            
            # Format table name
            if schema_name and schema_name != 'doc':
                table_display = f"{schema_name}.{table_name}"
            else:
                table_display = table_name
            
            # Format partition
            if partition_values and partition_values != 'NULL':
                partition_display = partition_values[:27] + "..." if len(partition_values) > 30 else partition_values
            else:
                partition_display = "-"
            
            primary_display = "P" if is_primary else "R"
            
            # Color code translog based on size
            if translog_mb > 1000:
                translog_color = "bright_red"
            elif translog_mb > 500:
                translog_color = "red"
            elif translog_mb > 100:
                translog_color = "yellow"
            else:
                translog_color = "green"
            
            results_table.add_row(
                table_display,
                partition_display,
                str(shard_id),
                node_name,
                f"[{translog_color}]{translog_mb:.0f}[/{translog_color}]",
                primary_display
            )
        
        # Show timestamp and summary
        total_shards = len(shards_data)
        primary_count = sum(1 for row in shards_data if row[6])  # is_primary is at index 6
        replica_count = total_shards - primary_count
        avg_translog = sum(row[5] for row in shards_data) / total_shards if total_shards > 0 else 0  # translog_mb is at index 5
        
        if show_header:
            threshold_display = f"{translogsize}MB" if translogsize < 1000 else f"{translogsize/1000:.1f}GB"
            console.print(f"[bold blue]Large Translogs (>{threshold_display}) - {timestamp}[/bold blue]")
        else:
            console.print(f"[dim]{timestamp}[/dim]")
            
        console.print(results_table)
        console.print(f"[dim]{total_shards} shards ({primary_count}P/{replica_count}R) - Avg translog: {avg_translog:.0f}MB[/dim]")
    
    def run_single_analysis():
        """Run a single analysis cycle"""
        if not watch:
            console.print(Panel.fit("[bold blue]Large Translog Monitor[/bold blue]"))
        
        # Show configuration
        threshold_display = f"{translogsize}MB" if translogsize < 1000 else f"{translogsize/1000:.1f}GB"
        if watch:
            config_parts = [f"{interval}s", f">{threshold_display}", f"top {count}"]
            if table:
                config_parts.append(f"table: {table}")
            if node:
                config_parts.append(f"node: {node}")
            console.print(f"[dim]{' | '.join(config_parts)}[/dim]")
        else:
            config_info = [f"Threshold: >{threshold_display}"]
            if count != 50:
                config_info.append(f"Limit: {count}")
            if table:
                config_info.append(f"Table: {table}")
            if node:
                config_info.append(f"Node: {node}")
                
            console.print("[dim]" + " | ".join(config_info) + "[/dim]")
        if not watch:
            console.print()
        
        # Get shards with large translogs
        shards_data = get_large_translog_shards()
        
        # Display results
        display_large_translog_shards(shards_data, show_header=not watch)
    
    try:
        if watch:
            console.print("[dim]Press Ctrl+C to stop monitoring[/dim]")
            console.print()
            
            while True:
                run_single_analysis()
                if watch:
                    console.print(f"\n[dim]━━━ Next update in {interval}s ━━━[/dim]\n")
                time.sleep(interval)
        else:
            run_single_analysis()
            
    except KeyboardInterrupt:
        console.print("\n[yellow]Monitoring stopped by user[/yellow]")
    except Exception as e:
        console.print(f"[red]Error during large translog monitoring: {e}[/red]")
        import traceback
        console.print(f"[dim]{traceback.format_exc()}[/dim]")


@main.command("deep-analyze")
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
    if validate_rules:
        if validate_rules_file(validate_rules):
            console.print(f"[green]✅ Rules file {validate_rules} is valid[/green]")
            sys.exit(0)
        else:
            sys.exit(1)
    
    try:
        client = ctx.obj['client']
        
        # Initialize monitor with optional custom rules
        monitor = ShardSizeMonitor(client, rules_file)
        
        console.print("[bold blue]🔍 XMover Deep Shard Size Analysis[/bold blue]")
        console.print("Analyzing cluster configuration and shard distributions...\n")
        
        # Run analysis
        report = monitor.analyze_cluster_shard_sizes(schema_filter=schema)
        
        # Display results
        monitor.display_report(report, severity_filter=severity)
        
        # Export CSV if requested
        if export_csv:
            monitor.export_csv(report, export_csv)
            console.print(f"\n[green]📊 Results exported to {export_csv}[/green]")
        
        # Summary footer
        violation_counts = report.total_violations_by_severity
        total_violations = sum(violation_counts.values())
        
        if total_violations > 0:
            console.print(f"\n[bold]Analysis completed:[/bold] {total_violations} optimization opportunities identified")
            if violation_counts['critical'] > 0:
                console.print("[red]⚠️  Critical issues require immediate attention[/red]")
        else:
            console.print("\n[bold green]🎉 Excellent! No optimization issues detected[/bold green]")
            
    except Exception as e:
        console.print(f"[red]Error during deep shard size analysis: {e}[/red]")
        import traceback
        console.print(f"[dim]{traceback.format_exc()}[/dim]")


if __name__ == '__main__':
    main()
