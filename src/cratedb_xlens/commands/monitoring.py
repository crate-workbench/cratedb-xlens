"""
Monitoring commands for XMover CLI - handles cluster monitoring operations
"""

import time
import signal
import json
from datetime import datetime
from typing import Optional, Dict, List, Any
import click
from rich.console import Console
# Table import moved to where needed to avoid unused import warning
from rich.panel import Panel
from rich import box
from loguru import logger

from ..analyzer import RecoveryMonitor, ActiveShardMonitor
from ..utils import format_table_display_with_partition, format_translog_info, format_recovery_progress
from .base import BaseCommand

console = Console()


def format_timestamp_with_health(timestamp: str, health: str) -> str:
    """Format timestamp with color based on cluster health"""
    if health == 'RED':
        return f"[red]{timestamp}[/red]"
    elif health == 'YELLOW':
        return f"[yellow]{timestamp}[/yellow]"
    else:  # GREEN or unknown
        return f"[green]{timestamp}[/green]"


def format_underreplicated_shards_status(health_info: Optional[dict]) -> str:
    """Format underreplicated shards status in a concise way"""
    if not health_info or health_info.get('cluster_health') == 'GREEN':
        return ""
    
    parts = []
    
    # Red shards (highest priority)
    red_shards = health_info.get('red_underreplicated_shards', 0)
    if red_shards and red_shards > 0:
        parts.append(f"[red]{red_shards}R[/red]")
    
    # Yellow shards  
    yellow_shards = health_info.get('yellow_underreplicated_shards', 0)
    if yellow_shards and yellow_shards > 0:
        parts.append(f"[yellow]{yellow_shards}Y[/yellow]")
    
    # Green shards (shouldn't normally have underreplicated, but just in case)
    green_shards = health_info.get('green_underreplicated_shards', 0)
    if green_shards and green_shards > 0:
        parts.append(f"[green]{green_shards}G[/green]")
    
    # Other shards
    other_shards = health_info.get('other_underreplicated_shards', 0)
    if other_shards and other_shards > 0:
        parts.append(f"[dim]{other_shards}?[/dim]")
    
    if parts:
        return f" ({'/'.join(parts)} under-replicated)"
    return ""


def get_recovery_status_text(health_info: Optional[dict], active_count: int) -> str:
    """Get the appropriate status text based on cluster health and recovery count"""
    if not health_info:
        return f"{active_count} recovering" if active_count > 0 else "stable"
    
    cluster_health = health_info.get('cluster_health', 'UNKNOWN')
    
    if cluster_health == 'GREEN' and active_count > 0:
        return f"{active_count} rebalancing"
    elif active_count > 0:
        return f"{active_count} recovering"
    else:
        return "stable"


class MonitoringCommands(BaseCommand):
    """Handler for monitoring-related commands"""

    def execute(self, command: str, **kwargs) -> None:
        """Execute a monitoring command"""
        if command == "monitor_recovery":
            self.monitor_recovery(**kwargs)
        elif command == "active_shards":
            self.active_shards(**kwargs)
        elif command == "large_translogs":
            self.large_translogs(**kwargs)
        elif command == "read_check":
            self.read_check(**kwargs)
        else:
            raise ValueError(f"Unknown monitoring command: {command}")

    def monitor_recovery(self, ctx, table: str, node: str, watch: bool, refresh_interval: int, 
                        recovery_type: str, include_transitioning: bool):
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

                        # Get cluster health information
                        health_info = recovery_monitor.get_cluster_health()

                        # Display current time with health-based coloring
                        from datetime import datetime
                        current_time = datetime.now().strftime("%H:%M:%S")
                        if health_info:
                            cluster_health = health_info.get('cluster_health', 'UNKNOWN')
                            colored_time = format_timestamp_with_health(current_time, cluster_health)
                        else:
                            colored_time = current_time

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
                            underreplicated_status = format_underreplicated_shards_status(health_info)
                            console.print(f"{colored_time} | [green]No issues - cluster stable[/green]{underreplicated_status}")
                            previous_recoveries.clear()
                        elif not recoveries and non_recovering_shards:
                            underreplicated_status = format_underreplicated_shards_status(health_info)
                            console.print(f"{colored_time} | [yellow]{len(non_recovering_shards)} shards need attention (not recovering)[/yellow]{underreplicated_status}")
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
                            recovery_status_text = get_recovery_status_text(health_info, active_count)
                            if active_count > 0:
                                status_parts.append(recovery_status_text)
                            if completed_count > 0:
                                status_parts.append(f"{completed_count} done")
                            if non_recovering_shards:
                                status_parts.append(f"[yellow]{len(non_recovering_shards)} awaiting recovery[/yellow]")
                            
                            status = " | ".join(status_parts)
                            underreplicated_status = format_underreplicated_shards_status(health_info)

                            # Show status line with changes or periodic update
                            if changes:
                                console.print(f"{colored_time} | {status}{underreplicated_status}")
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
                                    from datetime import datetime
                                    current_dt = datetime.now()
                                    
                                    # Show transitioning details every 30 seconds or first time
                                    should_show_details = (
                                        last_transitioning_display is None or 
                                        (current_dt - last_transitioning_display).total_seconds() >= 30
                                    )
                                    
                                    if should_show_details:
                                        console.print(f"{colored_time} | {status}{underreplicated_status} (transitioning)")
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
                                        console.print(f"{colored_time} | {status}{underreplicated_status} (transitioning)")
                                elif active_count > 0:
                                    console.print(f"{colored_time} | {status}{underreplicated_status} (no changes)")
                                elif non_recovering_shards:
                                    console.print(f"{colored_time} | {status}{underreplicated_status} (issues persist)")

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
            console.print(f"[red]Error monitoring recovery: {e}[/red]")
            if ctx.obj.get('debug'):
                raise

    def active_shards(self, ctx, count: int, interval: int, min_checkpoint_delta: int, 
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

    def large_translogs(self, ctx, translogsize: int, interval: int, watch: bool, 
                       table: Optional[str], node: Optional[str], count: int):
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
                    COALESCE(node['name'], 'unknown-' || COALESCE(node['id'], 'corrupted')) AS node_name,
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
            
            params = []
            params.append(translogsize)
            
            # Add table filter if specified
            if table:
                if '.' in table:
                    schema_name, table_name = table.split('.', 1)
                    query += " AND sh.schema_name = ? AND sh.table_name = ?"
                    params.append(schema_name)
                    params.append(table_name)
                else:
                    query += " AND sh.table_name = ?"
                    params.append(table)
            
            # Add node filter if specified
            if node:
                query += " AND COALESCE(node['name'], 'unknown-' || COALESCE(node['id'], 'corrupted')) = ?"
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

    def read_check(self, ctx, seconds: int):
        """Monitor cluster data readability by sampling from largest tables
        
        This command continuously monitors cluster health by:
        - Discovering the largest 5 tables/partitions by size
        - Efficiently querying max(_seq_no) from each table every --seconds (default 30s)
        - Tracking changes in _seq_no and total_docs
        - Detecting stale or problematic tables
        - Providing performance metrics and health scoring
        
        Features:
        - 🟢 Active tables (seq_no changing regularly)
        - 🟡 Slow tables (minimal activity)
        - 🔴 Stale tables (no activity detected)
        - Optimized queries using max() aggregation
        - Query performance tracking
        - Baseline anomaly detection
        - Fresh connections for each check
        - Exponential backoff retry on errors
        """
        
        # Store monitoring interval for health status calculations
        self.monitoring_interval = seconds
        
        # Setup loguru for this command (first to use it)
        logger.remove()  # Remove default handler
        logger.add(
            lambda msg: self.console.print(msg, end=""),
            format="{time:HH:mm:ss.SSS} | {level} | {message}",
            level="INFO"
        )
        
        # Get cluster info
        cluster_name = self.client.get_cluster_name() or "Unknown"
        
        # State tracking
        stats = {
            'samples_taken': 0,
            'changes_detected': 0,
            'tables_queried': set(),
            'connection_failures': 0,
            'query_failures': 0,
            'start_time': datetime.now()
        }
        
        # Enhanced per-table statistics
        table_stats = {}  # table_key -> {doc_changes: [deltas], performance: [times], anomalies: count, shard_info: dict}
        
        # Data tracking
        last_discovery = 0
        table_data = {}  # table_key -> {seq_no, total_docs, last_seen, baseline_activity}
        discovery_data = []
        performance_metrics = {}  # table_key -> [response_times]
        
        def signal_handler(signum, frame):
            """Handle CTRL+C gracefully with stats"""
            self._print_read_check_stats(stats, table_stats, cluster_name)
            raise KeyboardInterrupt()
        
        # Register signal handler
        signal.signal(signal.SIGINT, signal_handler)
        
        try:
            self._display_read_check_header(cluster_name, seconds)
            
            while True:
                current_time = time.time()
                
                # Re-discover largest tables every 10 minutes (600 seconds)
                if current_time - last_discovery > 600:
                    new_discovery = self._discover_largest_tables()
                    if new_discovery != discovery_data:
                        if discovery_data:  # Not first run
                            logger.info(f"📋 Table discovery updated - {len(new_discovery)} largest tables identified")
                        discovery_data = new_discovery
                        last_discovery = current_time
                
                if not discovery_data:
                    logger.error("❌ No tables found for monitoring")
                    time.sleep(seconds)
                    continue
                
                # Sample from each discovered table
                for table_info in discovery_data:
                    table_key = self._get_table_key(table_info)
                    stats['tables_queried'].add(table_key)
                    
                    success = self._sample_table_data(table_info, table_data, performance_metrics, stats, table_stats)
                    if success:
                        stats['samples_taken'] += 1
                
                # Sleep until next cycle
                time.sleep(seconds)
                
        except KeyboardInterrupt:
            pass  # Stats already printed by signal handler
        except Exception as e:
            logger.error(f"💥 Unexpected error: {e}")
            self._print_read_check_stats(stats, table_stats, cluster_name)
    
    def _display_read_check_header(self, cluster_name: str, seconds: int):
        """Display professional header for read-check command"""
        header_text = f"CrateDB Read Check [{cluster_name}]"
        subheader = f"Monitoring max(_seq_no) every {seconds}s from largest tables"
        
        self.console.print()
        self.console.print(Panel.fit(
            f"[bold blue]{header_text}[/bold blue]\n[dim]{subheader}[/dim]",
            border_style="blue"
        ))
        self.console.print()
        
        # Show clearer legend separating write activity from query performance
        activity_legend = "Write Activity: 🟢 Active • 🟡 Slow • 🔴 Stale"
        performance_legend = "Query Performance: ⚡ >1000ms • ⚠️ Anomaly"
        self.console.print(f"[dim]{activity_legend}[/dim]")
        self.console.print(f"[dim]{performance_legend}[/dim]")
        self.console.print()
    
    def _discover_largest_tables(self) -> List[Dict[str, Any]]:
        """Discover the 5 largest tables/partitions with simplified query (no JOIN for performance)"""
        discovery_query = """
        SELECT
            schema_name,
            table_name,
            partition_ident,
            ROUND(SUM(size) / 1024 / 1024 / 1024, 2) AS size_gb,
            SUM(num_docs) AS total_docs
        FROM sys.shards
        WHERE "primary" = true
        GROUP BY schema_name, table_name, partition_ident
        ORDER BY size_gb DESC
        LIMIT 5
        """
        
        try:
            # Fresh connection for discovery
            fresh_client = self._create_fresh_client()
            result = fresh_client.execute_query(discovery_query)

            # Check if result contains an error
            if 'error' in result:
                logger.error(f"🔍 Discovery query returned error: {result['error']}")
                if 'error_trace' in result:
                    logger.error(f"   Error trace: {result['error_trace']}")
                return []

            tables = []
            for row in result.get('rows', []):
                schema, table, partition_ident, size_gb, total_docs = row
                tables.append({
                    'schema_name': schema,
                    'table_name': table,
                    'partition_ident': partition_ident,
                    'partition_values': None,  # Not queried for performance reasons
                    'size_gb': float(size_gb) if size_gb else 0.0,
                    'total_docs': int(total_docs) if total_docs else 0
                })

            logger.info(f"🔍 Discovery found {len(tables)} tables")
            return tables

        except Exception as e:
            import traceback
            logger.error(f"🔍 Discovery failed: {e}")
            logger.error(f"   Exception type: {type(e).__name__}")
            logger.debug(f"   Full traceback: {traceback.format_exc()}")
            return []
    
    def _sample_table_data(self, table_info: Dict, table_data: Dict, performance_metrics: Dict, 
                          stats: Dict, table_stats: Dict) -> bool:
        """Sample data from a specific table with retry logic"""
        table_key = self._get_table_key(table_info)
        max_retries = 3
        retry_delay = 1
        
        for attempt in range(max_retries):
            try:
                # Create sample query
                sample_query = self._build_sample_query(table_info)
                
                # Fresh connection for each sample
                fresh_client = self._create_fresh_client()
                result = fresh_client.execute_query(sample_query)
                
                # Use CrateDB's actual query execution time (avoids RTT)
                query_time_ms = int(result.get('duration', 0))
                if table_key not in performance_metrics:
                    performance_metrics[table_key] = []
                performance_metrics[table_key].append(query_time_ms)
                
                # Keep only last 20 measurements
                if len(performance_metrics[table_key]) > 20:
                    performance_metrics[table_key] = performance_metrics[table_key][-20:]
                
                # Process results - max() query returns single value
                rows = result.get('rows', [])
                if rows and rows[0][0] is not None:
                    max_seq_no = rows[0][0]  # max() returns single value
                    self._process_sample_results(table_info, table_key, max_seq_no, table_data, stats, table_stats, query_time_ms)
                else:
                    logger.warning(f"🔍 {table_key}: No data returned")
                
                # Update table statistics (only if we got valid duration)
                if query_time_ms > 0:
                    self._update_table_stats(table_key, table_info, query_time_ms, table_stats)
                
                return True
                
            except Exception as e:
                stats['query_failures'] += 1
                if attempt < max_retries - 1:
                    logger.warning(f"⚠️ {table_key}: Retry {attempt + 1}/{max_retries} after {retry_delay}s - {e}")
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error(f"❌ {table_key}: Failed after {max_retries} attempts - {e}")
                    return False
        
        return False
    
    def _build_sample_query(self, table_info: Dict) -> str:
        """Build the optimized sample query for a table/partition"""
        schema = table_info['schema_name']
        table = table_info['table_name']
        partition_values = table_info.get('partition_values')
        
        # Build table reference
        if schema and schema != "doc":
            table_ref = f'"{schema}"."{table}"'
        else:
            table_ref = f'"{table}"'
        
        # Optimized query using max() aggregation
        query = f"SELECT max(_seq_no) FROM {table_ref}"
        
        # Add partition filter if needed
        if partition_values:
            try:
                # Parse partition values (JSON format)
                partition_dict = json.loads(partition_values)
                where_clauses = []
                for key, value in partition_dict.items():
                    where_clauses.append(f"{key} = {value}")
                if where_clauses:
                    query += f" WHERE {' AND '.join(where_clauses)}"
            except (json.JSONDecodeError, TypeError):
                # Fallback - skip partition filter if parsing fails
                pass
        
        return query
    
    def _process_sample_results(self, table_info: Dict, table_key: str, max_seq_no: int, 
                               table_data: Dict, stats: Dict, table_stats: Dict, query_time_ms: int):
        """Process sample results and update tracking data"""
        current_total_docs = table_info['total_docs']
        current_time = time.time()
        
        # Get previous data
        prev_data = table_data.get(table_key, {})
        prev_seq_no = prev_data.get('seq_no')
        prev_total_docs = prev_data.get('total_docs')
        prev_time = prev_data.get('last_seen', current_time)
        
        # Calculate deltas
        seq_no_delta = max_seq_no - prev_seq_no if prev_seq_no is not None else 0
        docs_delta = current_total_docs - prev_total_docs if prev_total_docs is not None else 0
        
        # Detect changes
        has_changes = seq_no_delta != 0 or docs_delta != 0
        if has_changes and prev_seq_no is not None:
            stats['changes_detected'] += 1
        
        # Determine health status
        time_since_last = current_time - prev_time if prev_seq_no is not None else 0
        health_status = self._determine_health_status(seq_no_delta, prev_data, table_data)
        
        # Check for anomalies and update counter
        is_anomaly = self._detect_anomaly(table_key, seq_no_delta, docs_delta, prev_data, table_stats)
        
        # Update tracking data
        table_data[table_key] = {
            'seq_no': max_seq_no,
            'total_docs': current_total_docs,
            'last_seen': current_time,
            'last_change': current_time if has_changes else prev_data.get('last_change'),
            'baseline_activity': prev_data.get('baseline_activity', 0) * 0.9 + seq_no_delta * 0.1
        }
        
        # Log result with anomaly indicator
        anomaly_indicator = " ⚠️" if is_anomaly else ""
        self._log_sample_result(table_key, seq_no_delta, docs_delta, query_time_ms, health_status + anomaly_indicator)
    
    def _determine_health_status(self, seq_no_delta: int, prev_data: Dict, table_data: Dict) -> str:
        """Determine health status based on activity patterns over time"""
        if seq_no_delta > 0:
            return "🟢"  # Active - sequence number changed this check
        
        # For tables with no current activity, check historical patterns
        baseline_activity = prev_data.get('baseline_activity', 0)
        last_change_time = prev_data.get('last_change')
        current_time = time.time()
        
        # If we've never seen activity, it's just stable (green)
        if baseline_activity == 0 and last_change_time is None:
            return "🟢"  # Stable table, no activity expected
        
        # If we've seen activity before but not recently
        if baseline_activity > 0 and last_change_time:
            time_since_last_change = current_time - last_change_time
            if time_since_last_change > 600:  # 10 minutes without activity
                return "🔴"  # Stale - had activity before, now inactive for long time
            elif time_since_last_change > 180:  # 3 minutes without activity  
                return "🟡"  # Slow - had activity before, now quiet
        
        return "🟢"  # Default to active/stable
    
    def _log_sample_result(self, table_key: str, seq_no_delta: int, docs_delta: int, 
                          query_time_ms: int, health_status: str):
        """Log the sample result in professional format"""
        # Format deltas
        seq_str = f"+{seq_no_delta}" if seq_no_delta > 0 else f"{seq_no_delta}" if seq_no_delta < 0 else "±0"
        docs_str = f"+{docs_delta}" if docs_delta > 0 else f"{docs_delta}" if docs_delta < 0 else "±0"
        
        # Performance indicator
        perf_indicator = "⚡" if query_time_ms > 1000 else ""
        
        # Log message
        logger.info(f"{health_status} {table_key} // _seq_no {seq_str} // total_docs {docs_str} // {query_time_ms}ms {perf_indicator}")
    
    def _get_table_key(self, table_info: Dict) -> str:
        """Generate a unique key for a table/partition"""
        schema = table_info['schema_name']
        table = table_info['table_name']
        partition_values = table_info.get('partition_values')
        
        base = f"{schema}.{table}" if schema and schema != "doc" else table
        
        if partition_values:
            try:
                partition_dict = json.loads(partition_values)
                partition_str = ",".join(f"{k}={v}" for k, v in partition_dict.items())
                return f"{base}[{partition_str}]"
            except (json.JSONDecodeError, TypeError):
                pass
        
        return base
    
    def _get_shard_distribution(self, table_info: Dict) -> Dict:
        """Get shard distribution for a specific table with targeted query"""
        schema = table_info['schema_name']
        table = table_info['table_name']
        partition_ident = table_info.get('partition_ident')
        
        # Build targeted query for specific table/partition
        query = """
        SELECT 
            sum(CASE WHEN primary = true THEN 1 ELSE 0 END) as primary_shards,
            sum(CASE WHEN primary = false THEN 1 ELSE 0 END) as replica_shards,
            avg(CASE WHEN primary = true THEN num_docs ELSE NULL END) as avg_docs_per_primary_shard
        FROM sys.shards 
        WHERE schema_name = ? 
          AND table_name = ?
        """
        
        params = [schema, table]
        
        # Add partition filter if needed
        if partition_ident:
            query += " AND partition_ident = ?"
            params.append(partition_ident)
        
        try:
            fresh_client = self._create_fresh_client()
            result = fresh_client.execute_query(query, params)
            
            if result.get('rows'):
                row = result['rows'][0]
                primary_shards = row[0] or 0
                replica_shards = row[1] or 0
                avg_docs_per_shard = row[2] or 0
                
                return {
                    'primary_shards': primary_shards,
                    'replica_shards': replica_shards, 
                    'avg_docs_per_primary_shard': int(avg_docs_per_shard)
                }
        except Exception as e:
            # Fallback if shard query fails
            pass
        
        return {
            'primary_shards': 0,
            'replica_shards': 0,
            'avg_docs_per_primary_shard': 0
        }
    
    def _create_fresh_client(self):
        """Create a fresh database client connection"""
        from ..database import CrateDBClient
        return CrateDBClient(self.client.connection_string)
    
    def _update_table_stats(self, table_key: str, table_info: Dict, query_time_ms: int, table_stats: Dict):
        """Update per-table statistics tracking"""
        if table_key not in table_stats:
            table_stats[table_key] = {
                'doc_changes': [],
                'performance': [],
                'anomalies': 0,
                'last_total_docs': table_info['total_docs']
            }
        
        # Calculate document change since last measurement
        current_docs = table_info['total_docs']
        last_docs = table_stats[table_key].get('last_total_docs', current_docs)
        doc_change = current_docs - last_docs
        
        # Track document changes and performance
        if last_docs != current_docs:  # Only track when there's a change
            table_stats[table_key]['doc_changes'].append(doc_change)
        
        table_stats[table_key]['performance'].append(query_time_ms)
        table_stats[table_key]['last_total_docs'] = current_docs
        
        # Store shard info for analysis (only once) 
        if 'shard_info' not in table_stats[table_key]:
            # Get shard distribution for this specific table
            shard_info = self._get_shard_distribution(table_info)
            table_stats[table_key]['shard_info'] = {
                'total_docs': table_info['total_docs'],
                'size_gb': table_info['size_gb'],
                'shard_distribution': shard_info
            }
        
        # Keep last 50 measurements for rolling statistics
        if len(table_stats[table_key]['doc_changes']) > 50:
            table_stats[table_key]['doc_changes'] = table_stats[table_key]['doc_changes'][-50:]
        if len(table_stats[table_key]['performance']) > 50:
            table_stats[table_key]['performance'] = table_stats[table_key]['performance'][-50:]
    
    def _detect_anomaly(self, table_key: str, seq_no_delta: int, docs_delta: int, 
                       prev_data: Dict, table_stats: Dict) -> bool:
        """Detect anomalies in table behavior"""
        if table_key not in table_stats:
            return False
        
        # Simple anomaly detection based on baseline activity
        baseline = prev_data.get('baseline_activity', 0)
        
        # Consider it an anomaly if activity is significantly different from baseline
        # (more than 3x the baseline activity or sudden large negative change)
        is_anomaly = False
        if baseline > 0 and abs(seq_no_delta) > baseline * 3:
            is_anomaly = True
        elif docs_delta < -1000:  # Large negative document change
            is_anomaly = True
        
        if is_anomaly:
            table_stats[table_key]['anomalies'] += 1
        
        return is_anomaly
    
    def _print_read_check_stats(self, stats: Dict, table_stats: Dict, cluster_name: str):
        """Print comprehensive statistics on exit"""
        runtime = datetime.now() - stats['start_time']
        runtime_str = str(runtime).split('.')[0]  # Remove microseconds
        
        self.console.print()
        self.console.print(Panel.fit(
            f"[bold blue]📊 Read Check Statistics [{cluster_name}][/bold blue]",
            border_style="blue"
        ))
        
        self.console.print(f"• Runtime: {runtime_str}")
        self.console.print(f"• Samples taken: [green]{stats['samples_taken']}[/green]")
        self.console.print(f"• Changes detected: [yellow]{stats['changes_detected']}[/yellow]")
        self.console.print(f"• Connection failures: [red]{stats['connection_failures']}[/red]")
        self.console.print(f"• Query failures: [red]{stats['query_failures']}[/red]")
        
        if stats['tables_queried']:
            self.console.print(f"\n• Tables monitored ({len(stats['tables_queried'])}):")
            for table in sorted(stats['tables_queried']):
                self.console.print(f"  - {table}")
        
        # Enhanced per-table statistics
        if table_stats:
            self.console.print(f"\n• Per-table statistics:")
            for table in sorted(table_stats.keys()):
                if table in stats['tables_queried']:
                    stats_data = table_stats[table]
                    
                    # Calculate document change statistics
                    doc_changes = stats_data['doc_changes']
                    if doc_changes:
                        total_change = sum(doc_changes)
                        avg_change = total_change / len(doc_changes)
                        max_change = max(doc_changes)
                        min_change = min(doc_changes)
                        change_stats = f"Δ {total_change:+,} (min/avg/max: {min_change:+}/{avg_change:+.0f}/{max_change:+})"
                    else:
                        change_stats = "no changes detected"
                    
                    # Calculate min/avg/max for performance
                    perf_values = stats_data['performance']
                    if perf_values:
                        perf_min = min(perf_values)
                        perf_max = max(perf_values)
                        perf_avg = sum(perf_values) / len(perf_values)
                        perf_stats = f"{perf_min}/{perf_avg:.0f}/{perf_max}ms"
                        
                        # Calculate shard performance analysis
                        shard_info = stats_data.get('shard_info', {})
                        total_docs = shard_info.get('total_docs', 0)
                        size_gb = shard_info.get('size_gb', 0)
                        shard_dist = shard_info.get('shard_distribution', {})
                        
                        if total_docs > 0 and perf_avg > 0:
                            docs_per_million = total_docs / 1_000_000
                            ms_per_million_docs = perf_avg / docs_per_million if docs_per_million > 0 else 0
                            
                            # Build shard distribution info
                            primary_shards = shard_dist.get('primary_shards', 0)
                            replica_shards = shard_dist.get('replica_shards', 0)  
                            avg_docs_per_shard = shard_dist.get('avg_docs_per_primary_shard', 0)
                            
                            shard_info_str = f"{primary_shards}P/{replica_shards}R shards"
                            if avg_docs_per_shard > 0:
                                shard_info_str += f", avg {avg_docs_per_shard/1_000_000:.1f}M docs/shard"
                            
                            shard_analysis = f" ({perf_avg:.0f}ms to scan {total_docs/1_000_000:.1f}M docs in {size_gb:.0f}GB = {ms_per_million_docs:.0f}ms per million docs, {shard_info_str})"
                        else:
                            shard_analysis = ""
                    else:
                        perf_stats = "N/A"
                        shard_analysis = ""
                    
                    anomaly_count = stats_data['anomalies']
                    anomaly_str = f" • {anomaly_count} anomalies" if anomaly_count > 0 else ""
                    
                    self.console.print(f"    [dim]{table}[/dim]")
                    self.console.print(f"      docs: {change_stats} • perf: {perf_stats}{anomaly_str}")
                    if shard_analysis:
                        self.console.print(f"      [dim]max(_seq_no) analysis:{shard_analysis}[/dim]")
        
        self.console.print()


def create_monitoring_commands(main_group):
    """Register monitoring commands with the main CLI group"""
    
    @main_group.command()
    @click.option('--table', '-t', help='Monitor recovery for specific table only')
    @click.option('--node', '-n', help='Monitor recovery on specific node only')
    @click.option('--watch', '-w', is_flag=True, help='Continuously monitor (refresh every 10s)')
    @click.option('--refresh-interval', default=10, help='Refresh interval for watch mode (seconds)')
    @click.option('--recovery-type', type=click.Choice(['PEER', 'DISK', 'all']), default='all', help='Filter by recovery type')
    @click.option('--include-transitioning', is_flag=True, help='Include completed recoveries still in transitioning state')
    @click.pass_context
    def monitor_recovery(ctx, table, node, watch, refresh_interval, recovery_type, include_transitioning):
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
        client = ctx.obj['client']
        commands = MonitoringCommands(client)
        commands.monitor_recovery(ctx, table, node, watch, refresh_interval, recovery_type, include_transitioning)

    @main_group.command()
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
    def active_shards(ctx, count, interval, min_checkpoint_delta, table, node, watch, exclude_system, min_rate, show_replicas):
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
        commands = MonitoringCommands(client)
        commands.active_shards(ctx, count, interval, min_checkpoint_delta, table, node, watch, exclude_system, min_rate, show_replicas)

    @main_group.command()
    @click.option('--translogsize', default=500, help='Minimum translog uncommitted size threshold in MB (default: 500)')
    @click.option('--interval', default=60, help='Monitoring interval in seconds for watch mode (default: 60)')
    @click.option('--watch', '-w', is_flag=True, help='Continuously monitor (refresh every interval)')
    @click.option('--table', '-t', help='Monitor specific table only')
    @click.option('--node', '-n', help='Monitor specific node only')
    @click.option('--count', default=50, help='Maximum number of shards with large translogs to show (default: 50)')
    @click.pass_context
    def large_translogs(ctx, translogsize, interval, watch, table, node, count):
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
        commands = MonitoringCommands(client)
        commands.large_translogs(ctx, translogsize, interval, watch, table, node, count)

    @main_group.command()
    @click.option('--seconds', default=30, help='Sampling interval in seconds (default: 30)')
    @click.pass_context
    def read_check(ctx, seconds):
        """Monitor cluster data readability by sampling from largest tables
        
        This professional monitoring tool continuously checks cluster health by:
        
        • 🔍 Discovering the 5 largest tables/partitions automatically
        • ⚡ Optimized max(_seq_no) queries every --seconds to detect write activity
        • 📊 Write activity scoring: Active/Slow/Stale based on _seq_no changes
        • 🎯 Query performance tracking with response time monitoring
        • 🔄 Fresh connections for each check (isolated testing)
        • 🛡️ Exponential backoff retry with comprehensive error handling
        • 📈 Baseline anomaly detection after establishing patterns
        • 🎯 Partition-aware querying with proper SQL generation
        
        HEALTH INDICATORS:
        • Write Activity: 🟢 Active (changing), 🟡 Slow (quiet), 🔴 Stale (inactive)
        • Query Performance: ⚡ Slow queries (>1000ms), ⚠️ Anomaly detection
        • Optimized queries using max() aggregation (no sorting/LIMIT)
        • Tracks both _seq_no and total_docs changes over time
        • Re-discovers largest tables every 10 minutes
        • Graceful degradation if individual tables fail
        • Professional statistics on CTRL+C
        
        LOGGING FORMAT:
        timestamp: schema.table // _seq_no ±0 // total_docs ±0 // 45ms [⚡ if slow]
        
        EXAMPLES:
            xmover read-check                           # Default: 30s interval
            xmover read-check --seconds 60              # Custom interval
            xmover read-check --seconds 10              # High-frequency monitoring
        
        This is the first XMover command to use loguru for professional logging.
        Perfect for detecting cluster availability issues and write activity patterns.
        """
        client = ctx.obj['client']
        commands = MonitoringCommands(client)
        commands.read_check(ctx, seconds)
                
