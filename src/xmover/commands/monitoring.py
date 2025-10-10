"""
Monitoring commands for XMover CLI - handles cluster monitoring operations
"""

import time
from typing import Optional
import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

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
                
