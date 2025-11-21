"""
Command line interface for XMover - CrateDB Shard Analyzer and Movement Tool
"""

import sys
import time
from typing import Optional
try:
    import click
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich import box
except ImportError as e:
    print(f"Missing required dependency: {e}")
    print("Please install dependencies with: pip install -e .")
    sys.exit(1)

from .database import CrateDBClient
from .analyzer import ShardAnalyzer, MoveRecommendation
from .distribution_analyzer import DistributionAnalyzer
from .shard_size_monitor import ShardSizeMonitor
from .utils import format_size
# Formatting utilities are now used by command modules
from .commands import create_diagnostics_commands, create_analysis_commands, create_monitoring_commands, create_operations_commands, create_maintenance_commands


console = Console()




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


# explain_error command is now handled by DiagnosticsCommands module
# See src/xmover/commands/diagnostics.py


def _wait_for_recovery_capacity(client, max_concurrent_recoveries: int = 5):
    """Wait until active recovery count is below threshold"""
    from xmover.analyzer import RecoveryMonitor
    # Using time.sleep from main imports
    
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
            
            time.sleep(10)  # Check every 10 seconds
            wait_time += 10


def _execute_recommendations_safely(client, recommendations, validate: bool):
    """Execute recommendations with extensive safety measures"""
    # Using existing time import
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


# Register command modules
create_diagnostics_commands(main)
create_analysis_commands(main)
create_monitoring_commands(main)
create_operations_commands(main)
create_maintenance_commands(main)

if __name__ == '__main__':
    main()