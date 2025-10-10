"""
Diagnostic Commands for XMover CLI

This module contains diagnostic command handlers extracted from the monolithic CLI file.
Handles: test-connection, explain-error, check-balance, zone-analysis commands.
"""

from typing import Optional
import click
from rich.panel import Panel

from .base import BaseCommand
from ..analyzer import ShardAnalyzer


class DiagnosticsCommands(BaseCommand):
    """Command handlers for diagnostic operations"""
    
    def execute(self, command: str, **kwargs) -> None:
        """Execute a diagnostic command by name"""
        if command == 'test_connection':
            self.test_connection(**kwargs)
        elif command == 'explain_error':
            self.explain_error(**kwargs)
        elif command == 'check_balance':
            self.check_balance(**kwargs)
        elif command == 'zone_analysis':
            self.zone_analysis(**kwargs)
        else:
            raise ValueError(f"Unknown diagnostic command: {command}")
    
    def test_connection(self, connection_string: Optional[str] = None, verbose: bool = False) -> None:
        """Test connection to CrateDB cluster"""
        try:
            from ..database import CrateDBClient
            
            if connection_string:
                test_client = CrateDBClient(connection_string)
            else:
                test_client = self.client
            
            self.print_header("CrateDB Connection Test")
            
            if test_client.test_connection():
                self.console.print("[green]✅ Successfully connected to CrateDB cluster[/green]")
                
                # Get cluster health summary first
                try:
                    health = test_client.get_cluster_health_summary()
                    if health:
                        status_color = "green" if health['cluster_health'] == 'GREEN' else ("yellow" if health['cluster_health'] == 'YELLOW' else "red")
                        self.console.print(f"[blue]🏥 Cluster Health:[/blue] [{status_color}]{health['cluster_health']}[/{status_color}]")
                        
                        # Show entity breakdown if there are issues
                        if health['yellow_entities'] > 0 or health['red_entities'] > 0:
                            self.console.print(f"  • Issues: {health['red_entities']} RED, {health['yellow_entities']} YELLOW entities")
                        
                        self.console.print(f"  • Tables: {health['total_tables']}, Partitions: {health['total_partitions']}")
                except Exception as e:
                    self.console.print(f"[yellow]⚠️  Cluster health unavailable: {e}[/yellow]")
                
                # Get basic cluster info
                try:
                    nodes = test_client.get_nodes_info()
                    self.console.print(f"[blue]📊 Cluster Info:[/blue]")
                    self.console.print(f"  • Nodes: {len(nodes)}")
                    
                    zones = set(node.zone for node in nodes if node.zone and node.zone != 'unknown')
                    if zones:
                        self.console.print(f"  • Zones: {len(zones)} ({', '.join(sorted(zones))})")
                    
                    if verbose:
                        self.console.print(f"\n[blue]📋 Detailed Node Information:[/blue]")
                        
                        # Get master node ID
                        master_node_id = None
                        try:
                            master_node_id = test_client.get_master_node_id()
                        except Exception:
                            pass  # Master node info not available
                        
                        # Add legend
                        legend_parts = ["🔥 Critical (>90% heap)", "⚠️ Warning (>75% heap)", "💾 Disk Critical (>90%)", "📁 Disk Warning (>85%)", "✅ Healthy"]
                        if master_node_id:
                            legend_parts.append("👑 Master node")
                        self.console.print(f"[dim]    Legend: {' | '.join(legend_parts)}[/dim]")
                        
                        def get_severity_score(node):
                            """Calculate severity score for sorting (higher = more critical)"""
                            if node.heap_max <= 1 and node.fs_total == 0:
                                return 100  # Corrupted metadata - highest priority
                            
                            heap_pct = (node.heap_used / node.heap_max * 100) if node.heap_max > 0 else 0
                            disk_pct = (node.fs_used / node.fs_total * 100) if node.fs_total > 0 else 0
                            
                            severity = 0
                            if heap_pct > 90:
                                severity += 50
                            elif heap_pct > 75:
                                severity += 25
                                
                            if disk_pct > 90:
                                severity += 40
                            elif disk_pct > 85:
                                severity += 20
                                
                            return severity
                        
                        # Count nodes by severity for summary
                        critical_nodes = 0
                        warning_nodes = 0
                        healthy_nodes = 0
                        corrupted_nodes = 0
                        
                        for node in nodes:
                            if node.heap_max <= 1 and node.fs_total == 0:
                                corrupted_nodes += 1
                            else:
                                severity_score = get_severity_score(node)
                                if severity_score >= 50:
                                    critical_nodes += 1
                                elif severity_score >= 25:
                                    warning_nodes += 1
                                else:
                                    healthy_nodes += 1
                        
                        # Display severity summary
                        if critical_nodes > 0 or warning_nodes > 0 or corrupted_nodes > 0:
                            summary_parts = []
                            if critical_nodes > 0:
                                summary_parts.append(f"[red]{critical_nodes} Critical[/red]")
                            if warning_nodes > 0:
                                summary_parts.append(f"[yellow]{warning_nodes} Warning[/yellow]")
                            if corrupted_nodes > 0:
                                summary_parts.append(f"[red]{corrupted_nodes} Corrupted[/red]")
                            if healthy_nodes > 0:
                                summary_parts.append(f"[green]{healthy_nodes} Healthy[/green]")
                            
                            self.console.print(f"[dim]    Summary: {' | '.join(summary_parts)}[/dim]")
                        else:
                            self.console.print(f"[dim]    Summary: [green]{healthy_nodes} Healthy nodes[/green][/dim]")
                        
                        self.console.print("")  # Add blank line before node details
                        
                        # Sort by severity (descending), then by name (ascending)
                        sorted_nodes = sorted(nodes, key=lambda n: (-get_severity_score(n), n.name))
                        
                        for node in sorted_nodes:
                            heap_pct = (node.heap_used / node.heap_max * 100) if node.heap_max > 0 else 0
                            disk_pct = (node.fs_used / node.fs_total * 100) if node.fs_total > 0 else 0
                            disk_free_gb = node.fs_available / (1024**3) if node.fs_available > 0 else 0
                            heap_used_gb = node.heap_used / (1024**3) if node.heap_used > 0 else 0
                            heap_max_gb = node.heap_max / (1024**3) if node.heap_max > 0 else 0
                            
                            # Determine status indicators
                            status_indicators = []
                            if heap_pct > 90:
                                status_indicators.append("🔥")
                            elif heap_pct > 75:
                                status_indicators.append("⚠️")
                            
                            if disk_pct > 90:
                                status_indicators.append("💾")
                            elif disk_pct > 85:
                                status_indicators.append("📁")
                            
                            if not status_indicators:
                                status_indicators.append("✅")
                            
                            status_str = " ".join(status_indicators)
                            
                            # Check if this is the master node
                            is_master = master_node_id and node.id == master_node_id
                            master_symbol = " 👑" if is_master else ""
                            
                            # Handle nodes with missing metadata
                            if node.heap_max <= 1 and node.fs_total == 0:
                                self.console.print(f"      • [red]{node.name}[/red] ({node.zone}): [dim]Metadata unavailable[/dim] ⚠️{master_symbol}")
                            else:
                                # Determine node name color based on severity
                                severity_score = get_severity_score(node)
                                if severity_score >= 50:
                                    name_color = "red"
                                elif severity_score >= 25:
                                    name_color = "yellow"
                                else:
                                    name_color = "green"
                                
                                self.console.print(f"      • [{name_color}]{node.name}[/{name_color}] ([dim]{node.zone}[/dim]): Heap {heap_pct:.1f}% ([cyan]{heap_used_gb:.1f}GB/{heap_max_gb:.1f}GB[/cyan]), Disk {disk_pct:.1f}% ([cyan]{disk_free_gb:.1f}GB free[/cyan]) {status_str}{master_symbol}")
                    
                except Exception as e:
                    self.console.print(f"[yellow]⚠️  Basic cluster info unavailable: {e}[/yellow]")
            else:
                self.console.print("[red]❌ Failed to connect to CrateDB cluster[/red]")
                self.console.print("[yellow]💡 Check your connection configuration[/yellow]")
                
        except Exception as e:
            self.handle_error(e, "testing connection")
    
    def explain_error(self, error_message: Optional[str] = None) -> None:
        """Explain CrateDB allocation error messages and provide solutions"""
        self.print_header("CrateDB Error Message Decoder")
        
        if not error_message:
            self.console.print("[blue]Enter a CrateDB error message to analyze:[/blue]")
            error_message = input("> ").strip()
        
        if not error_message:
            self.console.print("[yellow]No error message provided[/yellow]")
            return
        
        self.console.print(f"\n[bold]Analyzing error:[/bold] [red]{error_message}[/red]\n")
        
        # Error pattern matching and explanations
        error_lower = error_message.lower()
        
        if "no(a copy of this shard is already allocated to this node)" in error_lower:
            self._explain_already_allocated_error()
        elif "no(allocation id does not match)" in error_lower:
            self._explain_allocation_id_mismatch()
        elif "no(disk usage exceeded)" in error_lower:
            self._explain_disk_usage_exceeded()
        elif "no(not allowed to allocate on same node)" in error_lower:
            self._explain_same_node_allocation()
        elif "unassigned_info" in error_lower:
            self._explain_unassigned_shard()
        elif "watermark" in error_lower:
            self._explain_watermark_exceeded()
        else:
            self._explain_generic_error()
    
    def check_balance(self, table: Optional[str] = None, tolerance: float = 10.0) -> None:
        """Check zone balance for shards"""
        if not self.validate_connection():
            return
        
        analyzer = ShardAnalyzer(self.client)
        
        self.print_header("Zone Balance Check")
        self.console.print("[dim]Note: Analyzing all shards regardless of state for complete cluster view[/dim]")
        self.console.print()
        
        try:
            balance_info = analyzer.check_zone_balance(table, tolerance)
            
            if not balance_info:
                self.console.print("[yellow]No shards found for analysis[/yellow]")
                return
            
            # Calculate totals and targets for balance assessment
            total_shards = sum(stats['TOTAL'] for stats in balance_info.values())
            zones = list(balance_info.keys())
            target_per_zone = total_shards // len(zones) if zones else 0
            tolerance_range = (
                target_per_zone * (1 - tolerance / 100),
                target_per_zone * (1 + tolerance / 100)
            )
            
            # Display balance statistics
            balanced = True
            for zone, stats in balance_info.items():
                total = stats['TOTAL']
                
                if not (tolerance_range[0] <= total <= tolerance_range[1]):
                    balanced = False
                    
                if tolerance_range[0] <= total <= tolerance_range[1]:
                    color = "green"
                    status = "✅ Balanced"
                elif total < tolerance_range[0]:
                    color = "yellow"
                    status = f"⚠ Under ({total - target_per_zone:+})"
                else:
                    color = "red" 
                    status = f"⚠ Over ({total - target_per_zone:+})"
                
                self.console.print(f"  [{color}]{zone}[/{color}]: {total} shards - {status}")
            
            if balanced:
                self.console.print(f"\n[green]✅ Cluster zones are well balanced[/green]")
            else:
                self.console.print(f"\n[yellow]⚠️  Zone imbalance detected - consider rebalancing[/yellow]")
                
        except Exception as e:
            self.handle_error(e, "checking zone balance")
    
    def zone_analysis(self, table: Optional[str] = None, show_shards: bool = False) -> None:
        """Detailed analysis of zone distribution and potential conflicts"""
        if not self.validate_connection():
            return
        
        self.print_header("Detailed Zone Analysis", 
                         "Comprehensive zone distribution analysis for CrateDB cluster")
        
        try:
            analyzer = ShardAnalyzer(self.client)
            
            # Get all shards for analysis
            shards = self.client.get_shards_info(table_name=table, for_analysis=True)
            
            if not shards:
                self.console.print("[yellow]No shards found for analysis[/yellow]")
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
                self.console.print(f"\n[bold cyan]Table: {table_name}[/bold cyan]")
                
                # Create analysis table
                from rich.table import Table
                from rich import box
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
                            self.console.print(f"    {health_indicator} {shard_copy.shard_type} on {shard_copy.node_name} ({shard_copy.zone}) - {shard_copy.routing_state}")
                
                self.console.print(analysis_table)
            
            # Summary
            self.console.print(f"\n[bold]Zone Analysis Summary:[/bold]")
            self.console.print(f"  • Tables analyzed: [cyan]{len(tables)}[/cyan]")
            self.console.print(f"  • Zone conflicts detected: [red]{zone_conflicts}[/red]")
            self.console.print(f"  • Under-replicated shards: [yellow]{under_replicated}[/yellow]")
            
            if zone_conflicts > 0:
                self.console.print(f"\n[red]⚠ Found {zone_conflicts} zone conflicts that need attention![/red]")
                self.console.print("[dim]Zone conflicts occur when all copies of a shard are in the same zone.[/dim]")
                self.console.print("[dim]This violates CrateDB's zone-awareness and creates availability risks.[/dim]")
            
            if under_replicated > 0:
                self.console.print(f"\n[yellow]⚠ Found {under_replicated} under-replicated shards.[/yellow]")
                self.console.print("[dim]Consider increasing replication for better availability.[/dim]")
            
            if zone_conflicts == 0 and under_replicated == 0:
                self.console.print("\n[green]✓ No critical zone distribution issues detected![/green]")
                
        except Exception as e:
            self.handle_error(e, "performing zone analysis")
    
    def _explain_already_allocated_error(self) -> None:
        """Explain already allocated shard error"""
        panel_content = """[bold red]Shard Already Allocated Error[/bold red]

[bold]Cause:[/bold]
You're trying to move a shard to a node that already has a copy (primary or replica) of the same shard.

[bold]Solutions:[/bold]
• Check current shard allocation: [cyan]xmover analyze -t your_table[/cyan]
• Move to a different node that doesn't have this shard
• If moving replicas, ensure target node doesn't have primary
• Use [cyan]xmover find-candidates[/cyan] to find suitable target nodes

[bold]Prevention:[/bold]
• Always validate moves with: [cyan]xmover validate-move[/cyan]
• Use XMover's recommend command for safe suggestions"""
        
        self.console.print(Panel(panel_content, border_style="red"))
    
    def _explain_allocation_id_mismatch(self) -> None:
        """Explain allocation ID mismatch error"""
        panel_content = """[bold red]Allocation ID Mismatch Error[/bold red]

[bold]Cause:[/bold]
The shard's internal allocation ID doesn't match the cluster's expectation.
This usually happens after node restarts or network splits.

[bold]Solutions:[/bold]
• Wait for cluster to stabilize and retry the operation
• Check cluster health: [cyan]SELECT health FROM sys.health[/cyan]
• If persistent, cancel and retry: [cyan]ALTER TABLE ... REROUTE CANCEL SHARD ...[/cyan]
• Monitor recovery: [cyan]xmover monitor-recovery[/cyan]

[bold]When to Act:[/bold]
• Only intervene if error persists for >30 minutes
• Let CrateDB auto-recover first"""
        
        self.console.print(Panel(panel_content, border_style="red"))
    
    def _explain_disk_usage_exceeded(self) -> None:
        """Explain disk usage exceeded error"""
        panel_content = """[bold red]Disk Usage Exceeded Error[/bold red]

[bold]Cause:[/bold]
Target node has insufficient disk space for the shard.
CrateDB enforces disk watermarks (typically 85% low, 90% high).

[bold]Solutions:[/bold]
• Check disk usage: [cyan]xmover analyze[/cyan] (shows node disk usage)
• Move shards FROM the target node first to free space
• Use [cyan]xmover find-candidates --min-free-space 200[/cyan] for nodes with space
• Consider adding more storage or nodes

[bold]Best Practices:[/bold]
• Keep nodes below 80% disk usage
• Monitor with: [cyan]xmover active-shards[/cyan]
• Plan capacity proactively"""
        
        self.console.print(Panel(panel_content, border_style="red"))
    
    def _explain_same_node_allocation(self) -> None:
        """Explain same node allocation error"""
        panel_content = """[bold red]Same Node Allocation Error[/bold red]

[bold]Cause:[/bold]
Attempting to allocate primary and replica of same shard on same node.
CrateDB prevents this for data safety.

[bold]Solutions:[/bold]
• Choose a different target node
• Check shard distribution: [cyan]xmover analyze -t your_table[/cyan]
• Use [cyan]xmover zone-analysis[/cyan] to see node assignments
• Ensure you have enough nodes for your replica settings

[bold]Requirements:[/bold]
• Need N+1 nodes for N replicas
• Each shard copy must be on different node"""
        
        self.console.print(Panel(panel_content, border_style="red"))
    
    def _explain_unassigned_shard(self) -> None:
        """Explain unassigned shard error"""
        panel_content = """[bold red]Unassigned Shard Error[/bold red]

[bold]Cause:[/bold]
Shard cannot be allocated to any node due to allocation constraints.
Common reasons: insufficient nodes, disk space, or allocation filters.

[bold]Diagnosis:[/bold]
• Check cluster status: [cyan]SELECT * FROM sys.allocations WHERE state = 'UNASSIGNED'[/cyan]
• Monitor recovery: [cyan]xmover monitor-recovery --include-transitioning[/cyan]
• Check allocation explain: Use CrateDB's allocation explain API

[bold]Solutions:[/bold]
• Add more nodes if insufficient
• Free disk space on existing nodes  
• Check and adjust allocation filters
• Review routing allocation settings"""
        
        self.console.print(Panel(panel_content, border_style="red"))
    
    def _explain_watermark_exceeded(self) -> None:
        """Explain watermark exceeded error"""
        panel_content = """[bold red]Watermark Exceeded Error[/bold red]

[bold]Cause:[/bold]
Node disk usage exceeds CrateDB's watermark thresholds.
Default: 85% (low), 90% (high), 95% (flood stage).

[bold]Immediate Actions:[/bold]
• Check current usage: [cyan]xmover analyze[/cyan]
• Move shards away: [cyan]xmover recommend --prioritize-space[/cyan]
• Free disk space externally if possible

[bold]Prevention:[/bold]
• Monitor regularly: [cyan]xmover active-shards --watch[/cyan]
• Set up alerting at 75% usage
• Plan capacity expansion proactively
• Use [cyan]xmover shard-distribution[/cyan] for planning"""
        
        self.console.print(Panel(panel_content, border_style="red"))
    
    def _explain_generic_error(self) -> None:
        """Explain generic/unknown error"""
        panel_content = """[bold yellow]Generic Error Analysis[/bold yellow]

[bold]Common CrateDB Error Patterns:[/bold]

[bold]Connection Issues:[/bold]
• Check cluster health and node status
• Verify network connectivity
• Test with: [cyan]xmover test-connection[/cyan]

[bold]Allocation Issues:[/bold]
• Use [cyan]xmover analyze[/cyan] for current state
• Check [cyan]xmover zone-analysis[/cyan] for conflicts
• Validate with [cyan]xmover validate-move[/cyan]

[bold]Recovery Issues:[/bold]
• Monitor with [cyan]xmover monitor-recovery[/cyan]
• Check for large translogs: [cyan]xmover large-translogs[/cyan]
• Look for problematic shards: [cyan]xmover problematic-translogs[/cyan]

[bold]For More Help:[/bold]
• Check CrateDB documentation
• Use XMover's diagnostic commands
• Consider cluster health analysis"""
        
        self.console.print(Panel(panel_content, border_style="yellow"))
    



# Click command wrappers that use the DiagnosticsCommands class
def create_diagnostics_commands(main_group):
    """Create and register diagnostic command handlers"""
    
    @main_group.command()
    @click.option('--connection-string', help='Override connection string from .env')
    @click.option('--verbose', '-v', is_flag=True, help='Show detailed node information including resource usage')
    @click.pass_context
    def test_connection(ctx, connection_string: Optional[str], verbose: bool):
        """Test connection to CrateDB cluster"""
        client = ctx.obj['client']
        cmd = DiagnosticsCommands(client)
        cmd.test_connection(connection_string, verbose)
    
    @main_group.command()
    @click.argument('error_message', required=False)
    @click.pass_context
    def explain_error(ctx, error_message: Optional[str]):
        """Explain CrateDB allocation error messages and provide solutions

        ERROR_MESSAGE: The CrateDB error message to analyze (optional - can be provided interactively)

        Example: xmover explain-error "NO(a copy of this shard is already allocated to this node)"
        """
        client = ctx.obj['client']
        cmd = DiagnosticsCommands(client)
        cmd.explain_error(error_message)
    
    @main_group.command()
    @click.option('--table', '-t', help='Check balance for specific table only')
    @click.option('--tolerance', default=10.0, help='Zone balance tolerance percentage (default: 10)')
    @click.pass_context
    def check_balance(ctx, table: Optional[str], tolerance: float):
        """Check zone balance for shards"""
        client = ctx.obj['client']
        cmd = DiagnosticsCommands(client)
        cmd.check_balance(table, tolerance)
    
    @main_group.command()
    @click.option('--table', '-t', help='Analyze zones for specific table only')
    @click.option('--show-shards/--no-show-shards', default=False, help='Show individual shard details (default: False)')
    @click.pass_context
    def zone_analysis(ctx, table: Optional[str], show_shards: bool):
        """Detailed analysis of zone distribution and potential conflicts"""
        client = ctx.obj['client']
        cmd = DiagnosticsCommands(client)
        cmd.zone_analysis(table, show_shards)