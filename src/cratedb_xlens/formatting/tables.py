"""
Table formatting utilities for XMover CLI
"""

from typing import List, Optional, Any, Dict
from rich.table import Table
from rich.console import Console
from rich import box


class RichTableFormatter:
    """Rich table formatting utilities for XMover CLI"""
    
    def __init__(self, console: Optional[Console] = None):
        self.console = console or Console()
    
    @staticmethod
    def create_shard_table(shards: List[Any], title: str = "Shard Information") -> Table:
        """Create a formatted table for shard information"""
        table = Table(title=title, box=box.ROUNDED)
        
        # Add columns
        table.add_column("Table", style="cyan", no_wrap=True)
        table.add_column("Partition", style="dim", no_wrap=True)
        table.add_column("Shard", justify="right")
        table.add_column("Node", style="blue")
        table.add_column("Type", justify="center")
        table.add_column("Size", justify="right", style="green")
        table.add_column("Docs", justify="right")
        table.add_column("Zone", style="yellow")
        
        for shard in shards:
            # Handle different shard info formats
            schema_name = getattr(shard, 'schema_name', '')
            table_name = getattr(shard, 'table_name', 'unknown')
            
            # Format full table name
            if schema_name and schema_name != 'doc':
                full_table_name = f"{schema_name}.{table_name}"
            else:
                full_table_name = table_name
                
            # Get partition information
            partition_ident = getattr(shard, 'partition_ident', None)
            partition_display = partition_ident if partition_ident else "—"
            
            shard_id = str(getattr(shard, 'shard_id', '?'))
            node_name = getattr(shard, 'node_name', 'unknown')
            shard_type = getattr(shard, 'shard_type', 'unknown')
            
            # Format size
            size_gb = getattr(shard, 'size_gb', 0) or 0
            if size_gb >= 1:
                size_str = f"{size_gb:.1f}GB"
            else:
                size_mb = size_gb * 1000
                size_str = f"{size_mb:.0f}MB"
            
            # Format document count
            doc_count = getattr(shard, 'doc_count', 0) or 0
            doc_str = f"{doc_count:,}" if doc_count > 0 else "-"
            
            # Get zone info
            zone = getattr(shard, 'zone', 'unknown')
            
            table.add_row(
                full_table_name,
                partition_display,
                shard_id,
                node_name,
                shard_type,
                size_str,
                doc_str,
                zone
            )
        
        return table
    
    @staticmethod
    def create_recovery_table(recoveries: List[Any], title: str = "Recovery Status") -> Table:
        """Create a formatted table for recovery information"""
        table = Table(title=title, box=box.ROUNDED)
        
        # Add columns
        table.add_column("Table", style="cyan")
        table.add_column("Partition", style="dim", no_wrap=True)
        table.add_column("Shard", justify="right")
        table.add_column("Stage", style="blue")
        table.add_column("Progress", justify="right")
        table.add_column("From → To", style="yellow")
        table.add_column("Size", justify="right", style="green")
        table.add_column("Time", justify="right")
        
        for recovery in recoveries:
            # Get basic info
            schema_name = getattr(recovery, 'schema_name', '')
            table_name = getattr(recovery, 'table_name', 'unknown')
            
            # Format full table name
            if schema_name and schema_name != 'doc':
                full_table_name = f"{schema_name}.{table_name}"
            else:
                full_table_name = table_name
                
            # Get partition information
            partition_ident = getattr(recovery, 'partition_ident', None)
            partition_display = partition_ident if partition_ident else "—"
            
            shard_id = str(getattr(recovery, 'shard_id', '?'))
            stage = getattr(recovery, 'stage', 'unknown')
            
            # Format progress
            progress = getattr(recovery, 'overall_progress', 0)
            if progress >= 100:
                progress_str = "[green]100%[/green]"
            elif progress >= 75:
                progress_str = f"[blue]{progress:.1f}%[/blue]"
            elif progress >= 25:
                progress_str = f"[yellow]{progress:.1f}%[/yellow]"
            else:
                progress_str = f"[red]{progress:.1f}%[/red]"
            
            # Format nodes
            source_node = getattr(recovery, 'source_node', 'unknown')
            target_node = getattr(recovery, 'target_node', 'unknown')
            nodes_str = f"{source_node} → {target_node}"
            
            # Format size
            size_gb = getattr(recovery, 'size_gb', 0) or 0
            if size_gb >= 1:
                size_str = f"{size_gb:.1f}GB"
            else:
                size_mb = size_gb * 1000
                size_str = f"{size_mb:.0f}MB"
            
            # Format time
            time_seconds = getattr(recovery, 'total_time_seconds', 0)
            if time_seconds > 0:
                if time_seconds > 3600:
                    time_str = f"{time_seconds/3600:.1f}h"
                elif time_seconds > 60:
                    time_str = f"{time_seconds/60:.1f}m"
                else:
                    time_str = f"{time_seconds:.0f}s"
            else:
                time_str = "-"
            
            table.add_row(
                full_table_name,
                partition_display,
                shard_id,
                stage,
                progress_str,
                nodes_str,
                size_str,
                time_str
            )
        
        return table
    
    @staticmethod
    def create_node_table(nodes: List[Any], title: str = "Node Information") -> Table:
        """Create a formatted table for node information"""
        table = Table(title=title, box=box.ROUNDED)
        
        # Add columns
        table.add_column("Node", style="cyan")
        table.add_column("Zone", style="yellow")
        table.add_column("Shards", justify="right")
        table.add_column("Disk Usage", justify="right")
        table.add_column("Heap Usage", justify="right")
        table.add_column("Load", justify="right")
        table.add_column("Status", justify="center")
        
        for node in nodes:
            node_name = getattr(node, 'name', 'unknown')
            zone = getattr(node, 'zone', 'unknown')
            
            # Shard count
            shard_count = getattr(node, 'shard_count', 0)
            shard_str = str(shard_count) if shard_count > 0 else "-"
            
            # Disk usage
            disk_usage = getattr(node, 'disk_usage_percent', 0)
            if disk_usage > 90:
                disk_str = f"[red]{disk_usage:.1f}%[/red]"
            elif disk_usage > 80:
                disk_str = f"[yellow]{disk_usage:.1f}%[/yellow]"
            else:
                disk_str = f"[green]{disk_usage:.1f}%[/green]"
            
            # Heap usage
            heap_usage = getattr(node, 'heap_usage_percent', 0)
            if heap_usage > 85:
                heap_str = f"[red]{heap_usage:.1f}%[/red]"
            elif heap_usage > 75:
                heap_str = f"[yellow]{heap_usage:.1f}%[/yellow]"
            else:
                heap_str = f"[green]{heap_usage:.1f}%[/green]"
            
            # Load average
            load_avg = getattr(node, 'load_avg', 0)
            load_str = f"{load_avg:.2f}" if load_avg > 0 else "-"
            
            # Status
            status = getattr(node, 'status', 'unknown')
            if status.lower() == 'online':
                status_str = "[green]●[/green] Online"
            elif status.lower() == 'offline':
                status_str = "[red]●[/red] Offline"
            else:
                status_str = "[yellow]●[/yellow] Unknown"
            
            table.add_row(
                node_name,
                zone,
                shard_str,
                disk_str,
                heap_str,
                load_str,
                status_str
            )
        
        return table
    
    @staticmethod
    def create_table_summary_table(tables: List[Dict[str, Any]], title: str = "Table Summary") -> Table:
        """Create a formatted table for table summary information"""
        table = Table(title=title, box=box.ROUNDED)
        
        # Add columns
        table.add_column("Table", style="cyan")
        table.add_column("Partition", style="dim", no_wrap=True)
        table.add_column("Shards", justify="right")
        table.add_column("Size", justify="right", style="green")
        table.add_column("Documents", justify="right")
        table.add_column("Replicas", justify="right")
        table.add_column("Zones", justify="right")
        
        for table_info in tables:
            name = table_info.get('name', 'unknown')
            partition_ident = table_info.get('partition_ident', None)
            partition_display = partition_ident if partition_ident else "—"
            shard_count = table_info.get('shard_count', 0)
            
            # Format size
            size_gb = table_info.get('total_size_gb', 0) or 0
            if size_gb >= 1000:
                size_str = f"{size_gb/1000:.1f}TB"
            elif size_gb >= 1:
                size_str = f"{size_gb:.1f}GB"
            else:
                size_str = f"{size_gb*1000:.0f}MB"
            
            # Format document count
            doc_count = table_info.get('total_docs', 0) or 0
            if doc_count >= 1_000_000:
                doc_str = f"{doc_count/1_000_000:.1f}M"
            elif doc_count >= 1_000:
                doc_str = f"{doc_count/1_000:.1f}K"
            else:
                doc_str = str(doc_count)
            
            replica_count = table_info.get('replica_count', 0)
            zone_count = table_info.get('zone_count', 0)
            
            table.add_row(
                name,
                partition_display,
                str(shard_count),
                size_str,
                doc_str,
                str(replica_count),
                str(zone_count)
            )
        
        return table
    
    @staticmethod
    def create_distribution_table(distribution_data: List[Dict[str, Any]], title: str = "Distribution Analysis") -> Table:
        """Create a formatted table for distribution analysis"""
        table = Table(title=title, box=box.ROUNDED)
        
        # Add columns
        table.add_column("Item", style="cyan")
        table.add_column("Count", justify="right")
        table.add_column("Percentage", justify="right")
        table.add_column("Status", justify="center")
        
        for item in distribution_data:
            name = item.get('name', 'unknown')
            count = item.get('count', 0)
            percentage = item.get('percentage', 0)
            
            # Format percentage with color coding
            if percentage > 40:
                pct_str = f"[red]{percentage:.1f}%[/red]"
                status = "[red]⚠️[/red]"
            elif percentage > 25:
                pct_str = f"[yellow]{percentage:.1f}%[/yellow]"
                status = "[yellow]⚠️[/yellow]"
            else:
                pct_str = f"[green]{percentage:.1f}%[/green]"
                status = "[green]✓[/green]"
            
            table.add_row(name, str(count), pct_str, status)
        
        return table
    
    def print_table(self, table: Table):
        """Print a table to the console"""
        self.console.print(table)
    
    def print_tables_side_by_side(self, tables: List[Table]):
        """Print multiple tables side by side (if they fit)"""
        from rich.columns import Columns
        self.console.print(Columns(tables))