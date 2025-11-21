"""
Progress formatting utilities for XMover CLI
"""

from typing import Optional
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.console import Console


class ProgressFormatter:
    """Progress bar and status formatting utilities"""
    
    def __init__(self, console: Optional[Console] = None):
        self.console = console or Console()
    
    @staticmethod
    def format_recovery_progress(recovery_info) -> str:
        """Format recovery progress information"""
        if hasattr(recovery_info, 'overall_progress'):
            progress = recovery_info.overall_progress
            stage = getattr(recovery_info, 'stage', 'Unknown')
            
            # Color code based on progress
            if progress >= 100:
                color = "green"
                status = "✓"
            elif progress >= 75:
                color = "blue"
                status = "▶"
            elif progress >= 25:
                color = "yellow"
                status = "▶"
            else:
                color = "red"
                status = "▶"
            
            return f"[{color}]{status} {progress:.1f}% ({stage})[/{color}]"
        
        return "[dim]No progress info[/dim]"
    
    @staticmethod
    def format_translog_info(shard_info, include_size: bool = True) -> str:
        """Format translog information for display"""
        if not hasattr(shard_info, 'translog_stats') or not shard_info.translog_stats:
            return "[dim]No translog data[/dim]"
        
        translog = shard_info.translog_stats
        parts = []
        
        # Uncommitted operations
        if 'uncommitted_operations' in translog:
            ops = translog['uncommitted_operations']
            if ops > 0:
                parts.append(f"{ops:,} ops")
        
        # Uncommitted size
        if include_size and 'uncommitted_size' in translog:
            size_bytes = translog['uncommitted_size']
            if size_bytes > 0:
                size_mb = size_bytes / (1024 * 1024)
                if size_mb >= 1024:
                    parts.append(f"{size_mb/1024:.1f}GB")
                else:
                    parts.append(f"{size_mb:.1f}MB")
        
        # Generation info
        if 'min_referenced_seq_no' in translog and 'max_seq_no' in translog:
            min_seq = translog['min_referenced_seq_no']
            max_seq = translog['max_seq_no']
            if max_seq > min_seq:
                gap = max_seq - min_seq
                parts.append(f"gap:{gap:,}")
        
        if not parts:
            return "[dim]Empty[/dim]"
        
        result = " | ".join(parts)
        
        # Color code based on size/operations
        if any(term in result.lower() for term in ['gb', 'mb']) and ('gb' in result.lower() or '5' in result):
            return f"[red]{result}[/red]"
        elif any(term in result.lower() for term in ['mb']) and any(char.isdigit() for char in result):
            return f"[yellow]{result}[/yellow]"
        else:
            return f"[green]{result}[/green]"
    
    def create_progress_bar(self, description: str = "Processing") -> Progress:
        """Create a rich progress bar"""
        return Progress(
            TextColumn("[bold blue]{task.description}"),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeRemainingColumn(),
            console=self.console
        )
    
    def format_time_remaining(self, seconds: float) -> str:
        """Format time remaining in human readable format"""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            minutes = seconds / 60
            return f"{minutes:.1f}m"
        else:
            hours = seconds / 3600
            return f"{hours:.1f}h"
    
    def format_throughput(self, bytes_per_second: float) -> str:
        """Format throughput in human readable format"""
        if bytes_per_second >= 1024 * 1024 * 1024:
            return f"{bytes_per_second / (1024 * 1024 * 1024):.1f} GB/s"
        elif bytes_per_second >= 1024 * 1024:
            return f"{bytes_per_second / (1024 * 1024):.1f} MB/s"
        elif bytes_per_second >= 1024:
            return f"{bytes_per_second / 1024:.1f} KB/s"
        else:
            return f"{bytes_per_second:.0f} B/s"
    
    @staticmethod
    def format_eta(start_time: float, current_progress: float, total_progress: float = 100.0) -> str:
        """Calculate and format estimated time of arrival"""
        import time
        
        if current_progress <= 0:
            return "Unknown"
        
        elapsed = time.time() - start_time
        rate = current_progress / elapsed
        
        if rate <= 0:
            return "Unknown"
        
        remaining_progress = total_progress - current_progress
        eta_seconds = remaining_progress / rate
        
        return ProgressFormatter().format_time_remaining(eta_seconds)
    
    def show_spinner(self, message: str):
        """Show a spinner with message"""
        from rich.spinner import Spinner
        spinner = Spinner("dots", text=message)
        self.console.print(spinner, end="")