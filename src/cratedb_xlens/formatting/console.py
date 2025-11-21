"""
Console formatting utilities for XMover CLI
"""

from rich.console import Console
from rich.panel import Panel
from rich.text import Text
from typing import Optional


class ConsoleFormatter:
    """Centralized console formatting utilities"""
    
    def __init__(self, console: Optional[Console] = None):
        self.console = console or Console()
    
    @staticmethod
    def format_size(size_gb: float) -> str:
        """Format size in GB with appropriate precision"""
        if size_gb >= 1000:
            return f"{size_gb/1000:.1f}TB"
        elif size_gb >= 1:
            return f"{size_gb:.1f}GB"
        else:
            return f"{size_gb*1000:.0f}MB"

    @staticmethod
    def format_percentage(value: float) -> str:
        """Format percentage with color coding"""
        color = "green"
        if value > 80:
            color = "red"
        elif value > 70:
            color = "yellow"
        return f"[{color}]{value:.1f}%[/{color}]"

    @staticmethod
    def format_table_display_with_partition(table_name: str, schema_name: str = "doc") -> str:
        """Format table name with schema and partition info"""
        if '.' in table_name:
            return table_name
        
        full_name = f"{schema_name}.{table_name}"
        
        # Check if this looks like a partitioned table
        if '_' in table_name and any(char.isdigit() for char in table_name.split('_')[-1]):
            base_table = '_'.join(table_name.split('_')[:-1])
            partition_suffix = table_name.split('_')[-1]
            return f"{schema_name}.{base_table} (partition: {partition_suffix})"
        
        return full_name

    def print_error(self, message: str, details: Optional[str] = None):
        """Print formatted error message"""
        self.console.print(f"[red]❌ {message}[/red]")
        if details:
            self.console.print(f"[dim]{details}[/dim]")

    def print_success(self, message: str):
        """Print formatted success message"""
        self.console.print(f"[green]✅ {message}[/green]")

    def print_warning(self, message: str):
        """Print formatted warning message"""
        self.console.print(f"[yellow]⚠️  {message}[/yellow]")

    def print_info(self, message: str):
        """Print formatted info message"""
        self.console.print(f"[blue]ℹ️  {message}[/blue]")

    def print_panel(self, content: str, title: str, style: str = "blue"):
        """Print content in a panel"""
        panel = Panel(content, title=title, style=style)
        self.console.print(panel)

    def print_header(self, text: str):
        """Print a formatted header"""
        self.console.print(f"\n[bold blue]{text}[/bold blue]")
        self.console.print("=" * len(text))

    def print_subheader(self, text: str):
        """Print a formatted subheader"""
        self.console.print(f"\n[bold]{text}[/bold]")