"""
Base Command class for XMover CLI commands

This module provides the BaseCommand class that encapsulates common functionality
shared across all command handlers, including error handling, formatting, and
client management.
"""

from abc import ABC, abstractmethod
from typing import Optional, Any, Dict, List
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ..database import CrateDBClient
from ..formatting import ConsoleFormatter, RichTableFormatter, ProgressFormatter


class BaseCommand(ABC):
    """
    Abstract base class for all XMover commands.
    
    Provides common functionality including:
    - Error handling and user-friendly error messages
    - Consistent output formatting
    - Database client management
    - Common utility methods
    """
    
    def __init__(self, client: CrateDBClient):
        """
        Initialize the base command.
        
        Args:
            client: CrateDB client instance for database operations
        """
        self.client = client
        self.console = Console()
        self.formatter = ConsoleFormatter(self.console)
        self.table_formatter = RichTableFormatter()
        self.progress_formatter = ProgressFormatter(self.console)
    
    @abstractmethod
    def execute(self, **kwargs) -> None:
        """
        Execute the command with the given parameters.
        
        This method must be implemented by all concrete command classes.
        
        Args:
            **kwargs: Command-specific parameters
        """
        pass
    
    def handle_error(self, error: Exception, context: str) -> None:
        """
        Handle errors with consistent formatting and logging.
        
        Args:
            error: The exception that occurred
            context: Context description for the error
        """
        error_msg = str(error)
        
        # Format database connection errors
        if "connection" in error_msg.lower():
            self.console.print(f"[red]❌ Database Connection Error[/red]")
            self.console.print(f"[dim]{context}[/dim]")
            self.console.print(f"Error: {error_msg}")
            self.console.print("\n[yellow]💡 Troubleshooting tips:[/yellow]")
            self.console.print("• Check your .env file configuration")
            self.console.print("• Verify CrateDB cluster is running")
            self.console.print("• Test connection with: xmover test-connection")
        
        # Format query errors
        elif "sql" in error_msg.lower() or "query" in error_msg.lower():
            self.console.print(f"[red]❌ Database Query Error[/red]")
            self.console.print(f"[dim]{context}[/dim]")
            self.console.print(f"Error: {error_msg}")
        
        # Generic error handling
        else:
            self.console.print(f"[red]❌ Error in {context}[/red]")
            self.console.print(f"Error: {error_msg}")
    
    def print_header(self, title: str, subtitle: Optional[str] = None) -> None:
        """
        Print a consistent header for command output.
        
        Args:
            title: Main title for the command
            subtitle: Optional subtitle with additional context
        """
        self.console.print(Panel.fit(f"[bold blue]{title}[/bold blue]"))
        if subtitle:
            self.console.print(f"[dim]{subtitle}[/dim]")
        self.console.print()
    
    def validate_connection(self) -> bool:
        """
        Validate database connection before executing command.
        
        Returns:
            True if connection is valid, False otherwise
        """
        try:
            if not self.client.test_connection():
                self.console.print("[red]❌ Failed to connect to CrateDB cluster[/red]")
                return False
            return True
        except Exception as e:
            self.handle_error(e, "testing database connection")
            return False
    
    def format_output(self, data: Any, format_type: str = 'table', **kwargs) -> None:
        """
        Format and display output data.
        
        Args:
            data: Data to format and display
            format_type: Type of formatting ('table', 'json', 'text')
            **kwargs: Additional formatting options
        """
        if format_type == 'table' and isinstance(data, list):
            if data:
                table = self._create_generic_table(data, **kwargs)
                self.console.print(table)
            else:
                self.console.print("[yellow]No data to display[/yellow]")
        
        elif format_type == 'json':
            import json
            self.console.print_json(json.dumps(data, indent=2, default=str))
        
        else:
            self.console.print(str(data))
    
    def _create_generic_table(self, data: List[Dict], **kwargs) -> Table:
        """
        Create a generic Rich table from list of dictionaries.
        
        Args:
            data: List of dictionaries to display
            **kwargs: Table formatting options
            
        Returns:
            Rich Table object
        """
        if not data:
            return Table()
        
        # Get column headers from first row
        headers = list(data[0].keys())
        
        # Create table with formatting options
        table = Table(
            title=kwargs.get('title'),
            show_header=kwargs.get('show_header', True),
            header_style=kwargs.get('header_style', 'bold blue'),
        )
        
        # Add columns
        for header in headers:
            table.add_column(header.replace('_', ' ').title())
        
        # Add rows
        for row in data:
            table.add_row(*[str(row.get(header, '')) for header in headers])
        
        return table
    
    def confirm_action(self, message: str, default: bool = False) -> bool:
        """
        Prompt user for confirmation of an action.
        
        Args:
            message: Message to display to user
            default: Default value if user just presses enter
            
        Returns:
            True if user confirms, False otherwise
        """
        suffix = " [Y/n]" if default else " [y/N]"
        response = input(f"{message}{suffix}: ").strip().lower()
        
        if not response:
            return default
        
        return response in ('y', 'yes', 'true', '1')
    
    def print_summary(self, title: str, stats: Dict[str, Any]) -> None:
        """
        Print a summary panel with key statistics.
        
        Args:
            title: Title for the summary panel
            stats: Dictionary of statistics to display
        """
        lines = []
        for key, value in stats.items():
            formatted_key = key.replace('_', ' ').title()
            lines.append(f"[bold]{formatted_key}:[/bold] {value}")
        
        content = "\n".join(lines)
        panel = Panel(content, title=title, border_style="blue")
        self.console.print(panel)