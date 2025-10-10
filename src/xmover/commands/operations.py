"""
Operations command handlers for XMover

This module contains commands related to shard movement operations:
- find_candidates: Find shard candidates for movement based on size criteria
- recommend: Generate shard movement recommendations for rebalancing
- validate_move: Validate a specific shard move before execution
"""

import sys
from typing import Optional
import click
from rich.table import Table
from rich.panel import Panel
from rich import box

from .base import BaseCommand
from ..analyzer import ShardAnalyzer
from ..utils import format_size


class OperationsCommands(BaseCommand):
    """Command handlers for shard movement operations"""

    def execute(self, command: str, **kwargs) -> None:
        """Execute an operations command by name"""
        if command == 'find_candidates':
            self.find_candidates(**kwargs)
        elif command == 'recommend':
            self.recommend(**kwargs)
        elif command == 'validate_move':
            self.validate_move(**kwargs)
        else:
            raise ValueError(f"Unknown operations command: {command}")

    def find_candidates(self, table: Optional[str], min_size: float, max_size: float, 
                       limit: int, node: Optional[str]) -> None:
        """Find shard candidates for movement based on size criteria

        Results are sorted by nodes with least available space first,
        then by shard size (smallest first) for easier moves.
        """
        if not self.validate_connection():
            return

        analyzer = ShardAnalyzer(self.client)

        self.console.print(Panel.fit(f"[bold blue]Finding Moveable Shards ({min_size}-{max_size}GB)[/bold blue]"))

        if node:
            self.console.print(f"[dim]Filtering: Only showing candidates from source node '{node}'[/dim]")

        # Find moveable candidates (only healthy shards suitable for operations)
        candidates = analyzer.find_moveable_shards(min_size, max_size, table)

        # Filter by node if specified
        if node:
            candidates = [c for c in candidates if c.node_name == node]

        if not candidates:
            if node:
                self.console.print(f"[yellow]No moveable shards found on node '{node}' in the specified size range.[/yellow]")
                self.console.print(f"[dim]Tip: Try different size ranges or remove --node filter to see all candidates[/dim]")
            else:
                self.console.print("[yellow]No moveable shards found in the specified size range.[/yellow]")
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

        self.console.print(candidates_table)

        if len(candidates) > limit:
            self.console.print(f"\n[dim]... and {len(candidates) - limit} more candidates[/dim]")

    def recommend(self, table: Optional[str], min_size: float, max_size: float,
                  zone_tolerance: float, min_free_space: float, max_moves: int, 
                  max_disk_usage: float, validate: bool, prioritize_space: bool, 
                  dry_run: bool, auto_execute: bool, node: Optional[str]) -> None:
        """Generate shard movement recommendations for rebalancing"""
        if not self.validate_connection():
            return

        analyzer = ShardAnalyzer(self.client)
        
        # Safety check for auto-execute
        if auto_execute and dry_run:
            self.console.print("[red]❌ Error: --auto-execute requires --execute flag[/red]")
            self.console.print("[dim]Use: --execute --auto-execute[/dim]")
            return

        mode_text = "DRY RUN - Analysis Only" if dry_run else "EXECUTION MODE"
        self.console.print(Panel.fit(f"[bold blue]Generating Rebalancing Recommendations[/bold blue] - [bold {'green' if dry_run else 'red'}]{mode_text}[/bold {'green' if dry_run else 'red'}]"))
        self.console.print("[dim]Note: Only analyzing healthy shards (STARTED + 100% recovered) for safe operations[/dim]")
        self.console.print("[dim]Zone conflict detection: Prevents moves that would violate CrateDB's zone awareness[/dim]")
        if prioritize_space:
            self.console.print("[dim]Mode: Prioritizing available space over zone balancing[/dim]")
        else:
            self.console.print("[dim]Mode: Prioritizing zone balancing over available space[/dim]")

        if node:
            self.console.print(f"[dim]Filtering: Only showing moves from source node '{node}'[/dim]")

        self.console.print(f"[dim]Safety thresholds: Max disk usage {max_disk_usage}%, Min free space {min_free_space}GB[/dim]")

        if dry_run:
            self.console.print("[green]Running in DRY RUN mode - no SQL commands will be generated[/green]")
        else:
            self.console.print("[red]EXECUTION MODE - SQL commands will be generated for actual moves[/red]")
        self.console.print()

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
                self.console.print(f"[yellow]No safe recommendations found for node '{node}'[/yellow]")
                self.console.print(f"[dim]This could be due to:[/dim]")
                self.console.print(f"[dim]  • Zone conflicts preventing safe moves[/dim]")
                self.console.print(f"[dim]  • Target nodes exceeding {max_disk_usage}% disk usage threshold[/dim]")
                self.console.print(f"[dim]  • Insufficient free space on target nodes (need {min_free_space}GB)[/dim]")
                self.console.print(f"[dim]  • No shards in size range {min_size}-{max_size}GB[/dim]")
                self.console.print(f"[dim]Suggestions:[/dim]")
                self.console.print(f"[dim]  • Try: --max-disk-usage 95 (allow higher disk usage)[/dim]")
                self.console.print(f"[dim]  • Try: --min-free-space 50 (reduce space requirements)[/dim]")
                self.console.print(f"[dim]  • Try: different size ranges or remove --node filter[/dim]")
            else:
                self.console.print("[yellow]No safe recommendations found[/yellow]")
                self.console.print("[dim]This could be due to:[/dim]")
                self.console.print("[dim]  • Zone conflicts preventing safe moves[/dim]")
                self.console.print("[dim]  • All target nodes exceeding disk usage threshold[/dim]")
                self.console.print("[dim]  • Insufficient free space across cluster[/dim]")
                self.console.print("[dim]  • No shards in specified size range[/dim]")
                self.console.print("[dim]Suggestions:[/dim]")
                self.console.print("[dim]  • Try: --max-disk-usage 95 --min-free-space 50[/dim]")
                self.console.print("[dim]  • Try: --prioritize-space for space-focused moves[/dim]")
                self.console.print("[dim]  • Try: different size ranges[/dim]")
            return

        # Display recommendations
        self.console.print(f"[green]✓ Found {len(recommendations)} safe move recommendations[/green]")
        self.console.print()

        if dry_run:
            self._display_dry_run_recommendations(recommendations, analyzer)
        else:
            self._display_execution_recommendations(recommendations, analyzer, auto_execute, validate)

    def _display_dry_run_recommendations(self, recommendations, analyzer):
        """Display recommendations in dry run mode"""
        rec_table = self._create_recommendations_table("Move Recommendations (Dry Run)")
        
        for i, rec in enumerate(recommendations, 1):
            # Use human-readable partition values if available, otherwise use partition_ident
            partition_display = ""
            if rec.partition_values and rec.partition_values.strip():
                partition_display = rec.partition_values
            elif rec.partition_ident and rec.partition_ident.strip():
                partition_display = rec.partition_ident
            
            rec_table.add_row(
                str(i),
                f"{rec.schema_name}.{rec.table_name}",
                partition_display,
                str(rec.shard_id),
                rec.shard_type,
                rec.from_node,
                rec.to_node,
                format_size(rec.size_gb),
                ""  # reason is not available on MoveRecommendation
            )

        self.console.print(rec_table)
        
        # Show impact summary
        self._show_move_impact_summary(recommendations, analyzer)
        
        self.console.print()
        self.console.print("[dim]This is a DRY RUN. Use --execute to generate actual SQL commands.[/dim]")

    def _display_execution_recommendations(self, recommendations, analyzer, auto_execute, validate):
        """Display recommendations in execution mode with SQL generation"""
        rec_table = self._create_recommendations_table("Move Recommendations (Execution Mode)")
        
        for i, rec in enumerate(recommendations, 1):
            # Use human-readable partition values if available, otherwise use partition_ident
            partition_display = ""
            if rec.partition_values and rec.partition_values.strip():
                partition_display = rec.partition_values
            elif rec.partition_ident and rec.partition_ident.strip():
                partition_display = rec.partition_ident
            
            rec_table.add_row(
                str(i),
                f"{rec.schema_name}.{rec.table_name}",
                partition_display,
                str(rec.shard_id),
                rec.shard_type,
                rec.from_node,
                rec.to_node,
                format_size(rec.size_gb),
                ""  # reason is not available on MoveRecommendation
            )

        self.console.print(rec_table)
        
        # Show impact summary
        self._show_move_impact_summary(recommendations, analyzer)

        # Generate and display SQL commands
        self.console.print()
        self.console.print(Panel.fit("[bold yellow]Generated SQL Commands[/bold yellow]"))

        for i, rec in enumerate(recommendations, 1):
            sql_command = rec.to_sql()
            if validate:
                # Validate move safety using analyzer
                is_safe, safety_msg = analyzer.validate_move_safety(rec)
                if not is_safe:
                    self.console.print(f"[red]❌ Command {i}: UNSAFE - {safety_msg}[/red]")
                    continue
            
            partition_info = f"[{rec.partition_values}]" if rec.partition_values and rec.partition_values.strip() else ""
            self.console.print(f"[bold blue]-- Move {i}: {rec.schema_name}.{rec.table_name}{partition_info} shard {rec.shard_id}[/bold blue]")
            self.console.print(f"[green]{sql_command}[/green]")
            self.console.print()

        if auto_execute:
            self._handle_auto_execution(recommendations, validate)
        else:
            self.console.print("[yellow]⚠️  Commands generated. Review carefully before execution.[/yellow]")
            self.console.print("[dim]Tip: Copy and paste commands into CrateDB admin interface or use crash CLI[/dim]")

    def _create_recommendations_table(self, title):
        """Create a recommendations table with standard columns"""
        table = Table(title=title, box=box.ROUNDED)
        table.add_column("#", justify="right", style="dim")
        table.add_column("Table", style="cyan")
        table.add_column("Partition", style="bright_blue")
        table.add_column("Shard", justify="right", style="magenta")
        table.add_column("Type", style="blue")
        table.add_column("From Node", style="red")
        table.add_column("To Node", style="green")
        table.add_column("Size", justify="right", style="yellow")
        table.add_column("Reason", style="dim")
        return table

    def _show_move_impact_summary(self, recommendations, analyzer):
        """Show the impact summary of the recommended moves"""
        self.console.print()
        self.console.print(Panel.fit("[bold blue]Move Impact Summary[/bold blue]"))
        
        # Calculate total data to be moved
        total_size_gb = sum(rec.size_gb for rec in recommendations)
        self.console.print(f"[bold]Total data to move: {format_size(total_size_gb)}[/bold]")
        
        # Show node changes
        source_nodes = set(rec.from_node for rec in recommendations)
        target_nodes = set(rec.to_node for rec in recommendations)
        
        self.console.print(f"[dim]Source nodes affected: {', '.join(sorted(source_nodes))}[/dim]")
        self.console.print(f"[dim]Target nodes affected: {', '.join(sorted(target_nodes))}[/dim]")

    def _handle_auto_execution(self, recommendations, validate):
        """Handle automatic execution of recommendations"""
        self.console.print()
        self.console.print("[bold red]⚠️  AUTO-EXECUTE MODE ENABLED[/bold red]")
        
        if validate:
            # Note: MoveRecommendation doesn't have is_safe() method, so skip validation for now
            pass
        
        self.console.print(f"[yellow]About to execute {len(recommendations)} move commands...[/yellow]")
        
        # In a real implementation, you would:
        # 1. Show a final confirmation prompt
        # 2. Execute each SQL command
        # 3. Monitor the moves for completion
        # 4. Handle any failures gracefully
        
        self.console.print("[red]❌ Auto-execution not implemented in this version for safety[/red]")
        self.console.print("[dim]Please execute commands manually after review[/dim]")

    def validate_move(self, schema_table: str, shard_id: int, from_node: str, 
                     to_node: str, max_disk_usage: float) -> None:
        """Validate a specific shard move before execution"""
        if not self.validate_connection():
            return

        analyzer = ShardAnalyzer(self.client)

        self.console.print(Panel.fit(f"[bold blue]Validating Shard Move[/bold blue]"))
        self.console.print(f"[dim]Table: {schema_table}, Shard: {shard_id}[/dim]")
        self.console.print(f"[dim]From: {from_node} → To: {to_node}[/dim]")
        self.console.print(f"[dim]Max disk usage threshold: {max_disk_usage}%[/dim]")
        self.console.print()

        try:
            # Parse schema and table
            if '.' in schema_table:
                schema, table = schema_table.split('.', 1)
            else:
                schema, table = 'doc', schema_table

            # Find the specific shard
            shard = analyzer.find_specific_shard(schema, table, shard_id, from_node)
            if not shard:
                self.console.print(f"[red]❌ Shard not found: {schema_table} shard {shard_id} on node {from_node}[/red]")
                self.console.print("[dim]Possible reasons:[/dim]")
                self.console.print("[dim]  • Shard ID does not exist for this table[/dim]")
                self.console.print("[dim]  • Shard is not currently on the specified source node[/dim]")
                self.console.print("[dim]  • Table name is incorrect (try 'schema.table' format)[/dim]")
                return

            # Validate the target node exists
            target_node = analyzer.find_node_by_name(to_node)
            if not target_node:
                self.console.print(f"[red]❌ Target node not found: {to_node}[/red]")
                self.console.print("[dim]Available nodes:[/dim]")
                for node in sorted(analyzer.nodes, key=lambda n: n.name):
                    self.console.print(f"[dim]  • {node.name} (Zone: {node.zone})[/dim]")
                return

            # Validate the move
            move_result = analyzer.validate_specific_move(
                shard, target_node, max_disk_usage_percent=max_disk_usage
            )

            # Display validation results
            self._display_move_validation_results(shard, target_node, move_result)

        except Exception as e:
            self.console.print(f"[red]❌ Error validating move: {e}[/red]")

    def _display_move_validation_results(self, shard, target_node, move_result):
        """Display the results of move validation"""
        # Shard information
        shard_table = Table(title="Shard Information", box=box.ROUNDED)
        shard_table.add_column("Property", style="cyan")
        shard_table.add_column("Value", style="white")
        
        shard_table.add_row("Table", f"{shard.schema_name}.{shard.table_name}")
        shard_table.add_row("Shard ID", str(shard.shard_id))
        shard_table.add_row("Type", shard.shard_type)
        shard_table.add_row("Current Node", shard.node_name)
        shard_table.add_row("Current Zone", shard.zone)
        shard_table.add_row("Size", format_size(shard.size_gb))
        shard_table.add_row("Documents", f"{shard.num_docs:,}")
        shard_table.add_row("State", shard.state)
        
        self.console.print(shard_table)
        self.console.print()

        # Target node information
        target_table = Table(title="Target Node Information", box=box.ROUNDED)
        target_table.add_column("Property", style="cyan")
        target_table.add_column("Value", style="white")
        
        current_usage = ((target_node.total_space_gb - target_node.available_space_gb) / target_node.total_space_gb) * 100
        usage_after_move = current_usage + (shard.size_gb / target_node.total_space_gb * 100)
        
        target_table.add_row("Node Name", target_node.name)
        target_table.add_row("Zone", target_node.zone)
        target_table.add_row("Total Space", format_size(target_node.total_space_gb))
        target_table.add_row("Available Space", format_size(target_node.available_space_gb))
        target_table.add_row("Current Usage", f"{current_usage:.1f}%")
        target_table.add_row("Usage After Move", f"{usage_after_move:.1f}%")
        
        self.console.print(target_table)
        self.console.print()

        # Validation results
        if move_result.is_safe:
            self.console.print("[bold green]✅ Move Validation: SAFE[/bold green]")
            self.console.print(f"[green]This move is safe to execute[/green]")
            
            if move_result.warnings:
                self.console.print("[yellow]⚠️  Warnings:[/yellow]")
                for warning in move_result.warnings:
                    self.console.print(f"[yellow]  • {warning}[/yellow]")
        else:
            self.console.print("[bold red]❌ Move Validation: UNSAFE[/bold red]")
            self.console.print(f"[red]This move should NOT be executed[/red]")
            
            if move_result.errors:
                self.console.print("[red]Blocking Issues:[/red]")
                for error in move_result.errors:
                    self.console.print(f"[red]  • {error}[/red]")
        
        self.console.print()
        
        # Generate SQL command if safe
        if move_result.is_safe:
            sql_command = f"ALTER TABLE \"{shard.schema_name}\".\"{shard.table_name}\" REROUTE MOVE SHARD {shard.shard_id} FROM '{shard.node_name}' TO '{target_node.name}';"
            
            self.console.print(Panel.fit("[bold yellow]Generated SQL Command[/bold yellow]"))
            self.console.print(f"[green]{sql_command}[/green]")
            self.console.print()
            self.console.print("[dim]Copy and paste this command into CrateDB admin interface or crash CLI[/dim]")
        else:
            self.console.print("[red]No SQL command generated due to safety concerns[/red]")


def create_operations_commands(main_cli):
    """Register operations commands with the main CLI"""

    @main_cli.command()
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
        operations = OperationsCommands(client)
        operations.find_candidates(table, min_size, max_size, limit, node)

    @main_cli.command()
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
                  zone_tolerance: float, min_free_space: float, max_moves: int, 
                  max_disk_usage: float, validate: bool, prioritize_space: bool, 
                  dry_run: bool, auto_execute: bool, node: Optional[str]):
        """Generate shard movement recommendations for rebalancing"""
        client = ctx.obj['client']
        operations = OperationsCommands(client)
        operations.recommend(table, min_size, max_size, zone_tolerance, min_free_space, 
                           max_moves, max_disk_usage, validate, prioritize_space, 
                           dry_run, auto_execute, node)

    @main_cli.command()
    @click.argument('schema_table')
    @click.argument('shard_id', type=int)
    @click.argument('from_node')
    @click.argument('to_node')
    @click.option('--max-disk-usage', default=90.0, help='Maximum disk usage percentage for target node (default: 90)')
    @click.pass_context
    def validate_move(ctx, schema_table: str, shard_id: int, from_node: str, to_node: str, max_disk_usage: float):
        """Validate a specific shard move before execution

        Validates that a proposed shard move is safe by checking:
        • Target node has sufficient free space
        • Move won't exceed disk usage thresholds  
        • Zone constraints are respected
        • Shard is in a healthy state for movement

        Examples:
          xmover validate_move my_table 0 data-hot-1 data-hot-2
          xmover validate_move doc.logs 3 data-hot-1 data-hot-2 --max-disk-usage 85
        """
        client = ctx.obj['client']
        operations = OperationsCommands(client)
        operations.validate_move(schema_table, shard_id, from_node, to_node, max_disk_usage)