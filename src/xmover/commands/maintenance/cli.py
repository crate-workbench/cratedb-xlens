"""
CLI command registration for maintenance commands

This module registers all maintenance-related commands with the Click CLI framework.
"""

from typing import Optional
import click


def create_maintenance_commands(main_cli):
    """Register maintenance commands with the main CLI

    This function creates and registers three maintenance commands:
    1. shard-distribution: Analyze shard distribution anomalies
    2. problematic-translogs: Find and fix problematic translog sizes
    3. check-maintenance: Analyze node decommissioning feasibility
    """

    @main_cli.command()
    @click.option('--top-tables', default=10, help='Number of largest tables to analyze (default: 10)')
    @click.option('--table', help='Analyze specific table only (e.g., "my_table" or "schema.table")')
    @click.pass_context
    def shard_distribution(ctx, top_tables: int, table: Optional[str]):
        """Analyze shard distribution anomalies across cluster nodes

        This command analyzes the largest tables in your cluster to detect:
        • Uneven shard count distribution between nodes
        • Storage imbalances across nodes
        • Missing node coverage for tables
        • Document count anomalies within tables

        The coefficient of variation (CV%) indicates distribution uniformity:
        • CV < 20%: Well distributed
        • CV 20-40%: Moderately uneven
        • CV > 40%: Highly uneven, consider rebalancing

        Examples:
            xmover shard-distribution                    # Analyze top 10 tables
            xmover shard-distribution --top-tables 25   # Analyze top 25 tables
            xmover shard-distribution --table my_table   # Analyze specific table
        """
        from .shard_distribution import ShardDistributionCommand

        client = ctx.obj['client']
        command = ShardDistributionCommand(client)
        command.execute(top_tables, table)

    @main_cli.command()
    @click.option('--sizeMB', default=512, help='Minimum translog uncommitted size in MB (default: 512)')
    @click.option('--execute', is_flag=True, help='Generate SQL commands for display (does not execute against database)')
    @click.option('--autoexec', is_flag=True, help='Automatically execute replica reset operations')
    @click.option('--dry-run', is_flag=True, help='Simulate operations without actual database changes')
    @click.option('--percentage', default=200, help='Only process tables exceeding this percentage of threshold (default: 200)')
    @click.option('--max-wait', default=720, help='Maximum seconds to wait for retention leases (default: 720)')
    @click.option('--log-format', type=click.Choice(['console', 'json']), default='console', help='Logging format for container environments')
    @click.option('--debug', is_flag=True, help='Enable debug mode: log node names and SQL queries for troubleshooting')
    @click.pass_context
    def problematic_translogs(ctx, sizemb: int, execute: bool, autoexec: bool, dry_run: bool,
                             percentage: int, max_wait: int, log_format: str, debug: bool):
        """Find tables with problematic translog sizes and optionally execute automatic replica reset

        This command can operate in three modes:

        1. ANALYSIS MODE (default): Shows problematic shards only
        2. COMMAND GENERATION MODE (--execute): Generates SQL commands for manual execution
        3. AUTOEXEC MODE (--autoexec): Automatically executes replica reset operations

        AUTOEXEC MODE performs these operations for each problematic table:
        • Set number_of_replicas to 0
        • Monitor retention leases until cleared (with incremental backoff)
        • Restore original replica count

        Use --dry-run with --autoexec to simulate operations without database changes.
        Use --log-format json for structured logging in container environments.

        Examples:
            xmover problematic-translogs                                    # Analysis only
            xmover problematic-translogs --execute                         # Generate SQL commands
            xmover problematic-translogs --autoexec                        # Execute operations
            xmover problematic-translogs --autoexec --dry-run              # Simulate execution
            xmover problematic-translogs --autoexec --percentage 150       # Process tables >150% of threshold
            xmover problematic-translogs --autoexec --log-format json      # Container-friendly logging
        """
        from .problematic_translogs import ProblematicTranslogsCommand

        # Validation
        if autoexec and execute:
            click.echo("Error: --autoexec and --execute flags are mutually exclusive", err=True)
            ctx.exit(1)

        if dry_run and not autoexec:
            click.echo("Error: --dry-run can only be used with --autoexec", err=True)
            ctx.exit(1)

        client = ctx.obj['client']
        command = ProblematicTranslogsCommand(client)
        command.execute(sizemb, execute, autoexec, dry_run, percentage, max_wait, log_format, debug)

    @main_cli.command()
    @click.option('--node', required=True, help='Target node to analyze for decommissioning')
    @click.option('--min-availability',
                  type=click.Choice(['full', 'primaries'], case_sensitive=False),
                  required=True,
                  help='Minimum availability level: "full" (move all shards) or "primaries" (move only primaries without replicas)')
    @click.option('--short', is_flag=True,
                  help='Display only essential information: shard count, data size, target nodes, and ETA')
    @click.pass_context
    def check_maintenance(ctx, node: str, min_availability: str, short: bool):
        """Check whether a node could be decommissioned and analyze shard movement requirements

        This command analyzes if a node can be safely decommissioned by checking:
        • Available capacity on other nodes (considering disk watermarks)
        • Shard types and replica availability
        • Estimated recovery time based on cluster settings

        Use --short for a brief summary with only essential information:
        • Amount of shards to move
        • Amount of data to move
        • Possible target nodes
        • ETA for moving data (including recovery rate)

        Minimum availability levels:
        • "full": All shards need to be moved away from the node
        • "primaries": Only primaries without replicas need data movement.
          Primary shards with replicas can be quickly converted to replicas.

        The analysis considers:
        • Low watermark thresholds for target node capacity
        • Max number of shards per node limits
        • Availability zone constraints (capacity must be in same AZ)
        • Recovery bandwidth settings for time estimation

        Reports:
        • Primary shards without replicas (slow data movement required)
        • Primary shards with replicas (fast primary→replica conversion)
        • Replica shards (no action needed for "primaries" type)
        • Estimated time based on recovery.max_bytes_per_sec and routing.node_concurrent_recoveries

        Examples:
            xmover check-maintenance --node data-hot-4 --min-availability full        # Check full decommission
            xmover check-maintenance --node data-hot-4 --min-availability primaries   # Check primaries maintenance
            xmover check-maintenance --node data-hot-4 --min-availability full --short # Brief summary only
        """
        from .node_maintenance import NodeMaintenanceCommand

        client = ctx.obj['client']
        command = NodeMaintenanceCommand(client)
        command.execute(node, min_availability.lower(), short)
