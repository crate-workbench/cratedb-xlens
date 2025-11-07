"""
SQL generation utilities for problematic translog operations

This module contains SQL generation classes for building comprehensive shard management
commands and replica-related SQL statements.
"""

from typing import List, Dict, Any, Union, Optional

from rich.console import Console

from ..base import TableInfo, PARTITION_NULL_VALUE

console = Console()


class ReplicaSQLBuilder:
    """Helper class for building replica-related SQL statements

    Provides consistent SQL generation for ALTER TABLE and monitoring queries,
    eliminating duplication and ensuring security best practices.
    """

    @staticmethod
    def validate_identifier(identifier: str) -> None:
        """Validate SQL identifier to prevent injection"""
        if not identifier:
            raise ValueError("Identifier cannot be empty")
        if '"' in identifier:
            raise ValueError(f"Identifier contains invalid character: {identifier}")

    @staticmethod
    def build_alter_replicas_sql(schema_name: str, table_name: str,
                                 partition_values: Optional[str],
                                 replica_count: int) -> str:
        """Build ALTER TABLE SQL for setting replica count safely

        Args:
            schema_name: Schema name
            table_name: Table name
            partition_values: Partition clause (e.g., "(date='2024-01-01')") or None
            replica_count: Number of replicas to set

        Returns:
            SQL string with properly quoted identifiers

        Raises:
            ValueError: If identifiers contain invalid characters
        """
        ReplicaSQLBuilder.validate_identifier(schema_name)
        ReplicaSQLBuilder.validate_identifier(table_name)

        sql = f'ALTER TABLE "{schema_name}"."{table_name}"'

        if partition_values and partition_values != PARTITION_NULL_VALUE:
            sql += f' PARTITION {partition_values}'

        sql += f' SET ("number_of_replicas" = {replica_count});'
        return sql

    @staticmethod
    def build_retention_lease_query(table_name: str, schema_name: str,
                                    partition_ident: Optional[str] = None) -> tuple[str, List[str]]:
        """Build parameterized query for checking retention leases

        Args:
            table_name: Table name
            schema_name: Schema name
            partition_ident: Partition identifier (for partitioned tables)

        Returns:
            Tuple of (sql_query, parameters_list)
        """
        if partition_ident:
            sql = """SELECT array_length(retention_leases['leases'], 1) as cnt_leases, id
FROM sys.shards
WHERE table_name = ?
  AND schema_name = ?
  AND partition_ident = ?
ORDER BY array_length(retention_leases['leases'], 1);"""
            params = [table_name, schema_name, partition_ident]
        else:
            sql = """SELECT array_length(retention_leases['leases'], 1) as cnt_leases, id
FROM sys.shards
WHERE table_name = ?
  AND schema_name = ?
ORDER BY array_length(retention_leases['leases'], 1);"""
            params = [table_name, schema_name]

        return sql, params

    @staticmethod
    def format_parameterized_query_for_display(sql: str, params: List[str]) -> str:
        """Format a parameterized query for display purposes

        Replaces ? placeholders with quoted parameter values for human readability.
        WARNING: Only use for display, never for execution!

        Args:
            sql: SQL query with ? placeholders
            params: List of parameter values

        Returns:
            Formatted SQL string with parameters substituted
        """
        result = sql
        for param in params:
            # Escape single quotes in the parameter
            escaped_param = str(param).replace("'", "''")
            result = result.replace("?", f"'{escaped_param}'", 1)
        return result


class ProblematicTranslogsSQLGenerator:
    """Generator for comprehensive shard management SQL commands"""

    def __init__(self, client, console):
        """Initialize the SQL generator

        Args:
            client: Database client for executing queries
            console: Rich console for output
        """
        self.client = client
        self.console = console

    def generate_comprehensive_commands(self, individual_shards: List[Dict[str, Any]],
                                       summary_rows: List[Dict[str, Any]],
                                       get_current_replica_count_fn) -> None:
        """Generate comprehensive shard management commands with full 6-step process, grouped by table/partition

        Args:
            individual_shards: List of individual problematic shards
            summary_rows: List of table summary data
            get_current_replica_count_fn: Function to get current replica count
        """
        self.console.print()
        self.console.print("[bold]Generated Comprehensive Shard Management Commands:[/bold]")
        self.console.print()

        # Convert to TableInfo objects and enrich with current replica counts
        valid_table_info = []
        for row in summary_rows:
            # Convert to TableInfo for type safety
            table_info = TableInfo.from_dict(row)

            # Look up current replica count
            current_replicas = get_current_replica_count_fn(
                table_info.schema_name,
                table_info.table_name,
                table_info.partition_ident,
                table_info.partition_values
            )

            # Skip tables with unknown or zero replicas
            if current_replicas == "unknown" or current_replicas == 0:
                continue

            # Update replica count
            table_info.current_replicas = current_replicas
            valid_table_info.append(table_info)

        # 1. Stop automatic shard rebalancing
        self.console.print("[bold cyan]1. Stop Automatic Shard Rebalancing:[/bold cyan]")
        rebalance_disable_cmd = 'SET GLOBAL PERSISTENT "cluster.routing.rebalance.enable"=\'none\';'
        self.console.print(rebalance_disable_cmd)
        self.console.print()

        # 2. Generate REROUTE CANCEL SHARD commands for individual shards
        self.console.print("[bold cyan]2. REROUTE CANCEL Commands:[/bold cyan]")
        self.console.print("[yellow]⚠️  Note: CANCEL SHARDS is deprecated on certain CrateDB cluster versions[/yellow]")
        reroute_commands = []
        for shard in individual_shards:
            schema_name = shard['schema_name']
            table_name = shard['table_name']
            partition_values = shard.get('partition_values')
            shard_id = shard['shard_id']
            node_name = shard['node_name']

            # Include partition clause if this is a partitioned table
            partition_clause = f' PARTITION {partition_values}' if partition_values else ''
            cmd = f'ALTER TABLE "{schema_name}"."{table_name}"{partition_clause} REROUTE CANCEL SHARD {shard_id} on \'{node_name}\' WITH (allow_primary=False);'
            reroute_commands.append(cmd)
            self.console.print(cmd)
        self.console.print()

        # Group remaining commands by table/partition for convenience
        for table_info in valid_table_info:
            # Use TableInfo methods for cleaner code
            table_display = table_info.get_display_name()

            self.console.print(f"[bold green]-- For {table_display}:[/bold green]")
            self.console.print()

            # 3. Set replicas to 0
            self.console.print("[dim]3. Set replicas to 0:[/dim]")
            cmd_set_zero = ReplicaSQLBuilder.build_alter_replicas_sql(
                table_info.schema_name,
                table_info.table_name,
                table_info.partition_values,
                0
            )
            self.console.print(cmd_set_zero)
            self.console.print()

            # 4. Retention lease monitoring query
            self.console.print("[dim]4. Monitor retention leases:[/dim]")
            # Build parameterized query then format for display
            retention_sql, retention_params = ReplicaSQLBuilder.build_retention_lease_query(
                table_info.table_name,
                table_info.schema_name,
                table_info.partition_ident if table_info.has_partition() else None
            )
            # Format for display (safe because it's not executed, just shown to user)
            retention_query = ReplicaSQLBuilder.format_parameterized_query_for_display(
                retention_sql,
                retention_params
            )
            self.console.print(retention_query)
            self.console.print()

            # 5. Restore replicas to original values
            self.console.print("[dim]5. Restore replicas to original value:[/dim]")
            cmd_restore = ReplicaSQLBuilder.build_alter_replicas_sql(
                table_info.schema_name,
                table_info.table_name,
                table_info.partition_values,
                table_info.current_replicas
            )
            self.console.print(cmd_restore)
            self.console.print()
            self.console.print("[dim]" + "─" * 80 + "[/dim]")  # Visual separator between tables
            self.console.print()

        # 6. Re-enable automatic shard rebalancing
        self.console.print("[bold cyan]6. Re-enable Automatic Shard Rebalancing:[/bold cyan]")
        rebalance_enable_cmd = 'SET GLOBAL PERSISTENT "cluster.routing.rebalance.enable"=\'all\';'
        self.console.print(rebalance_enable_cmd)
        self.console.print()

        # Summary
        self.console.print(f"[bold]Total Commands:[/bold]")
        self.console.print(f"  • 1 rebalancing disable command")
        self.console.print(f"  • {len(reroute_commands)} REROUTE CANCEL commands")
        self.console.print(f"  • {len(valid_table_info)} set replicas to 0 commands")
        self.console.print(f"  • {len(valid_table_info)} retention lease queries (for monitoring)")
        self.console.print(f"  • {len(valid_table_info)} restore replicas commands")
        self.console.print(f"  • 1 rebalancing enable command")
