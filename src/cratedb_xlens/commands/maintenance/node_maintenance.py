"""
Node Maintenance Command for XMover

This module provides functionality for analyzing node decommissioning feasibility
in CrateDB clusters. It helps operators understand the requirements and impacts
of removing a node from the cluster by analyzing:

- Shard distribution and movement requirements
- Available capacity in target nodes (same availability zone)
- Estimated recovery time based on cluster settings
- Capacity constraints (disk space and max shards per node)
- Recommendations for safe node maintenance

The analysis supports two maintenance strategies:
1. Full maintenance: Move all shards (primaries and replicas) off the node
2. Primaries maintenance: Move only primary shards, allowing faster operations
   by converting primaries with replicas to replica status (fast) and only
   moving primaries without replicas (slow)

Key features:
- Zone-aware analysis (data can only move within same availability zone)
- Watermark-aware capacity calculations
- Shard count limit validation
- Recovery time estimation based on cluster recovery settings
- Detailed breakdowns and recommendations

Usage:
    xmover check-maintenance <node> --min-availability <full|primaries>
"""

from typing import Optional, Dict, Any, List
from rich.table import Table
from rich.panel import Panel
from rich import box

from ..base import BaseCommand


class NodeMaintenanceCommand(BaseCommand):
    """
    Command for analyzing node decommissioning feasibility.

    Provides comprehensive analysis of what's required to safely decommission
    a node from the cluster, including capacity checks, time estimates, and
    actionable recommendations.
    """

    def execute(self, node: str, min_availability: str, short: bool = False):
        """Check whether a node could be decommissioned and analyze shard movement requirements

        Args:
            node: Target node to analyze for decommissioning
            min_availability: Minimum availability level - 'full' (move all shards) or 'primaries' (move only primaries without replicas)
            short: Display only essential information without detailed tables and recommendations
        """
        if not self.validate_connection():
            return

        # Get cluster name for display
        cluster_name = self.client.get_cluster_name()

        if not short:
            cluster_display = cluster_name or "Unknown"
            self.print_header(f"Pre-Flight Check {cluster_display}: {node}", f"Min-availability: {min_availability.title()}")

        try:
            # Get cluster recovery settings
            recovery_settings = self._get_cluster_recovery_settings()

            # Get node information and validate node exists
            nodes_info = self.client.get_nodes_info()
            target_node = None
            for n in nodes_info:
                if n.name == node:
                    target_node = n
                    break

            if not target_node:
                self.console.print(f"[red]❌ Node '{node}' not found in cluster[/red]")
                available_nodes = [n.name for n in nodes_info]
                self.console.print(f"Available nodes: {', '.join(available_nodes)}")
                return

            # Get all shards on the target node
            target_shards = self._get_node_shards(node)
            if not target_shards:
                self.console.print(f"[green]✅ Node '{node}' has no shards - safe to decommission[/green]")
                return

            # Analyze shards based on min-availability level
            if min_availability == "full":
                analysis = self._analyze_full_maintenance(target_shards, nodes_info, target_node)
            else:  # primaries
                analysis = self._analyze_primaries_maintenance(target_shards, nodes_info, target_node)

            # Display results
            if short:
                self._display_short_maintenance_analysis(analysis, recovery_settings, cluster_name)
            else:
                self._display_maintenance_analysis(analysis, recovery_settings, target_node)

        except Exception as e:
            self.handle_error(e, "analyzing node maintenance requirements")

    def _get_cluster_recovery_settings(self) -> dict:
        """Get cluster recovery settings and max shards per node from sys.cluster"""
        try:
            # Query recovery settings and max shards per node
            recovery_query = """
            SELECT
                settings['indices']['recovery']['max_bytes_per_sec'] as max_bytes_per_sec,
                settings['cluster']['routing']['allocation']['node_concurrent_recoveries'] as node_concurrent_recoveries,
                settings['cluster']['max_shards_per_node'] as max_shards_per_node
            FROM sys.cluster
            """

            result = self.client.execute_query(recovery_query)
            if result.get('rows'):
                row = result['rows'][0]
                max_bytes_per_sec = row[0] or "20mb"  # CrateDB default
                node_concurrent_recoveries = row[1] or 2  # CrateDB default
                max_shards_per_node = row[2] or 1000  # CrateDB default

                # Parse max_bytes_per_sec (could be "20mb", "100mb", etc.)
                if isinstance(max_bytes_per_sec, str):
                    if max_bytes_per_sec.lower().endswith('mb'):
                        bytes_per_sec = int(max_bytes_per_sec[:-2]) * 1024 * 1024
                    elif max_bytes_per_sec.lower().endswith('gb'):
                        bytes_per_sec = int(max_bytes_per_sec[:-2]) * 1024 * 1024 * 1024
                    else:
                        bytes_per_sec = int(max_bytes_per_sec)
                else:
                    bytes_per_sec = int(max_bytes_per_sec)

                return {
                    'max_bytes_per_sec': bytes_per_sec,
                    'node_concurrent_recoveries': int(node_concurrent_recoveries),
                    'max_shards_per_node': int(max_shards_per_node)
                }
        except Exception:
            pass

        # Return defaults if query fails
        return {
            'max_bytes_per_sec': 20 * 1024 * 1024,  # 20MB default
            'node_concurrent_recoveries': 2,  # Default
            'max_shards_per_node': 1000  # CrateDB default
        }

    def _get_node_shards(self, node_name: str) -> list:
        """Get all shards on a specific node with retention lease information"""
        query = """
        SELECT
            s.schema_name,
            s.table_name,
            s.partition_ident,
            s.id as shard_id,
            s."primary" as is_primary,
            s.size / 1024.0^3 as size_gb,
            s.retention_leases,
            n.attributes['zone'] as zone,
            s.state,
            s.routing_state
        FROM sys.shards s
        JOIN sys.nodes n ON s.node['id'] = n.id
        WHERE COALESCE(s.node['name'], 'unknown-' || COALESCE(s.node['id'], 'corrupted')) = ?
            AND s.routing_state = 'STARTED'
        ORDER BY s.size DESC
        """

        result = self.client.execute_query(query, [node_name])
        shards = []

        for row in result.get('rows', []):
            schema_name, table_name, partition_ident, shard_id, is_primary, size_gb, retention_leases, zone, state, routing_state = row

            # Count replicas from retention_leases
            replica_count = 0
            if retention_leases and isinstance(retention_leases, dict):
                leases = retention_leases.get('leases', [])
                replica_count = len(leases) if leases else 0

            shards.append({
                'schema_name': schema_name,
                'table_name': table_name,
                'partition_ident': partition_ident,
                'shard_id': shard_id,
                'is_primary': is_primary,
                'size_gb': size_gb,
                'replica_count': replica_count,
                'has_replicas': replica_count > 1,  # More than 1 lease indicates replicas
                'zone': zone
            })

        return shards

    def _get_node_shard_count(self, node_name: str) -> int:
        """Get current shard count for a specific node"""
        query = """
        SELECT COUNT(*) as shard_count
        FROM sys.shards s
        WHERE COALESCE(s.node['name'], 'unknown-' || COALESCE(s.node['id'], 'corrupted')) = ?
            AND s.routing_state = 'STARTED'
        """

        try:
            result = self.client.execute_query(query, [node_name])
            if result.get('rows'):
                count = result['rows'][0][0]
                return int(count)  # Ensure it's always returned as int
            return 0
        except Exception:
            return 0

    def _analyze_full_maintenance(self, target_shards: list, nodes_info: list, target_node) -> dict:
        """Analyze requirements for full node maintenance (move all shards)"""
        from ...utils import calculate_watermark_remaining_space

        # Get cluster watermark config and recovery settings (includes max_shards_per_node)
        watermark_config = self.client.get_cluster_watermark_config()
        recovery_settings = self._get_cluster_recovery_settings()
        max_shards_per_node = recovery_settings['max_shards_per_node']

        # Calculate target nodes capacity (same AZ)
        target_zone = target_node.zone
        candidate_nodes = []

        for node in nodes_info:
            if node.name != target_node.name and node.zone == target_zone and not node.name.startswith('master-'):
                watermark_info = calculate_watermark_remaining_space(node.fs_total, node.fs_used, watermark_config)

                # Get current shard count on this node
                current_shards = self._get_node_shard_count(node.name)
                remaining_shard_capacity = int(max_shards_per_node) - current_shards

                candidate_nodes.append({
                    'name': node.name,
                    'zone': node.zone,
                    'remaining_capacity_gb': watermark_info['remaining_to_low_gb'],
                    'disk_usage_percent': node.disk_usage_percent,
                    'available_below_watermark_gb': watermark_info['remaining_to_low_gb'],
                    'current_shards': current_shards,
                    'remaining_shard_capacity': remaining_shard_capacity,
                    'max_shards_per_node': int(max_shards_per_node)
                })

        # Sort by available capacity
        candidate_nodes.sort(key=lambda x: x['remaining_capacity_gb'], reverse=True)

        # Categorize shards
        primary_shards = [s for s in target_shards if s['is_primary']]
        replica_shards = [s for s in target_shards if not s['is_primary']]

        primary_without_replicas = [s for s in primary_shards if not s['has_replicas']]
        primary_with_replicas = [s for s in primary_shards if s['has_replicas']]

        # Calculate totals
        total_data_to_move = sum(s['size_gb'] for s in target_shards)
        total_available_capacity = sum(node['remaining_capacity_gb'] for node in candidate_nodes)

        # Check if there's sufficient shard capacity across all candidate nodes
        total_shard_capacity = sum(node['remaining_shard_capacity'] for node in candidate_nodes)
        shards_sufficient = total_shard_capacity >= len(target_shards)

        return {
            'min_availability': 'full',
            'target_node': target_node.name,
            'target_zone': target_zone,
            'candidate_nodes': candidate_nodes,
            'total_shards': len(target_shards),
            'primary_shards': len(primary_shards),
            'replica_shards': len(replica_shards),
            'primary_without_replicas': primary_without_replicas,
            'primary_with_replicas': primary_with_replicas,
            'total_data_to_move_gb': total_data_to_move,
            'total_available_capacity_gb': total_available_capacity,
            'capacity_sufficient': total_available_capacity >= total_data_to_move and shards_sufficient,
            'all_shards': target_shards,
            'total_shard_capacity': total_shard_capacity,
            'shards_sufficient': shards_sufficient
        }

    def _analyze_primaries_maintenance(self, target_shards: list, nodes_info: list, target_node) -> dict:
        """Analyze requirements for primaries-only maintenance"""
        from ...utils import calculate_watermark_remaining_space

        # Get cluster watermark config and recovery settings (includes max_shards_per_node)
        watermark_config = self.client.get_cluster_watermark_config()
        recovery_settings = self._get_cluster_recovery_settings()
        max_shards_per_node = recovery_settings['max_shards_per_node']

        # Calculate target nodes capacity (same AZ)
        target_zone = target_node.zone
        candidate_nodes = []

        for node in nodes_info:
            if node.name != target_node.name and node.zone == target_zone and not node.name.startswith('master-'):
                watermark_info = calculate_watermark_remaining_space(node.fs_total, node.fs_used, watermark_config)

                # Get current shard count on this node
                current_shards = self._get_node_shard_count(node.name)
                remaining_shard_capacity = int(max_shards_per_node) - current_shards

                candidate_nodes.append({
                    'name': node.name,
                    'zone': node.zone,
                    'remaining_capacity_gb': watermark_info['remaining_to_low_gb'],
                    'disk_usage_percent': node.disk_usage_percent,
                    'available_below_watermark_gb': watermark_info['remaining_to_low_gb'],
                    'current_shards': current_shards,
                    'remaining_shard_capacity': remaining_shard_capacity,
                    'max_shards_per_node': int(max_shards_per_node)
                })

        # Sort by available capacity
        candidate_nodes.sort(key=lambda x: x['remaining_capacity_gb'], reverse=True)

        # Categorize primary shards only
        primary_shards = [s for s in target_shards if s['is_primary']]
        replica_shards = [s for s in target_shards if not s['is_primary']]

        primary_without_replicas = [s for s in primary_shards if not s['has_replicas']]
        primary_with_replicas = [s for s in primary_shards if s['has_replicas']]

        # For primaries maintenance, only primaries without replicas need to be moved
        # Primaries with replicas can be demoted (fast operation)
        data_to_move_gb = sum(s['size_gb'] for s in primary_without_replicas)
        total_available_capacity = sum(node['remaining_capacity_gb'] for node in candidate_nodes)

        # Check if there's sufficient shard capacity for primaries that need to be moved
        total_shard_capacity = sum(node['remaining_shard_capacity'] for node in candidate_nodes)
        shards_sufficient = total_shard_capacity >= len(primary_without_replicas)

        return {
            'min_availability': 'primaries',
            'target_node': target_node.name,
            'target_zone': target_zone,
            'candidate_nodes': candidate_nodes,
            'total_shards': len(target_shards),
            'primary_shards': len(primary_shards),
            'replica_shards': len(replica_shards),
            'primary_without_replicas': primary_without_replicas,
            'primary_with_replicas': primary_with_replicas,
            'data_to_move_gb': data_to_move_gb,  # Only primaries without replicas
            'total_available_capacity_gb': total_available_capacity,
            'capacity_sufficient': total_available_capacity >= data_to_move_gb and shards_sufficient,
            'fast_operations': len(primary_with_replicas),  # Primary->replica conversions
            'slow_operations': len(primary_without_replicas),  # Actual data moves
            'all_shards': target_shards,
            'total_shard_capacity': total_shard_capacity,
            'shards_sufficient': shards_sufficient
        }

    def _display_maintenance_analysis(self, analysis: dict, recovery_settings: dict, target_node):
        """Display the maintenance analysis results"""
        from ...utils import format_size

        # Summary panel
        summary_lines = [
            f"[bold]Target Node:[/bold] {analysis['target_node']} (Zone: {analysis['target_zone']})",
            f"[bold]Min-availability:[/bold] {analysis['min_availability'].title()}",
            f"[bold]Total Shards on Node:[/bold] {analysis['total_shards']} ({analysis['primary_shards']} primaries, {analysis['replica_shards']} replicas)"
        ]

        if analysis['min_availability'] == 'full':
            summary_lines.extend([
                f"[bold]Data to Move:[/bold] {format_size(analysis['total_data_to_move_gb'])}",
                f"[bold]Available Capacity:[/bold] {format_size(analysis['total_available_capacity_gb'])}",
                f"[bold]Capacity Check:[/bold] {'✅ Sufficient' if analysis['capacity_sufficient'] else '❌ Insufficient'}"
            ])
        else:  # primaries
            summary_lines.extend([
                f"[bold]Fast Operations:[/bold] {analysis['fast_operations']} (primary→replica conversions)",
                f"[bold]Slow Operations:[/bold] {analysis['slow_operations']} (data moves)",
                f"[bold]Data to Move:[/bold] {format_size(analysis['data_to_move_gb'])}",
                f"[bold]Available Capacity:[/bold] {format_size(analysis['total_available_capacity_gb'])}",
                f"[bold]Capacity Check:[/bold] {'✅ Sufficient' if analysis['capacity_sufficient'] else '❌ Insufficient'}"
            ])

        self.console.print(Panel("\n".join(summary_lines), title="📊 Maintenance Analysis Summary", border_style="blue"))
        self.console.print()

        # Shard breakdown table
        from rich.table import Table
        from rich import box

        shard_table = Table(title="Shard Analysis by Type", box=box.ROUNDED)
        shard_table.add_column("Shard Type", style="cyan")
        shard_table.add_column("Count", justify="right")
        shard_table.add_column("Total Size", justify="right")
        shard_table.add_column("Action Required")

        if analysis['min_availability'] == 'full':
            shard_table.add_row(
                "Primary Shards (with replicas)",
                str(len(analysis['primary_with_replicas'])),
                format_size(sum(s['size_gb'] for s in analysis['primary_with_replicas'])),
                "Move data"
            )
            shard_table.add_row(
                "Primary Shards (without replicas)",
                str(len(analysis['primary_without_replicas'])),
                format_size(sum(s['size_gb'] for s in analysis['primary_without_replicas'])),
                "Move data"
            )
            shard_table.add_row(
                "Replica Shards",
                str(analysis['replica_shards']),
                format_size(sum(s['size_gb'] for s in [s for s in analysis['all_shards'] if not s['is_primary']])),
                "Move data"
            )
        else:  # primaries
            shard_table.add_row(
                "Primary Shards (with replicas)",
                str(len(analysis['primary_with_replicas'])),
                format_size(sum(s['size_gb'] for s in analysis['primary_with_replicas'])),
                "Convert to replica (fast)"
            )
            shard_table.add_row(
                "Primary Shards (without replicas)",
                str(len(analysis['primary_without_replicas'])),
                format_size(sum(s['size_gb'] for s in analysis['primary_without_replicas'])),
                "Move data (slow)"
            )
            replica_size = sum(s['size_gb'] for s in analysis['all_shards'] if not s['is_primary'])
            shard_table.add_row(
                "Replica Shards",
                str(analysis['replica_shards']),
                format_size(replica_size),
                "No action needed"
            )

        self.console.print(shard_table)
        self.console.print()

        # Target nodes capacity table
        if analysis['candidate_nodes']:
            capacity_table = Table(title=f"Target Nodes Capacity (Zone: {analysis['target_zone']})", box=box.ROUNDED)
            capacity_table.add_column("Node", style="cyan")
            capacity_table.add_column("Space Below Low WM", justify="right")
            capacity_table.add_column("Shard Capacity", justify="right")
            capacity_table.add_column("Disk Usage", justify="right")
            capacity_table.add_column("Status")

            for node in analysis['candidate_nodes']:
                # Check both space and shard capacity constraints
                space_ok = node['remaining_capacity_gb'] > 0
                shards_ok = node['remaining_shard_capacity'] > 0
                disk_high = node['disk_usage_percent'] > 90

                if space_ok and shards_ok and not disk_high:
                    status = "✅ Available"
                elif not space_ok:
                    status = "❌ No space"
                elif not shards_ok:
                    status = "❌ Max shards"
                elif disk_high:
                    status = "⚠️ High usage"
                else:
                    status = "❌ At capacity"

                # Format shard capacity display
                shard_display = f"{node['remaining_shard_capacity']} / {node['max_shards_per_node']}"

                capacity_table.add_row(
                    node['name'],
                    format_size(node['available_below_watermark_gb']),
                    shard_display,
                    f"{node['disk_usage_percent']:.1f}%",
                    status
                )

            self.console.print(capacity_table)
            self.console.print()
        else:
            self.console.print("[red]❌ CRITICAL: Data cannot be moved - no target nodes in same availability zone[/red]")
            self.console.print("[yellow]  • Target node is isolated in zone '{}'[/yellow]".format(analysis['target_zone']))
            self.console.print("[yellow]  • CrateDB requires data movement within the same availability zone[/yellow]")
            self.console.print("[yellow]  • Consider adding nodes to this zone or adjusting zone configuration[/yellow]")
            self.console.print()

        # Time estimation
        self._display_recovery_time_estimation(analysis, recovery_settings)

        # Recommendations
        self._display_maintenance_recommendations(analysis)

    def _display_recovery_time_estimation(self, analysis: dict, recovery_settings: dict):
        """Display estimated recovery time based on cluster settings"""
        from ...utils import format_size

        max_bytes_per_sec = recovery_settings['max_bytes_per_sec']
        concurrent_recoveries = recovery_settings['node_concurrent_recoveries']

        if analysis['min_availability'] == 'full':
            total_bytes = analysis['total_data_to_move_gb'] * 1024**3
            estimated_seconds = total_bytes / max_bytes_per_sec
        else:  # primaries
            total_bytes = analysis['data_to_move_gb'] * 1024**3
            estimated_seconds = total_bytes / max_bytes_per_sec

        # Convert to human readable time
        hours = int(estimated_seconds // 3600)
        minutes = int((estimated_seconds % 3600) // 60)

        # Format throughput display (fix units)
        throughput_mb_per_sec = max_bytes_per_sec / (1024 * 1024)

        time_lines = [
            f"[bold]Recovery Settings:[/bold]",
            f"  • Max bytes/sec: {throughput_mb_per_sec:.0f}MB/sec",
            f"  • Concurrent recoveries: {concurrent_recoveries}",
            f"",
            f"[bold]Estimated Time:[/bold] {hours}h {minutes}m"
        ]

        if analysis['min_availability'] == 'primaries' and analysis['fast_operations'] > 0:
            time_lines.extend([
                f"",
                f"[dim]Note: {analysis['fast_operations']} primary→replica conversions are fast (seconds)[/dim]",
                f"[dim]Time estimate only applies to {analysis['slow_operations']} data moves[/dim]"
            ])

        self.console.print(Panel("\n".join(time_lines), title="⏱️ Recovery Time Estimation", border_style="green"))
        self.console.print()

    def _display_maintenance_recommendations(self, analysis: dict):
        """Display maintenance recommendations"""
        recommendations = []

        if not analysis['capacity_sufficient']:
            # Check if it's a space issue or shard count issue
            space_sufficient = analysis['total_available_capacity_gb'] >= analysis.get('total_data_to_move_gb', analysis.get('data_to_move_gb', 0))
            shards_sufficient = analysis.get('shards_sufficient', True)

            recommendations.extend([
                "[red]❌ CRITICAL: Insufficient capacity in target zone[/red]"
            ])

            if not space_sufficient:
                recommendations.append(f"  • Need {analysis.get('total_data_to_move_gb', analysis.get('data_to_move_gb', 0)):.1f}GB but only {analysis['total_available_capacity_gb']:.1f}GB available")

            if not shards_sufficient:
                total_shards_needed = len(analysis.get('all_shards', [])) if analysis['min_availability'] == 'full' else len(analysis.get('primary_without_replicas', []))
                recommendations.append(f"  • Need capacity for {total_shards_needed} shards but only {analysis.get('total_shard_capacity', 0)} shard slots available")

            recommendations.extend([
                "  • Consider adding nodes or freeing space before maintenance",
                ""
            ])

        if len(analysis['candidate_nodes']) == 0:
            recommendations.extend([
                "[red]❌ CRITICAL: Node is isolated in its availability zone[/red]",
                f"  • No other nodes available in zone '{analysis['target_zone']}'",
                "  • Data movement is impossible due to zone constraints",
                "  • Solutions:",
                "    - Add nodes to the same availability zone",
                "    - Reconfigure zone allocation if appropriate for your setup",
                "    - Consider cross-zone data movement (requires cluster configuration changes)",
                ""
            ])
        elif len(analysis['candidate_nodes']) < 2:
            recommendations.extend([
                "[yellow]⚠️  Warning: Limited target nodes in availability zone[/yellow]",
                f"  • Only {len(analysis['candidate_nodes'])} candidate node(s) available",
                "  • Consider maintenance window timing to avoid single points of failure",
                ""
            ])

        if analysis['min_availability'] == 'primaries':
            if analysis['fast_operations'] > 0:
                recommendations.extend([
                    f"[green]✅ {analysis['fast_operations']} primary shards can be quickly converted to replicas[/green]",
                    "  • These operations complete in seconds",
                    ""
                ])

            if analysis['slow_operations'] > 0:
                recommendations.extend([
                    f"[yellow]⚠️  {analysis['slow_operations']} primary shards need data movement[/yellow]",
                    "  • These require full shard recovery and take significant time",
                    "  • Consider adding replicas before maintenance to reduce this number",
                    ""
                ])

        recommendations.extend([
            "[bold]Next Steps:[/bold]",
            "1. Verify cluster health before starting maintenance",
            "2. Consider maintenance window timing for minimal impact",
            "3. Monitor recovery progress during maintenance",
            "4. Use: [cyan]xmover monitor-recovery --watch[/cyan] during operations"
        ])

        if analysis['capacity_sufficient']:
            status_color = "green"
            status_title = "✅ Maintenance Feasible"
        else:
            status_color = "red"
            status_title = "❌ Maintenance Blocked"

        self.console.print(Panel("\n".join(recommendations), title=status_title, border_style=status_color))

    def _display_short_maintenance_analysis(self, analysis: dict, recovery_settings: dict, cluster_name: str = None):
        """Display compact maintenance analysis with only essential information"""
        from ...utils import format_size

        # Calculate time estimation
        max_bytes_per_sec = recovery_settings['max_bytes_per_sec']
        throughput_mb_per_sec = max_bytes_per_sec / (1024 * 1024)

        if analysis['min_availability'] == 'full':
            total_bytes = analysis['total_data_to_move_gb'] * 1024**3
            data_to_move = analysis['total_data_to_move_gb']
        else:  # primaries
            total_bytes = analysis['data_to_move_gb'] * 1024**3
            data_to_move = analysis['data_to_move_gb']

        estimated_seconds = total_bytes / max_bytes_per_sec
        hours = int(estimated_seconds // 3600)
        minutes = int((estimated_seconds % 3600) // 60)
        seconds = int(estimated_seconds % 60)

        # Build shard summary
        if analysis['min_availability'] == 'full':
            shard_summary = f"{analysis['total_shards']} total ({analysis['primary_shards']} primaries, {analysis['replica_shards']} replicas)"
        else:  # primaries
            fast_ops = analysis['fast_operations']
            slow_ops = analysis['slow_operations']
            replica_count = analysis['replica_shards']
            if fast_ops > 0:
                shard_summary = f"{analysis['primary_shards']} primaries ({slow_ops} move, {fast_ops} fast-convert), {replica_count} replicas (no action)"
            else:
                shard_summary = f"{analysis['primary_shards']} primaries, {replica_count} replicas (no action)"

        # Target nodes summary
        if len(analysis['candidate_nodes']) == 0:
            target_summary = "No nodes available (zone isolated)"
            status_icon = "❌"
            status_text = "BLOCKED - Zone Isolation"
        elif not analysis['capacity_sufficient']:
            available_count = len([n for n in analysis['candidate_nodes']
                                 if n['remaining_capacity_gb'] > 0 and n['remaining_shard_capacity'] > 0])
            at_capacity_count = len(analysis['candidate_nodes']) - available_count
            target_summary = f"{available_count} available, {at_capacity_count} at capacity"
            status_icon = "❌"
            status_text = "BLOCKED - Insufficient Capacity"
        else:
            available_count = len([n for n in analysis['candidate_nodes']
                                 if n['remaining_capacity_gb'] > 0 and n['remaining_shard_capacity'] > 0])
            at_capacity_count = len(analysis['candidate_nodes']) - available_count
            if at_capacity_count > 0:
                target_summary = f"{available_count} available, {at_capacity_count} at capacity"
            else:
                target_summary = f"{available_count} available"
            status_icon = "✅"
            status_text = "Feasible"

        # Display compact summary with cluster name
        cluster_display = cluster_name or "Unknown"
        self.console.print(f"📊 Pre-Flight Check {cluster_display}: {analysis['target_node']} (Zone: {analysis['target_zone']})")
        self.console.print(f"• Shards to move: {shard_summary}")
        self.console.print(f"• Data to move: {format_size(data_to_move)}")
        self.console.print(f"• Target nodes: {target_summary}")
        # Format time display with seconds if under 1 minute
        if hours == 0 and minutes == 0:
            time_display = f"{seconds}s"
        elif hours == 0:
            time_display = f"{minutes}m {seconds}s"
        else:
            time_display = f"{hours}h {minutes}m"

        self.console.print(f"• Estimated time: {time_display} ({throughput_mb_per_sec:.0f}MB/sec)")
        self.console.print(f"{status_icon} Status: {status_text}")

        # Add critical warnings for blocked scenarios
        if len(analysis['candidate_nodes']) == 0:
            self.console.print(f"[red]⚠️ CRITICAL: Node isolated in zone '{analysis['target_zone']}' - add nodes or reconfigure zones[/red]")
        elif not analysis['capacity_sufficient']:
            self.console.print("[red]⚠️ CRITICAL: Insufficient capacity - add nodes or free space before maintenance[/red]")
