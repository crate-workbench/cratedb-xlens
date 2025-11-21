"""
Maintenance commands package for XMover

This package contains commands related to cluster maintenance operations:
- shard_distribution: Analyze shard distribution anomalies across cluster nodes
- problematic_translogs: Find tables with problematic translog sizes and manage replicas
- check_maintenance: Analyze node decommissioning feasibility

The package is organized into focused modules:
- base: Shared data models and helpers
- shard_distribution: Shard distribution analysis
- problematic_translogs/: Translog management (multi-module subpackage)
- node_maintenance: Node decommissioning analysis
- cli: CLI command registration
"""

# Re-export base classes for backward compatibility
from .base import TableInfo, QueryResultHelper, json_logging_mode, PARTITION_NULL_VALUE

# Re-export command classes
from .shard_distribution import ShardDistributionCommand
from .problematic_translogs import ProblematicTranslogsCommand
from .node_maintenance import NodeMaintenanceCommand

# Re-export CLI registration function
from .cli import create_maintenance_commands


# Backward compatibility: MaintenanceCommands class that delegates to individual command classes
class MaintenanceCommands:
    """Legacy unified maintenance commands class for backward compatibility

    This class delegates to the individual command classes:
    - ShardDistributionCommand
    - ProblematicTranslogsCommand
    - NodeMaintenanceCommand

    New code should use the individual command classes directly.
    """

    def __init__(self, client):
        self.client = client
        self._shard_dist = ShardDistributionCommand(client)
        self._problematic = ProblematicTranslogsCommand(client)
        self._node_maint = NodeMaintenanceCommand(client)

    def shard_distribution(self, top_tables: int, table=None):
        """Delegate to ShardDistributionCommand"""
        return self._shard_dist.execute(top_tables, table)

    def problematic_translogs(self, sizemb: int, execute: bool, autoexec: bool = False,
                            dry_run: bool = False, percentage: int = 200,
                            max_wait: int = 720, log_format: str = "console"):
        """Delegate to ProblematicTranslogsCommand"""
        return self._problematic.execute(sizemb, execute, autoexec, dry_run,
                                        percentage, max_wait, log_format)

    def check_maintenance(self, node: str, min_availability: str, short: bool = False):
        """Delegate to NodeMaintenanceCommand"""
        return self._node_maint.execute(node, min_availability, short)


__all__ = [
    # Base classes and helpers
    'TableInfo',
    'QueryResultHelper',
    'json_logging_mode',
    'PARTITION_NULL_VALUE',

    # Command classes
    'ShardDistributionCommand',
    'ProblematicTranslogsCommand',
    'NodeMaintenanceCommand',

    # Legacy unified class
    'MaintenanceCommands',

    # CLI registration
    'create_maintenance_commands',
]
