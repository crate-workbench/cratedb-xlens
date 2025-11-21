"""
Maintenance command handlers for XMover (Compatibility Shim)

This module is a backward compatibility shim that imports from the refactored
maintenance package. All functionality has been moved to:
    src/xmover/commands/maintenance/

Package structure:
- maintenance/base.py: Shared data models and helpers
- maintenance/shard_distribution.py: Shard distribution analysis
- maintenance/problematic_translogs/: Translog management (subpackage)
    - command.py: Main command logic
    - display.py: Display formatting
    - sql_generator.py: SQL generation
    - autoexec.py: Automatic execution
- maintenance/node_maintenance.py: Node decommissioning analysis
- maintenance/cli.py: CLI command registration

For new code, prefer importing directly from the maintenance package:
    from xmover.commands.maintenance import ProblematicTranslogsCommand
"""

# Re-export everything from the maintenance package for backward compatibility
from .maintenance import (
    # Base classes and helpers
    TableInfo,
    QueryResultHelper,
    json_logging_mode,
    PARTITION_NULL_VALUE,

    # Command classes
    ShardDistributionCommand,
    ProblematicTranslogsCommand,
    NodeMaintenanceCommand,

    # Legacy unified class (delegates to individual command classes)
    MaintenanceCommands,

    # CLI registration
    create_maintenance_commands,
)

__all__ = [
    'TableInfo',
    'QueryResultHelper',
    'json_logging_mode',
    'PARTITION_NULL_VALUE',
    'ShardDistributionCommand',
    'ProblematicTranslogsCommand',
    'NodeMaintenanceCommand',
    'MaintenanceCommands',
    'create_maintenance_commands',
]
