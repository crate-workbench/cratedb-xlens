"""
XMover Commands Module

This module contains command handlers extracted from the monolithic CLI file.
Each command is organized into logical groups for better maintainability.

Command Organization:
- analysis.py: Analysis commands (analyze, deep_analyze)
- monitoring.py: Monitoring commands (monitor_recovery, active_shards, large_translogs)
- maintenance.py: Maintenance commands (problematic_translogs, shard_distribution)
- operations.py: Operations commands (find_candidates, recommend, validate_move)
- diagnostics.py: Diagnostic commands (test_connection, explain_error, check_balance)
"""

from .base import BaseCommand
from .diagnostics import DiagnosticsCommands, create_diagnostics_commands
from .analysis import AnalysisCommands, create_analysis_commands
from .monitoring import MonitoringCommands, create_monitoring_commands
from .operations import OperationsCommands, create_operations_commands
from .maintenance import MaintenanceCommands, create_maintenance_commands

__all__ = [
    'BaseCommand',
    'DiagnosticsCommands',
    'create_diagnostics_commands',
    'AnalysisCommands',
    'create_analysis_commands',
    'MonitoringCommands',
    'create_monitoring_commands',
    'OperationsCommands',
    'create_operations_commands',
    'MaintenanceCommands',
    'create_maintenance_commands',
]