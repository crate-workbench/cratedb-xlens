"""
Base classes and helpers for maintenance commands

This module contains shared data models, helpers, and utilities used across
maintenance command implementations.
"""

import sys
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

from loguru import logger


# ============================================================================
# Constants
# ============================================================================

PARTITION_NULL_VALUE = 'NULL'


# ============================================================================
# Domain Models
# ============================================================================

@dataclass
class TableInfo:
    """Domain model for table/partition information

    Provides type safety and validation for table metadata used in
    replica reset operations. Replaces untyped Dict[str, Any].
    """
    schema_name: str
    table_name: str
    partition_values: Optional[str] = None
    partition_ident: Optional[str] = None
    current_replicas: int = 0
    total_primary_shards: int = 1
    max_translog_uncommitted_mb: float = 0.0
    adaptive_threshold_mb: float = 563.2  # Default 512MB + 10% buffer
    adaptive_config_mb: float = 512.0

    def has_partition(self) -> bool:
        """Check if this represents a partitioned table/partition"""
        return bool(self.partition_values and self.partition_values != PARTITION_NULL_VALUE)

    def get_display_name(self) -> str:
        """Get human-readable table name with partition info"""
        name = f"{self.schema_name}.{self.table_name}"
        if self.has_partition():
            name += f" PARTITION {self.partition_values}"
        return name

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TableInfo':
        """Create TableInfo from dictionary (for backward compatibility)"""
        return cls(
            schema_name=data['schema_name'],
            table_name=data['table_name'],
            partition_values=data.get('partition_values'),
            partition_ident=data.get('partition_ident'),
            current_replicas=data.get('current_replicas', 0),
            total_primary_shards=data.get('total_primary_shards', 1),
            max_translog_uncommitted_mb=data.get('max_translog_uncommitted_mb', 0.0),
            adaptive_threshold_mb=data.get('adaptive_threshold_mb', 563.2),
            adaptive_config_mb=data.get('adaptive_config_mb', 512.0),
        )


# ============================================================================
# Query Helpers
# ============================================================================

class QueryResultHelper:
    """Helper for consistent error handling of CrateDB query results

    CrateDB returns dicts with either:
    - Success: {'rows': [...], ...} (no 'error' key)
    - Failure: {'error': 'message', ...} (has 'error' key)
    """

    @staticmethod
    def is_success(result: Dict[str, Any]) -> bool:
        """Check if query succeeded (no error key present)"""
        return 'error' not in result

    @staticmethod
    def is_error(result: Dict[str, Any]) -> bool:
        """Check if query failed (error key present)"""
        return 'error' in result

    @staticmethod
    def get_error_message(result: Dict[str, Any]) -> str:
        """Extract error message from failed query result"""
        return result.get('error', 'Unknown error')

    @staticmethod
    def get_rows(result: Dict[str, Any]) -> List[Any]:
        """Extract rows from query result"""
        return result.get('rows', [])


# ============================================================================
# Context Managers
# ============================================================================

@contextmanager
def json_logging_mode():
    """Context manager for JSON logging mode

    Temporarily configures loguru for JSON output, then restores original handlers.
    This prevents global state mutation and ensures proper cleanup.
    """
    # Save current handlers for restoration
    handler_id = logger.add(
        sys.stderr,
        format="{time:YYYY-MM-DDTHH:mm:ss.sssZ} | {level} | {message}",
        serialize=True,  # Enable JSON serialization
        level="INFO"
    )

    try:
        yield
    finally:
        # Restore original state by removing our handler
        logger.remove(handler_id)
