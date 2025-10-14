"""
Utility functions for XMover CLI

This module contains shared utility functions used across the XMover application,
including formatting functions for displaying data in the CLI.
"""

from typing import Dict, Any, Tuple


def parse_watermark_percentage(watermark_value: str) -> float:
    """Parse watermark percentage from string like '85%' or '0.85'"""
    try:
        if isinstance(watermark_value, str):
            if watermark_value.endswith('%'):
                parsed_value = float(watermark_value[:-1])
                # Reject negative values
                if parsed_value < 0:
                    return 85.0
                return parsed_value
            else:
                # Handle decimal format like '0.85'
                decimal_value = float(watermark_value)
                # Reject negative values
                if decimal_value < 0:
                    return 85.0
                if decimal_value <= 1.0:
                    return decimal_value * 100
                return decimal_value
        elif isinstance(watermark_value, (int, float)):
            # Reject negative values
            if watermark_value < 0:
                return 85.0
            if watermark_value <= 1.0:
                return watermark_value * 100
            return watermark_value
        else:
            # Default to common values if parsing fails
            return 85.0  # Default low watermark
    except (ValueError, TypeError):
        # Default to common values if parsing fails
        return 85.0  # Default low watermark


def get_effective_disk_usage_threshold(watermark_config: Dict[str, Any], safety_buffer_percent: float = 2.0) -> float:
    """
    Get the effective disk usage threshold based on cluster watermark settings
    
    Args:
        watermark_config: Watermark configuration from get_cluster_watermark_config()
        safety_buffer_percent: Safety buffer below low watermark (default: 2%)
        
    Returns:
        Maximum disk usage percentage to use for moves (with safety buffer)
    """
    # Handle empty or missing config
    if not watermark_config or not watermark_config.get('threshold_enabled', True):
        # If watermarks are disabled or config missing, use a conservative default
        return 85.0
    
    watermarks = watermark_config.get('watermarks', {})
    if not watermarks:
        # If no watermark settings, use conservative default
        return 85.0
        
    low_watermark_str = watermarks.get('low', '85%')
    
    # Parse the low watermark percentage
    low_watermark_percent = parse_watermark_percentage(low_watermark_str)
    
    # Apply safety buffer - ensure we don't get too close to the watermark
    effective_threshold = max(low_watermark_percent - safety_buffer_percent, 75.0)
    
    return effective_threshold


def calculate_watermark_remaining_space(node_total_bytes: int, node_used_bytes: int, 
                                       watermark_config: Dict[str, Any]) -> Dict[str, float]:
    """
    Calculate remaining space until various watermarks are reached
    
    Args:
        node_total_bytes: Total disk space on node in bytes
        node_used_bytes: Currently used disk space in bytes
        watermark_config: Watermark configuration from get_cluster_watermark_config()
        
    Returns:
        Dictionary with remaining space in GB until each watermark
    """
    if not watermark_config.get('threshold_enabled', True):
        # If watermarks disabled, return very high remaining space
        return {
            'remaining_to_low_gb': 999999.0,
            'remaining_to_high_gb': 999999.0,
            'remaining_to_flood_gb': 999999.0
        }
    
    watermarks = watermark_config.get('watermarks', {})
    
    # Parse watermark percentages
    low_percent = parse_watermark_percentage(watermarks.get('low', '85%'))
    high_percent = parse_watermark_percentage(watermarks.get('high', '90%'))
    flood_percent = parse_watermark_percentage(watermarks.get('flood_stage', '95%'))
    
    # Calculate bytes at each watermark
    low_used_bytes = node_total_bytes * (low_percent / 100.0)
    high_used_bytes = node_total_bytes * (high_percent / 100.0)
    flood_used_bytes = node_total_bytes * (flood_percent / 100.0)
    
    # Calculate remaining space (can be negative if already exceeded)
    remaining_to_low_gb = max(0, (low_used_bytes - node_used_bytes) / (1024**3))
    remaining_to_high_gb = max(0, (high_used_bytes - node_used_bytes) / (1024**3))
    remaining_to_flood_gb = max(0, (flood_used_bytes - node_used_bytes) / (1024**3))
    
    return {
        'remaining_to_low_gb': remaining_to_low_gb,
        'remaining_to_high_gb': remaining_to_high_gb,
        'remaining_to_flood_gb': remaining_to_flood_gb
    }


def format_size(size_gb: float) -> str:
    """Format size in GB with appropriate precision"""
    if size_gb >= 1000:
        return f"{size_gb/1000:.1f}TB"
    elif size_gb >= 1:
        return f"{size_gb:.1f}GB"
    else:
        return f"{size_gb*1000:.0f}MB"


def format_percentage(value: float) -> str:
    """Format percentage with color coding"""
    color = "green"
    if value > 80:
        color = "red"
    elif value > 70:
        color = "yellow"
    return f"[{color}]{value:.1f}%[/{color}]"


def format_table_display_with_partition(schema_name: str, table_name: str, partition_values: str = None) -> str:
    """Format table display with partition values if available"""
    # Create base table name
    if schema_name and schema_name != 'doc':
        base_display = f"{schema_name}.{table_name}"
    else:
        base_display = table_name
    
    # Add partition values if available
    if partition_values:
        return f"{base_display} {partition_values}"
    else:
        return base_display


def format_translog_info(recovery_info) -> str:
    """Format translog size information with color coding showing both total and uncommitted sizes"""
    tl_total_bytes = recovery_info.translog_size_bytes
    tl_uncommitted_bytes = recovery_info.translog_uncommitted_bytes
    
    # Only show if significant (>10MB for production) - check uncommitted size primarily
    if tl_uncommitted_bytes < 10 * 1024 * 1024 and tl_total_bytes < 50 * 1024 * 1024:  # 10MB uncommitted or 50MB total
        return ""
    
    tl_total_gb = recovery_info.translog_size_gb
    tl_uncommitted_gb = recovery_info.translog_uncommitted_gb
    
    # Format with color coding based on uncommitted size
    if tl_uncommitted_gb > 5:  # >5GB uncommitted - critical
        color = "red"
        indicator = "🔥"
    elif tl_uncommitted_gb > 1:  # >1GB uncommitted - warning
        color = "yellow" 
        indicator = "⚠️"
    else:  # <1GB - info
        color = "blue"
        indicator = "ℹ️"
    
    # Format display with both total and uncommitted sizes
    return f"[{color}]{indicator} TL: {tl_total_gb:.1f}GB (U: {tl_uncommitted_gb:.1f}GB)[/{color}]"


def format_recovery_progress(recovery_info) -> str:
    """Format recovery progress, using sequence number progress for replicas when available"""
    if not recovery_info.is_primary and recovery_info.seq_no_progress is not None:
        # For replica shards, show sequence number progress if available
        seq_progress = recovery_info.seq_no_progress
        traditional_progress = recovery_info.overall_progress
        
        # If sequence progress is significantly different from traditional progress, show both
        if abs(seq_progress - traditional_progress) > 5.0:
            return f"{seq_progress:.1f}% (seq) / {traditional_progress:.1f}% (rec)"
        else:
            # Use sequence progress as primary indicator for replicas
            return f"{seq_progress:.1f}%"
    else:
        # Use traditional progress for primaries or when seq progress not available
        return f"{recovery_info.overall_progress:.1f}%"


def parse_table_partition_identifier(identifier: str) -> tuple[str, str | None]:
    """Parse table[partition] syntax into table name and partition identifier
    
    Args:
        identifier: Table identifier, optionally with partition in square brackets
                   Examples: "events.logs[2024-01]", "simple_table", "schema.table"
    
    Returns:
        Tuple of (table_name, partition_ident) where partition_ident is None if not specified
    
    Examples:
        >>> parse_table_partition_identifier("events.logs[2024-01]")
        ("events.logs", "2024-01")
        >>> parse_table_partition_identifier("simple_table")
        ("simple_table", None)
    """
    # Check for valid partition syntax: contains '[', ends with ']', and has content before '['
    if '[' in identifier and identifier.endswith(']'):
        # Find the last '[' to handle nested brackets in partition names
        bracket_pos = identifier.rfind('[')
        table_part = identifier[:bracket_pos]
        partition_part = identifier[bracket_pos + 1:-1]  # Remove '[' and ']'
        
        # Only return parsed partition if we have a valid table name
        if table_part:
            return table_part, partition_part
    
    # Return as-is without partition for invalid syntax or no partition
    return identifier, None


def format_table_identifier_with_partition(schema_name: str, table_name: str, partition_ident: str = None) -> str:
    """Format a complete table identifier with optional partition
    
    Args:
        schema_name: Schema name (use 'doc' for default schema)
        table_name: Table name
        partition_ident: Optional partition identifier
        
    Returns:
        Formatted identifier string
        
    Examples:
        >>> format_table_identifier_with_partition('events', 'logs', '2024-01')
        'events.logs[2024-01]'
        >>> format_table_identifier_with_partition('doc', 'users', None)
        'users'
    """
    # Create base table name
    if schema_name and schema_name != 'doc':
        base_name = f"{schema_name}.{table_name}"
    else:
        base_name = table_name
    
    # Add partition if provided
    if partition_ident:
        return f"{base_name}[{partition_ident}]"
    return base_name


def format_partition_display(partition_ident: str = None, placeholder: str = "—") -> str:
    """Format partition identifier for display in tables
    
    Args:
        partition_ident: Partition identifier or None
        placeholder: What to show when no partition (default: "—")
        
    Returns:
        Formatted partition display string
    """
    if partition_ident and partition_ident.strip():
        return partition_ident.strip()
    return placeholder


def validate_partition_syntax(identifier: str) -> bool:
    """Validate that a table[partition] identifier has correct syntax
    
    Args:
        identifier: Table identifier to validate
        
    Returns:
        True if syntax is valid
        
    Examples:
        >>> validate_partition_syntax("table[partition]")
        True
        >>> validate_partition_syntax("table[partition")
        False
        >>> validate_partition_syntax("table]partition[")
        False
    """
    # Empty string is invalid
    if not identifier:
        return False
    
    # If no brackets, it's a valid table name
    if '[' not in identifier and ']' not in identifier:
        return True
    
    # If contains brackets, must be valid partition syntax
    if '[' not in identifier or ']' not in identifier:
        return False  # Has one bracket type but not the other
    
    # Must end with ']'
    if not identifier.endswith(']'):
        return False
    
    # Find the last '[' to handle potential nested brackets in partition names
    bracket_pos = identifier.rfind('[')
    if bracket_pos <= 0:  # '[' must not be at start (need table name)
        return False
    
    table_part = identifier[:bracket_pos]
    partition_part = identifier[bracket_pos + 1:-1]  # Remove '[' and ']'
    
    # Must have content in both table and partition parts
    if not table_part or not partition_part:
        return False
    
    # Check for invalid patterns
    if identifier.count('[') != 1:  # Multiple '[' brackets
        return False
    
    if ']' in identifier[:-1]:  # ']' appears before the end
        return False
    
    return True