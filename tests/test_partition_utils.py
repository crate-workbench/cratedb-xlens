"""
Tests for partition utility functions

Tests the utility functions for parsing and formatting partition identifiers
that were added to support the partition-aware functionality.
"""

import pytest
from xmover.utils import (
    parse_table_partition_identifier,
    format_table_identifier_with_partition,
    format_partition_display,
    validate_partition_syntax
)


class TestPartitionIdentifierParsing:
    """Test parsing of table[partition] syntax"""
    
    def test_parse_table_partition_identifier_valid_cases(self):
        """Test parsing valid table[partition] identifiers"""
        test_cases = [
            ("events.logs[2024-01]", ("events.logs", "2024-01")),
            ("simple_table[partition_1]", ("simple_table", "partition_1")),
            ("schema.table[part_2024_Q1]", ("schema.table", "part_2024_Q1")),
            ("logs[2024-01-15]", ("logs", "2024-01-15")),
            ("complex.name[very_long_partition_name_123]", ("complex.name", "very_long_partition_name_123")),
        ]
        
        for identifier, expected in test_cases:
            result = parse_table_partition_identifier(identifier)
            assert result == expected, f"Failed for input: {identifier}"
    
    def test_parse_table_partition_identifier_no_partition(self):
        """Test parsing identifiers without partition syntax"""
        test_cases = [
            ("simple_table", ("simple_table", None)),
            ("schema.table", ("schema.table", None)),
            ("doc.users", ("doc.users", None)),
            ("logs", ("logs", None)),
        ]
        
        for identifier, expected in test_cases:
            result = parse_table_partition_identifier(identifier)
            assert result == expected, f"Failed for input: {identifier}"
    
    def test_parse_table_partition_identifier_edge_cases(self):
        """Test parsing edge cases"""
        test_cases = [
            ("table[]", ("table", "")),  # Empty partition
            ("table[a]", ("table", "a")),  # Single character partition
            ("a[b]", ("a", "b")),  # Single character table and partition
            # Note: Nested brackets in partition names will be parsed literally
            ("schema.table[partition_with_underscores]", ("schema.table", "partition_with_underscores")),
        ]
        
        for identifier, expected in test_cases:
            result = parse_table_partition_identifier(identifier)
            assert result == expected, f"Failed for input: {identifier}"
    
    def test_parse_table_partition_identifier_invalid_cases(self):
        """Test that invalid syntax still parses (gracefully fails)"""
        # These should parse as table names without partitions
        test_cases = [
            ("table[partition", ("table[partition", None)),  # Missing closing bracket
            ("table]partition[", ("table]partition[", None)),  # Wrong bracket order
            ("[partition]", ("[partition]", None)),  # No table name (empty table part)
        ]
        
        for identifier, expected in test_cases:
            result = parse_table_partition_identifier(identifier)
            assert result == expected, f"Failed for input: {identifier}"


class TestPartitionIdentifierFormatting:
    """Test formatting of table identifiers with partitions"""
    
    def test_format_table_identifier_with_partition_doc_schema(self):
        """Test formatting with doc schema (should be omitted)"""
        result = format_table_identifier_with_partition('doc', 'users', None)
        assert result == 'users'
        
        result = format_table_identifier_with_partition('doc', 'users', '2024-01')
        assert result == 'users[2024-01]'
    
    def test_format_table_identifier_with_partition_other_schemas(self):
        """Test formatting with non-doc schemas"""
        result = format_table_identifier_with_partition('events', 'logs', None)
        assert result == 'events.logs'
        
        result = format_table_identifier_with_partition('events', 'logs', '2024-01')
        assert result == 'events.logs[2024-01]'
        
        result = format_table_identifier_with_partition('timeseries', 'metrics', 'Q1-2024')
        assert result == 'timeseries.metrics[Q1-2024]'
    
    def test_format_table_identifier_with_partition_edge_cases(self):
        """Test formatting edge cases"""
        # Empty/None schema
        result = format_table_identifier_with_partition(None, 'table', '2024-01')
        assert result == 'table[2024-01]'
        
        result = format_table_identifier_with_partition('', 'table', '2024-01') 
        assert result == 'table[2024-01]'
        
        # Empty partition (should not show brackets)
        result = format_table_identifier_with_partition('events', 'logs', '')
        assert result == 'events.logs'
        
        result = format_table_identifier_with_partition('events', 'logs', None)
        assert result == 'events.logs'


class TestPartitionDisplayFormatting:
    """Test display formatting for partition identifiers"""
    
    def test_format_partition_display_default_placeholder(self):
        """Test formatting with default placeholder"""
        assert format_partition_display('2024-01') == '2024-01'
        assert format_partition_display(None) == '—'
        assert format_partition_display('') == '—'
        assert format_partition_display('   ') == '—'  # Whitespace only
    
    def test_format_partition_display_custom_placeholder(self):
        """Test formatting with custom placeholder"""
        assert format_partition_display('2024-01', '-') == '2024-01'
        assert format_partition_display(None, 'N/A') == 'N/A'
        assert format_partition_display('', 'NONE') == 'NONE'
        assert format_partition_display('   ', '(empty)') == '(empty)'
    
    def test_format_partition_display_strips_whitespace(self):
        """Test that whitespace is stripped from partition identifiers"""
        assert format_partition_display('  2024-01  ') == '2024-01'
        assert format_partition_display('\t2024-01\n') == '2024-01'
        assert format_partition_display(' Q1-2024 ') == 'Q1-2024'


class TestPartitionSyntaxValidation:
    """Test validation of partition syntax"""
    
    def test_validate_partition_syntax_valid_cases(self):
        """Test validation of valid partition syntax"""
        valid_cases = [
            "table[partition]",
            "schema.table[2024-01]", 
            "logs[Q1]",
            "events.logs[partition_2024_01_15]",
            "a[b]",  # Minimal valid case
            "table[partition_with_underscores_123]",
            "schema.table[partition-with-dashes]",
        ]
        
        for identifier in valid_cases:
            assert validate_partition_syntax(identifier), f"Should be valid: {identifier}"
    
    def test_validate_partition_syntax_no_partition_is_valid(self):
        """Test that identifiers without partitions are valid"""
        valid_cases = [
            "table",
            "schema.table",
            "logs",
            "events.logs",
            "simple_name",
            "complex.schema.name",  # Multiple dots should be fine
        ]
        
        for identifier in valid_cases:
            assert validate_partition_syntax(identifier), f"Should be valid: {identifier}"
    
    def test_validate_partition_syntax_invalid_cases(self):
        """Test validation of invalid partition syntax"""
        invalid_cases = [
            "table[",  # Missing closing bracket
            "table]",  # Missing opening bracket  
            "table[partition",  # Missing closing bracket
            "table]partition[",  # Wrong bracket order
            "[partition]",  # Missing table name
            "table[]",  # Empty partition (should be invalid)
            "table[partition]extra",  # Content after closing bracket
            "table[partition][another]",  # Multiple partitions
            "table[[partition]]",  # Multiple opening brackets
            "table[part[ition]",  # Nested opening bracket
        ]
        
        for identifier in invalid_cases:
            assert not validate_partition_syntax(identifier), f"Should be invalid: {identifier}"
    
    def test_validate_partition_syntax_edge_cases(self):
        """Test validation of edge cases"""
        # These should be valid
        assert validate_partition_syntax("a[b]")  # Minimal
        assert validate_partition_syntax("very_long_table_name[very_long_partition_name]")  # Long names
        
        # These should be invalid
        assert not validate_partition_syntax("")  # Empty string
        assert not validate_partition_syntax("[")  # Just opening bracket
        assert not validate_partition_syntax("]")  # Just closing bracket
        assert not validate_partition_syntax("[]")  # Just brackets


class TestPartitionUtilsIntegration:
    """Integration tests for partition utilities working together"""
    
    def test_parse_and_format_roundtrip(self):
        """Test that parsing and formatting work together"""
        test_cases = [
            "events.logs[2024-01]",
            "simple_table[partition]", 
            "logs",  # No partition
            "schema.table",  # No partition
        ]
        
        for original in test_cases:
            # Parse the identifier
            table, partition = parse_table_partition_identifier(original)
            
            # Extract schema and table from the table part
            if '.' in table:
                schema, table_name = table.split('.', 1)
            else:
                schema, table_name = 'doc', table
            
            # Format it back
            formatted = format_table_identifier_with_partition(schema, table_name, partition)
            
            # For doc schema, the result might differ (doc. gets omitted)
            if original.startswith('doc.'):
                expected = original[4:]  # Remove 'doc.' prefix
            else:
                expected = original
            
            assert formatted == expected or (schema == 'doc' and formatted == table_name + (f'[{partition}]' if partition else ''))
    
    def test_display_formatting_consistency(self):
        """Test that display formatting is consistent"""
        test_cases = [
            ('2024-01', '2024-01'),
            (None, '—'),
            ('', '—'),
            ('  ', '—'),
            ('  Q1-2024  ', 'Q1-2024'),  # Should strip whitespace
        ]
        
        for input_partition, expected_display in test_cases:
            result = format_partition_display(input_partition)
            assert result == expected_display
    
    def test_validation_matches_parsing_behavior(self):
        """Test that validation results match parsing behavior"""
        # Valid syntax should parse correctly
        valid_identifiers = [
            "table[partition]",
            "schema.table[2024-01]",
            "simple_table",
        ]
        
        for identifier in valid_identifiers:
            if validate_partition_syntax(identifier):
                # Should parse without errors
                table, partition = parse_table_partition_identifier(identifier)
                assert table is not None
                # Partition can be None for non-partitioned tables
        
        # Invalid syntax might still parse (gracefully), but validation should catch it
        invalid_identifiers = [
            "table[",
            "table[]", 
            "[partition]",
        ]
        
        for identifier in invalid_identifiers:
            is_valid = validate_partition_syntax(identifier)
            # These should be flagged as invalid
            assert not is_valid, f"Should be invalid: {identifier}"


class TestPartitionUtilsErrorHandling:
    """Test error handling and edge cases"""
    
    def test_parse_table_partition_identifier_empty_input(self):
        """Test parsing empty or None input"""
        result = parse_table_partition_identifier("")
        assert result == ("", None)
        
        # Should handle None gracefully (though not expected in normal use)
        try:
            result = parse_table_partition_identifier(None)
            assert False, "Should raise an exception for None input"
        except (TypeError, AttributeError):
            pass  # Expected
    
    def test_format_table_identifier_with_partition_none_inputs(self):
        """Test formatting with None inputs"""
        # None table name should work (though unusual)
        result = format_table_identifier_with_partition('schema', None, '2024-01')
        assert result == 'schema.None[2024-01]'
        
        # None schema should default to no schema prefix
        result = format_table_identifier_with_partition(None, 'table', '2024-01')
        assert result == 'table[2024-01]'
    
    def test_validate_partition_syntax_type_safety(self):
        """Test that validation handles different input types safely"""
        # Should handle None gracefully by returning False
        result = validate_partition_syntax(None)
        assert result == False  # None should be invalid
        
        # Should handle other types gracefully by converting to string
        # (though this is not recommended usage)
        assert validate_partition_syntax("123") == True  # String number is valid table name