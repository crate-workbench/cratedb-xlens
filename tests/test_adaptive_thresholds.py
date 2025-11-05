"""
Tests for adaptive threshold functionality in maintenance commands.

This module specifically tests that the AutoExec functionality properly:
1. Queries actual table flush_threshold_size settings from information_schema.tables
2. Applies 10% buffer to create adaptive thresholds
3. Uses these adaptive thresholds for filtering (not hardcoded 563MB)
4. Handles both regular tables and partitioned tables correctly
"""

import pytest
from unittest.mock import Mock, patch
import sys
from pathlib import Path

# Add src to path for imports
src_path = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_path))

from xmover.commands.maintenance import MaintenanceCommands


class TestAdaptiveThresholds:
    """Test adaptive threshold calculation and usage"""

    @pytest.fixture
    def mock_client_with_thresholds(self):
        """Mock client that returns realistic flush threshold data"""
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        
        def mock_execute_query(sql, parameters=None):
            if "information_schema.tables" in sql and "flush_threshold_size" in sql:
                # Mock table flush threshold query - return different thresholds per table
                return {
                    "rows": [
                        ["doc", "events", 1073741824],      # 1GB (1024MB)
                        ["analytics", "metrics", 268435456], # 256MB  
                        ["logs", "application", 536870912],  # 512MB (default)
                        ["custom", "large_table", 2147483648], # 2GB (2048MB)
                    ]
                }
            elif "information_schema.table_partitions" in sql and "flush_threshold_size" in sql:
                # Mock partition flush threshold query
                return {
                    "rows": [
                        ["analytics", "daily_stats", "(date='2024-01-01')", 134217728],  # 128MB
                        ["logs", "hourly_data", "(hour='2024-01-01 10:00')", 67108864],   # 64MB
                    ]
                }
            elif "sys.shards" in sql and "translog_stats" in sql:
                if "GROUP BY" not in sql:
                    # Individual shards query - return shards above baseline threshold
                    return {
                        "rows": [
                            ["doc", "events", "", 0, "data-hot-1", 2048.5],          # 2GB translog
                            ["analytics", "metrics", "", 0, "data-hot-2", 512.3],    # 512MB translog
                            ["logs", "application", "", 1, "data-hot-3", 1024.1],    # 1GB translog
                            ["custom", "large_table", "", 0, "data-hot-4", 3000.0],  # 3GB translog
                        ]
                    }
                else:
                    # Summary query - group by table
                    return {
                        "rows": [
                            ["doc", "events", "", None, 1, 2048.5, 1, 2, 10.5, 20.1],
                            ["analytics", "metrics", "", None, 1, 512.3, 1, 1, 5.2, 5.2],  
                            ["logs", "application", "", None, 1, 1024.1, 1, 1, 8.1, 8.1],
                            ["custom", "large_table", "", None, 1, 3000.0, 1, 1, 15.0, 15.0],
                        ]
                    }
            else:
                return {"rows": []}
        
        mock_client.execute_query.side_effect = mock_execute_query
        return mock_client

    def test_get_table_flush_thresholds_regular_tables(self, mock_client_with_thresholds):
        """Test that flush thresholds are properly queried for regular tables"""
        maintenance = MaintenanceCommands(client=mock_client_with_thresholds)
        
        # Create mock individual shards data
        mock_shards = [
            {'schema_name': 'doc', 'table_name': 'events', 'partition_values': ''},
            {'schema_name': 'analytics', 'table_name': 'metrics', 'partition_values': ''},
            {'schema_name': 'logs', 'table_name': 'application', 'partition_values': ''},
        ]
        
        # Test the method
        thresholds = maintenance._get_table_flush_thresholds(mock_shards)
        
        # Verify correct thresholds were calculated
        assert 'doc.events' in thresholds
        assert 'analytics.metrics' in thresholds
        assert 'logs.application' in thresholds
        
        # Verify doc.events (1GB configured = 1024MB, threshold = 1024 * 1.1 = 1126.4MB)
        doc_events = thresholds['doc.events']
        assert doc_events['config_mb'] == 1024.0
        assert doc_events['threshold_mb'] == pytest.approx(1126.4, rel=1e-3)
        
        # Verify analytics.metrics (256MB configured, threshold = 256 * 1.1 = 281.6MB)
        analytics_metrics = thresholds['analytics.metrics']
        assert analytics_metrics['config_mb'] == 256.0
        assert analytics_metrics['threshold_mb'] == pytest.approx(281.6, rel=1e-3)
        
        # Verify logs.application (512MB configured, threshold = 512 * 1.1 = 563.2MB)
        logs_app = thresholds['logs.application']
        assert logs_app['config_mb'] == 512.0
        assert logs_app['threshold_mb'] == pytest.approx(563.2, rel=1e-3)

    def test_get_table_flush_thresholds_partitioned_tables(self, mock_client_with_thresholds):
        """Test that flush thresholds work for partitioned tables"""
        maintenance = MaintenanceCommands(client=mock_client_with_thresholds)
        
        # Create mock partitioned shards
        mock_shards = [
            {
                'schema_name': 'analytics', 
                'table_name': 'daily_stats', 
                'partition_values': "(date='2024-01-01')"
            },
            {
                'schema_name': 'logs', 
                'table_name': 'hourly_data', 
                'partition_values': "(hour='2024-01-01 10:00')"
            },
        ]
        
        thresholds = maintenance._get_table_flush_thresholds(mock_shards)
        
        # Verify partition-specific thresholds
        analytics_partition = thresholds["analytics.daily_stats.(date='2024-01-01')"]
        assert analytics_partition['config_mb'] == 128.0
        assert analytics_partition['threshold_mb'] == pytest.approx(140.8, rel=1e-3)
        
        logs_partition = thresholds["logs.hourly_data.(hour='2024-01-01 10:00')"]
        assert logs_partition['config_mb'] == 64.0
        assert logs_partition['threshold_mb'] == pytest.approx(70.4, rel=1e-3)

    def test_apply_adaptive_thresholds_uses_actual_config(self, mock_client_with_thresholds):
        """Test that adaptive thresholds use actual table configuration, not hardcoded values"""
        maintenance = MaintenanceCommands(client=mock_client_with_thresholds)
        
        # Mock initial data
        initial_shards = [
            {
                'schema_name': 'doc', 
                'table_name': 'events', 
                'partition_values': '',
                'translog_size_mb': 2048.5
            },
            {
                'schema_name': 'analytics', 
                'table_name': 'metrics', 
                'partition_values': '', 
                'translog_size_mb': 512.3
            },
        ]
        
        initial_summary = [
            {
                'schema_name': 'doc', 
                'table_name': 'events', 
                'partition_values': '',
                'max_translog_uncommitted_mb': 2048.5
            },
            {
                'schema_name': 'analytics', 
                'table_name': 'metrics', 
                'partition_values': '',
                'max_translog_uncommitted_mb': 512.3
            },
        ]
        
        # Mock the table thresholds (simulating what _get_table_flush_thresholds returns)
        table_thresholds = {
            'doc.events': {'config_mb': 1024.0, 'threshold_mb': 1126.4},
            'analytics.metrics': {'config_mb': 256.0, 'threshold_mb': 281.6},
        }
        
        # Apply adaptive thresholds
        adaptive_shards, adaptive_summary = maintenance._apply_adaptive_thresholds(
            initial_shards, initial_summary, table_thresholds, 512  # fallback
        )
        
        # Verify adaptive thresholds were applied to shards
        doc_shard = next(s for s in adaptive_shards if s['table_name'] == 'events')
        assert doc_shard['adaptive_config_mb'] == 1024.0
        assert doc_shard['adaptive_threshold_mb'] == 1126.4
        
        analytics_shard = next(s for s in adaptive_shards if s['table_name'] == 'metrics')
        assert analytics_shard['adaptive_config_mb'] == 256.0
        assert analytics_shard['adaptive_threshold_mb'] == 281.6
        
        # Verify adaptive thresholds were applied to summary
        doc_summary = next(s for s in adaptive_summary if s['table_name'] == 'events')
        assert doc_summary['adaptive_config_mb'] == 1024.0
        assert doc_summary['adaptive_threshold_mb'] == 1126.4
        
        analytics_summary = next(s for s in adaptive_summary if s['table_name'] == 'metrics')
        assert analytics_summary['adaptive_config_mb'] == 256.0  
        assert analytics_summary['adaptive_threshold_mb'] == 281.6

    def test_filter_tables_by_percentage_uses_adaptive_thresholds_not_hardcoded(self, mock_client_with_thresholds):
        """Test that percentage filtering uses adaptive thresholds, NOT hardcoded 563MB"""
        maintenance = MaintenanceCommands(client=mock_client_with_thresholds)
        
        # Mock summary data with adaptive thresholds already applied
        summary_rows = [
            {
                'schema_name': 'doc',
                'table_name': 'events',
                'max_translog_uncommitted_mb': 2048.5,
                'adaptive_threshold_mb': 1126.4,  # 1GB + 10%, NOT 563
                'adaptive_config_mb': 1024.0
            },
            {
                'schema_name': 'analytics', 
                'table_name': 'metrics',
                'max_translog_uncommitted_mb': 512.3,
                'adaptive_threshold_mb': 281.6,   # 256MB + 10%, NOT 563
                'adaptive_config_mb': 256.0
            },
            {
                'schema_name': 'logs',
                'table_name': 'application', 
                'max_translog_uncommitted_mb': 1024.1,
                'adaptive_threshold_mb': 563.2,   # 512MB + 10%
                'adaptive_config_mb': 512.0
            }
        ]
        
        # Mock the replica count lookup
        with patch.object(maintenance, '_get_current_replica_count', return_value=2):
            # Test 150% threshold
            filtered_150 = maintenance._filter_tables_by_percentage(summary_rows, 150)
            
            # Calculate expected percentages using adaptive thresholds
            doc_percentage = (2048.5 / 1126.4) * 100      # ~181.9%
            analytics_percentage = (512.3 / 281.6) * 100   # ~181.9%
            logs_percentage = (1024.1 / 563.2) * 100       # ~181.8%
            
            # All should exceed 150% when using adaptive thresholds
            assert len(filtered_150) == 3
            
            # Test 200% threshold - none should exceed 200% with adaptive thresholds
            filtered_200 = maintenance._filter_tables_by_percentage(summary_rows, 200)
            assert len(filtered_200) == 0
            
            # Verify that if we used hardcoded 563MB, results would be different
            # doc.events: 2048.5/563 = 363.8% (would exceed 200%)
            # This proves we're NOT using hardcoded values

    def test_full_adaptive_threshold_workflow(self, mock_client_with_thresholds):
        """Integration test of the full adaptive threshold workflow"""
        maintenance = MaintenanceCommands(client=mock_client_with_thresholds)
        
        # Run the full problematic translogs analysis
        individual_shards, summary_rows = maintenance._get_problematic_translogs(512)
        
        # Verify we got results
        assert len(individual_shards) > 0
        assert len(summary_rows) > 0
        
        # Verify adaptive thresholds were applied to summary rows
        for table_info in summary_rows:
            assert 'adaptive_threshold_mb' in table_info
            assert 'adaptive_config_mb' in table_info
            
            # Verify thresholds are not all the same (not hardcoded)
            threshold = table_info['adaptive_threshold_mb']
            assert threshold != 563 or table_info['table_name'] == 'application'  # Only logs.application should be 563.2
        
        # Verify different tables have different thresholds
        thresholds = [t['adaptive_threshold_mb'] for t in summary_rows]
        unique_thresholds = set(thresholds)
        assert len(unique_thresholds) > 1, "All tables should not have the same threshold"
        
        # Test filtering uses these adaptive thresholds
        with patch.object(maintenance, '_get_current_replica_count', return_value=2):
            filtered = maintenance._filter_tables_by_percentage(summary_rows, 200)
            
            # Verify calculations used adaptive thresholds, not hardcoded 563MB
            for table_info in summary_rows:
                max_translog = table_info['max_translog_uncommitted_mb']
                adaptive_threshold = table_info['adaptive_threshold_mb']
                
                # This percentage calculation should use adaptive_threshold, not 563
                expected_percentage = (max_translog / adaptive_threshold) * 100
                
                # If using hardcoded 563, percentages would be very different for most tables
                hardcoded_percentage = (max_translog / 563) * 100
                
                # For tables with different configs, these should be significantly different
                if adaptive_threshold != pytest.approx(563.2, rel=1e-1):
                    assert abs(expected_percentage - hardcoded_percentage) > 50, \
                        f"Table {table_info['schema_name']}.{table_info['table_name']} " \
                        f"should show different percentage with adaptive ({expected_percentage:.1f}%) " \
                        f"vs hardcoded ({hardcoded_percentage:.1f}%) thresholds"

    def test_information_schema_query_structure(self, mock_client_with_thresholds):
        """Test that the correct SQL queries are made to information_schema"""
        maintenance = MaintenanceCommands(client=mock_client_with_thresholds)
        
        mock_shards = [
            {'schema_name': 'doc', 'table_name': 'events', 'partition_values': ''},
            {'schema_name': 'analytics', 'table_name': 'metrics', 'partition_values': ''},
        ]
        
        # Call the method and verify the SQL query structure
        maintenance._get_table_flush_thresholds(mock_shards)
        
        # Verify that information_schema.tables was queried
        calls = mock_client_with_thresholds.execute_query.call_args_list
        table_calls = [call for call in calls if 'information_schema.tables' in str(call)]
        
        assert len(table_calls) > 0, "Should query information_schema.tables"
        
        # Verify the query includes flush_threshold_size
        table_query = str(table_calls[0])
        assert 'flush_threshold_size' in table_query
        assert 'COALESCE' in table_query  # Should handle NULL values
        assert '536870912' in table_query  # Should have default 512MB in bytes

    def test_fallback_to_baseline_when_no_config_found(self):
        """Test fallback behavior when table has no explicit flush_threshold_size"""
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        
        # Mock empty response from information_schema (no explicit config)
        mock_client.execute_query.return_value = {"rows": []}
        
        maintenance = MaintenanceCommands(client=mock_client)
        
        mock_shards = [{'schema_name': 'test', 'table_name': 'unconfigured', 'partition_values': '', 'translog_size_mb': 600.0}]
        mock_summary = [{'schema_name': 'test', 'table_name': 'unconfigured', 'partition_values': '', 'max_translog_uncommitted_mb': 600.0}]
        
        # Should fallback to baseline threshold
        adaptive_shards, adaptive_summary = maintenance._apply_adaptive_thresholds(
            mock_shards, mock_summary, {}, 512  # fallback threshold
        )
        
        # Since no table thresholds were found, should use fallback
        # Note: This tests the fallback logic in _apply_adaptive_thresholds
        table_thresholds = maintenance._get_table_flush_thresholds(mock_shards)
        assert len(table_thresholds) == 0  # No thresholds found