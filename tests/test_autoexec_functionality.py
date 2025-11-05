"""
Tests for the autoexec functionality in problematic-translogs command.

This module tests the state machine implementation for automatic replica reset operations,
including error handling, retry logic, and dry-run simulation.
"""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from enum import Enum

from xmover.commands.maintenance import TableResetProcessor, TableResetState, MaintenanceCommands
from xmover.database import CrateDBClient


class TestTableResetState:
    """Test the TableResetState enum"""
    
    def test_state_values(self):
        """Test that all states have correct string values"""
        assert TableResetState.DETECTED.value == "detected"
        assert TableResetState.SETTING_REPLICAS_ZERO.value == "setting_replicas_zero"
        assert TableResetState.MONITORING_LEASES.value == "monitoring_leases"
        assert TableResetState.RESTORING_REPLICAS.value == "restoring_replicas"
        assert TableResetState.COMPLETED.value == "completed"
        assert TableResetState.FAILED.value == "failed"


class TestTableResetProcessor:
    """Test the TableResetProcessor state machine"""
    
    @pytest.fixture
    def mock_client(self):
        """Mock CrateDB client"""
        client = Mock(spec=CrateDBClient)
        client.execute_query.return_value = {'success': True, 'rows': []}
        return client
    
    @pytest.fixture
    def sample_table_info(self):
        """Sample table info for testing"""
        return {
            'schema_name': 'test_schema',
            'table_name': 'test_table',
            'partition_values': None,
            'partition_ident': None,
            'current_replicas': 2,
            'total_primary_shards': 1,
            'max_translog_uncommitted_mb': 1000.0
        }
    
    @pytest.fixture
    def partitioned_table_info(self):
        """Sample partitioned table info for testing"""
        return {
            'schema_name': 'test_schema',
            'table_name': 'partitioned_table',
            'partition_values': "(date='2024-01-01')",
            'partition_ident': '04732cpp6osj0d1g64o30c1g64o32c9i64o34dpj64o38e9p6tgjchhpcdpkae9p6tgjchhr6dr74dhkdto0',
            'current_replicas': 3,
            'total_primary_shards': 2,
            'max_translog_uncommitted_mb': 800.0
        }
    
    def test_processor_initialization(self, mock_client, sample_table_info):
        """Test processor initialization with correct defaults"""
        processor = TableResetProcessor(sample_table_info, mock_client)
        
        assert processor.state == TableResetState.DETECTED
        assert processor.schema_name == 'test_schema'
        assert processor.table_name == 'test_table'
        assert processor.original_replicas == 2
        assert processor.dry_run is False
        assert processor.max_wait == 720
        assert processor.error_message is None
    
    def test_get_table_display_name_regular_table(self, mock_client, sample_table_info):
        """Test display name for regular table"""
        processor = TableResetProcessor(sample_table_info, mock_client)
        assert processor.get_table_display_name() == "test_schema.test_table"
    
    def test_get_table_display_name_partitioned_table(self, mock_client, partitioned_table_info):
        """Test display name for partitioned table"""
        processor = TableResetProcessor(partitioned_table_info, mock_client)
        expected = "test_schema.partitioned_table PARTITION (date='2024-01-01')"
        assert processor.get_table_display_name() == expected
    
    def test_get_backoff_delays_default(self, mock_client, sample_table_info):
        """Test backoff delay generation with default max_wait"""
        processor = TableResetProcessor(sample_table_info, mock_client, max_wait=720)
        delays = processor._get_backoff_delays()
        
        # Should start with 10, 15, 30, 45...
        assert delays[0] == 10
        assert delays[1] == 15
        assert delays[2] == 30
        assert delays[3] == 45
        
        # Total should not exceed max_wait
        assert sum(delays) <= 720
        
        # Should have reasonable progression
        assert len(delays) >= 5  # At least a few retry attempts
    
    def test_get_backoff_delays_short_timeout(self, mock_client, sample_table_info):
        """Test backoff delays with short timeout"""
        processor = TableResetProcessor(sample_table_info, mock_client, max_wait=60)
        delays = processor._get_backoff_delays()
        
        assert sum(delays) <= 60
        assert delays[0] == 10
        assert all(delay > 0 for delay in delays)
    
    def test_set_replicas_to_zero_success_regular_table(self, mock_client, sample_table_info):
        """Test setting replicas to zero for regular table"""
        mock_client.execute_query.return_value = {'success': True}
        
        processor = TableResetProcessor(sample_table_info, mock_client)
        result = processor._set_replicas_to_zero()
        
        assert result is True
        assert processor.state == TableResetState.SETTING_REPLICAS_ZERO
        
        # Verify correct SQL was called
        mock_client.execute_query.assert_called_once()
        sql_call = mock_client.execute_query.call_args[0][0]
        assert 'ALTER TABLE "test_schema"."test_table"' in sql_call
        assert 'SET ("number_of_replicas" = 0)' in sql_call
        assert 'PARTITION' not in sql_call
    
    def test_set_replicas_to_zero_success_partitioned_table(self, mock_client, partitioned_table_info):
        """Test setting replicas to zero for partitioned table"""
        mock_client.execute_query.return_value = {'success': True}
        
        processor = TableResetProcessor(partitioned_table_info, mock_client)
        result = processor._set_replicas_to_zero()
        
        assert result is True
        
        # Verify partition clause included
        sql_call = mock_client.execute_query.call_args[0][0]
        assert 'PARTITION (date=\'2024-01-01\')' in sql_call
    
    def test_set_replicas_to_zero_failure(self, mock_client, sample_table_info):
        """Test handling failure when setting replicas to zero"""
        mock_client.execute_query.return_value = {'success': False, 'error': 'Permission denied'}
        
        processor = TableResetProcessor(sample_table_info, mock_client)
        result = processor._set_replicas_to_zero()
        
        assert result is False
        assert processor.state == TableResetState.FAILED
        assert 'Permission denied' in processor.error_message
    
    def test_set_replicas_to_zero_dry_run(self, mock_client, sample_table_info):
        """Test dry run mode for setting replicas"""
        processor = TableResetProcessor(sample_table_info, mock_client, dry_run=True)
        result = processor._set_replicas_to_zero()
        
        assert result is True
        assert processor.state == TableResetState.SETTING_REPLICAS_ZERO
        # Should not execute actual SQL in dry run
        mock_client.execute_query.assert_not_called()
    
    def test_check_retention_leases_regular_table(self, mock_client, sample_table_info):
        """Test retention lease checking for regular table"""
        # Mock lease count response
        mock_client.execute_query.return_value = {'rows': [[2], [1]]}  # Two shards with different lease counts
        
        processor = TableResetProcessor(sample_table_info, mock_client)
        lease_count = processor._check_retention_leases()
        
        # Should return maximum lease count
        assert lease_count == 2
        
        # Verify correct SQL was called
        sql_call = mock_client.execute_query.call_args[0][0]
        assert 'array_length(retention_leases[\'leases\'], 1)' in sql_call
        assert 'test_table' in sql_call
        assert 'test_schema' in sql_call
        assert 'partition_ident' not in sql_call
    
    def test_check_retention_leases_partitioned_table(self, mock_client, partitioned_table_info):
        """Test retention lease checking for partitioned table"""
        mock_client.execute_query.return_value = {'rows': [[1]]}
        
        processor = TableResetProcessor(partitioned_table_info, mock_client)
        lease_count = processor._check_retention_leases()
        
        assert lease_count == 1
        
        # Verify partition_ident clause included
        sql_call = mock_client.execute_query.call_args[0][0]
        assert 'partition_ident' in sql_call
    
    def test_check_retention_leases_error(self, mock_client, sample_table_info):
        """Test error handling in lease checking"""
        mock_client.execute_query.side_effect = Exception("Connection lost")
        
        processor = TableResetProcessor(sample_table_info, mock_client)
        lease_count = processor._check_retention_leases()
        
        assert lease_count == -1  # Error condition
    
    @patch('time.sleep')  # Mock sleep to speed up tests
    def test_monitor_retention_leases_success(self, mock_sleep, mock_client, sample_table_info):
        """Test successful lease monitoring"""
        # Simulate lease count decreasing over time
        mock_client.execute_query.side_effect = [
            {'rows': [[2]]},  # First check: 2 leases
            {'rows': [[1]]},  # Second check: 1 lease (success!)
        ]
        
        processor = TableResetProcessor(sample_table_info, mock_client, max_wait=60)
        result = processor._monitor_retention_leases()
        
        assert result is True
        assert processor.state == TableResetState.MONITORING_LEASES
        assert mock_client.execute_query.call_count == 2
    
    @patch('time.sleep')
    def test_monitor_retention_leases_timeout(self, mock_sleep, mock_client, sample_table_info):
        """Test lease monitoring timeout"""
        # Always return 2 leases (never clears)
        mock_client.execute_query.return_value = {'rows': [[2]]}
        
        processor = TableResetProcessor(sample_table_info, mock_client, max_wait=30)  # Short timeout
        result = processor._monitor_retention_leases()
        
        assert result is False
        assert processor.state == TableResetState.FAILED
        assert 'Timeout' in processor.error_message or 'Exceeded maximum wait time' in processor.error_message
    
    def test_monitor_retention_leases_dry_run(self, mock_client, sample_table_info):
        """Test lease monitoring in dry run mode"""
        processor = TableResetProcessor(sample_table_info, mock_client, dry_run=True)
        result = processor._monitor_retention_leases()
        
        assert result is True
        # Should not execute actual queries in dry run
        mock_client.execute_query.assert_not_called()
    
    def test_restore_replicas_success(self, mock_client, sample_table_info):
        """Test successful replica restoration"""
        mock_client.execute_query.return_value = {'success': True}
        
        processor = TableResetProcessor(sample_table_info, mock_client)
        result = processor._restore_replicas()
        
        assert result is True
        assert processor.state == TableResetState.RESTORING_REPLICAS
        
        # Verify correct SQL with original replica count
        sql_call = mock_client.execute_query.call_args[0][0]
        assert 'SET ("number_of_replicas" = 2)' in sql_call
    
    def test_restore_replicas_failure(self, mock_client, sample_table_info):
        """Test failure during replica restoration"""
        mock_client.execute_query.return_value = {'success': False, 'error': 'Table not found'}
        
        processor = TableResetProcessor(sample_table_info, mock_client)
        result = processor._restore_replicas()
        
        assert result is False
        assert processor.state == TableResetState.FAILED
        assert 'CRITICAL' in processor.error_message
        assert 'Table not found' in processor.error_message
    
    def test_restore_replicas_dry_run(self, mock_client, sample_table_info):
        """Test replica restoration in dry run mode"""
        processor = TableResetProcessor(sample_table_info, mock_client, dry_run=True)
        result = processor._restore_replicas()
        
        assert result is True
        mock_client.execute_query.assert_not_called()
    
    @patch('time.sleep')
    def test_full_process_success(self, mock_sleep, mock_client, sample_table_info):
        """Test full successful processing workflow"""
        # Mock successful operations
        mock_client.execute_query.side_effect = [
            {'success': True},     # Set replicas to 0
            {'rows': [[1]]},       # Check leases - success immediately
            {'success': True},     # Restore replicas
        ]
        
        processor = TableResetProcessor(sample_table_info, mock_client)
        result = processor.process()
        
        assert result is True
        assert processor.state == TableResetState.COMPLETED
        assert processor.error_message is None
    
    @patch('time.sleep')
    def test_full_process_failure_in_set_replicas(self, mock_sleep, mock_client, sample_table_info):
        """Test failure during replica setting phase"""
        mock_client.execute_query.return_value = {'success': False, 'error': 'Access denied'}
        
        processor = TableResetProcessor(sample_table_info, mock_client)
        result = processor.process()
        
        assert result is False
        assert processor.state == TableResetState.FAILED
        assert 'Access denied' in processor.error_message
    
    @patch('time.sleep')
    def test_full_process_timeout_in_monitoring(self, mock_sleep, mock_client, sample_table_info):
        """Test timeout during monitoring phase"""
        mock_client.execute_query.side_effect = [
            {'success': True},     # Set replicas to 0 - success
            {'rows': [[2]]},       # Check leases - always 2 (timeout)
            {'rows': [[2]]},
            {'rows': [[2]]},
            {'rows': [[2]]},
            {'rows': [[2]]},
        ]
        
        processor = TableResetProcessor(sample_table_info, mock_client, max_wait=20)  # Short timeout
        result = processor.process()
        
        assert result is False
        assert processor.state == TableResetState.FAILED
    
    def test_attempt_rollback_success(self, mock_client, sample_table_info):
        """Test successful rollback operation"""
        mock_client.execute_query.return_value = {'success': True}
        
        processor = TableResetProcessor(sample_table_info, mock_client)
        processor.state = TableResetState.MONITORING_LEASES  # Simulate failure during monitoring
        
        processor._attempt_rollback()
        
        # Should attempt to restore original replicas
        sql_call = mock_client.execute_query.call_args[0][0]
        assert 'SET ("number_of_replicas" = 2)' in sql_call
    
    def test_attempt_rollback_failure(self, mock_client, sample_table_info):
        """Test rollback failure requiring manual intervention"""
        mock_client.execute_query.return_value = {'success': False, 'error': 'Rollback failed'}
        
        processor = TableResetProcessor(sample_table_info, mock_client)
        processor.state = TableResetState.MONITORING_LEASES
        
        processor._attempt_rollback()
        
        # Should log critical error - we can't easily test logging here
        # but the method should not raise an exception
    
    def test_attempt_rollback_dry_run(self, mock_client, sample_table_info):
        """Test rollback in dry run mode"""
        processor = TableResetProcessor(sample_table_info, mock_client, dry_run=True)
        processor.state = TableResetState.MONITORING_LEASES
        
        processor._attempt_rollback()
        
        # Should not execute actual SQL in dry run
        mock_client.execute_query.assert_not_called()


class TestMaintenanceCommandsAutoexec:
    """Test the autoexec functionality in MaintenanceCommands"""
    
    @pytest.fixture
    def mock_client(self):
        """Mock CrateDB client"""
        client = Mock(spec=CrateDBClient)
        client.test_connection.return_value = True
        return client
    
    @pytest.fixture
    def maintenance_commands(self, mock_client):
        """MaintenanceCommands instance with mock client"""
        return MaintenanceCommands(mock_client)
    
    @pytest.fixture
    def sample_summary_rows(self):
        """Sample problematic tables summary"""
        return [
            {
                'schema_name': 'doc',
                'table_name': 'table1',
                'partition_values': None,
                'partition_ident': None,
                'problematic_replica_shards': 2,
                'max_translog_uncommitted_mb': 1200.0,
                'total_primary_shards': 1,
                'total_replica_shards': 2,
                'current_replicas': 2
            },
            {
                'schema_name': 'doc',
                'table_name': 'table2',
                'partition_values': "(date='2024-01-01')",
                'partition_ident': 'partition123',
                'problematic_replica_shards': 1,
                'max_translog_uncommitted_mb': 900.0,
                'total_primary_shards': 2,
                'total_replica_shards': 2,
                'current_replicas': 1
            }
        ]
    
    def test_filter_tables_by_percentage_default(self, maintenance_commands, sample_summary_rows):
        """Test filtering tables by percentage threshold"""
        # Mock the _get_current_replica_count method
        maintenance_commands._get_current_replica_count = Mock(return_value=2)
        
        filtered = maintenance_commands._filter_tables_by_percentage(sample_summary_rows, 200)
        
        # Only table1 (1200MB) should exceed 200% of 563MB threshold (1126MB)
        # table2 (900MB) is only 159% of threshold
        assert len(filtered) == 1
        assert all('current_replicas' in table for table in filtered)
        assert filtered[0]['table_name'] == 'table1'
    
    def test_filter_tables_by_percentage_high_threshold(self, maintenance_commands, sample_summary_rows):
        """Test filtering with high percentage threshold"""
        maintenance_commands._get_current_replica_count = Mock(return_value=2)
        
        filtered = maintenance_commands._filter_tables_by_percentage(sample_summary_rows, 400)
        
        # No tables should exceed 400% of 563MB (2252MB)
        assert len(filtered) == 0
    
    def test_filter_tables_by_percentage_low_threshold(self, maintenance_commands, sample_summary_rows):
        """Test filtering with low percentage threshold"""
        maintenance_commands._get_current_replica_count = Mock(return_value=2)
        
        filtered = maintenance_commands._filter_tables_by_percentage(sample_summary_rows, 100)
        
        # Both tables should exceed 100% of 563MB threshold
        assert len(filtered) == 2
    
    @patch('xmover.commands.maintenance.TableResetProcessor')
    def test_execute_autoexec_success(self, mock_processor_class, maintenance_commands, sample_summary_rows):
        """Test successful autoexec execution"""
        # Mock processor instances
        mock_processor = Mock()
        mock_processor.process.return_value = True
        mock_processor.get_table_display_name.return_value = "doc.table1"
        mock_processor_class.return_value = mock_processor
        
        # Mock filtering
        maintenance_commands._filter_tables_by_percentage = Mock(return_value=sample_summary_rows)
        
        result = maintenance_commands._execute_autoexec(sample_summary_rows, False, 200, 720, "console")
        
        assert result is True
        assert mock_processor_class.call_count == len(sample_summary_rows)
        assert mock_processor.process.call_count == len(sample_summary_rows)
    
    @patch('xmover.commands.maintenance.TableResetProcessor')
    def test_execute_autoexec_partial_failure(self, mock_processor_class, maintenance_commands, sample_summary_rows):
        """Test autoexec with some failures"""
        # Mock processor - first succeeds, second fails
        def create_mock_processor(table_info, *args, **kwargs):
            mock_processor = Mock()
            if 'table1' in str(table_info):
                mock_processor.process.return_value = True
            else:
                mock_processor.process.return_value = False
            mock_processor.get_table_display_name.return_value = f"doc.{table_info.get('table_name', 'unknown')}"
            return mock_processor
        
        mock_processor_class.side_effect = create_mock_processor
        maintenance_commands._filter_tables_by_percentage = Mock(return_value=sample_summary_rows)
        
        result = maintenance_commands._execute_autoexec(sample_summary_rows, False, 200, 720, "console")
        
        assert result is False
        assert maintenance_commands._get_autoexec_exit_code() == 3  # Partial failure
    
    @patch('xmover.commands.maintenance.TableResetProcessor')
    def test_execute_autoexec_complete_failure(self, mock_processor_class, maintenance_commands, sample_summary_rows):
        """Test autoexec with complete failure"""
        # Mock processor - all fail
        mock_processor = Mock()
        mock_processor.process.return_value = False
        mock_processor.get_table_display_name.return_value = "doc.table"
        mock_processor_class.return_value = mock_processor
        
        maintenance_commands._filter_tables_by_percentage = Mock(return_value=sample_summary_rows)
        
        result = maintenance_commands._execute_autoexec(sample_summary_rows, False, 200, 720, "console")
        
        assert result is False
        assert maintenance_commands._get_autoexec_exit_code() == 2  # Complete failure
    
    def test_execute_autoexec_no_tables_to_process(self, maintenance_commands, sample_summary_rows):
        """Test autoexec when no tables exceed percentage threshold"""
        # Mock filtering to return no tables
        maintenance_commands._filter_tables_by_percentage = Mock(return_value=[])
        
        result = maintenance_commands._execute_autoexec(sample_summary_rows, False, 500, 720, "console")
        
        assert result is True  # Success when nothing to process
        assert maintenance_commands._get_autoexec_exit_code() == 0
    
    @patch('xmover.commands.maintenance.logger')
    def test_execute_autoexec_json_logging(self, mock_logger, maintenance_commands, sample_summary_rows):
        """Test autoexec with JSON logging format"""
        # Use tables that will pass the filter to ensure logging setup happens
        maintenance_commands._filter_tables_by_percentage = Mock(return_value=sample_summary_rows)
        
        with patch('xmover.commands.maintenance.TableResetProcessor') as mock_processor:
            mock_processor.return_value.process.return_value = True
            mock_processor.return_value.get_table_display_name.return_value = "test.table"
            
            maintenance_commands._execute_autoexec(sample_summary_rows, True, 200, 720, "json")
        
        # Should configure loguru for JSON format
        mock_logger.remove.assert_called_once()
        mock_logger.add.assert_called_once()
    
    def test_get_autoexec_exit_code_default(self, maintenance_commands):
        """Test default exit code"""
        assert maintenance_commands._get_autoexec_exit_code() == 1
    
    def test_get_autoexec_exit_code_set(self, maintenance_commands):
        """Test explicitly set exit code"""
        maintenance_commands._autoexec_exit_code = 3
        assert maintenance_commands._get_autoexec_exit_code() == 3


class TestAutoexecIntegration:
    """Integration tests for the complete autoexec workflow"""
    
    @pytest.fixture
    def mock_client_with_realistic_responses(self):
        """Mock client with realistic response patterns"""
        client = Mock(spec=CrateDBClient)
        client.test_connection.return_value = True
        
        # Simulate realistic query responses
        def execute_query_side_effect(sql, params=None):
            if "number_of_replicas" in sql and "SET" in sql:
                return {'success': True}
            elif "array_length(retention_leases" in sql:
                # Simulate lease count decreasing over time
                if not hasattr(client, '_lease_call_count'):
                    client._lease_call_count = 0
                client._lease_call_count += 1
                
                # First few calls return 2, then 1 (cleared)
                if client._lease_call_count <= 2:
                    return {'rows': [[2]]}
                else:
                    return {'rows': [[1]]}
            else:
                return {'success': True, 'rows': []}
        
        client.execute_query.side_effect = execute_query_side_effect
        return client
    
    def test_complete_autoexec_workflow(self, mock_client_with_realistic_responses):
        """Test complete autoexec workflow from detection to completion"""
        # Sample problematic translogs data
        individual_shards = [
            {
                'schema_name': 'doc',
                'table_name': 'test_table',
                'partition_values': None,
                'shard_id': 0,
                'node_name': 'data-hot-1',
                'translog_size_mb': 1200.0
            }
        ]
        
        summary_rows = [
            {
                'schema_name': 'doc',
                'table_name': 'test_table',
                'partition_values': None,
                'partition_ident': None,
                'problematic_replica_shards': 1,
                'max_translog_uncommitted_mb': 1200.0,
                'total_primary_shards': 1,
                'total_replica_shards': 1,
                'current_replicas': 1
            }
        ]
        
        # Create maintenance commands instance
        maintenance = MaintenanceCommands(mock_client_with_realistic_responses)
        
        # Mock the problematic translogs detection
        maintenance._get_problematic_translogs = Mock(return_value=(individual_shards, summary_rows))
        maintenance._get_current_replica_count = Mock(return_value=1)
        
        # Execute autoexec
        with patch('time.sleep'):  # Speed up the test
            result = maintenance._execute_autoexec(summary_rows, False, 200, 60, "console")
        
        assert result is True
        # Verify SQL operations were called
        assert mock_client_with_realistic_responses.execute_query.call_count >= 3  # Set 0, monitor, restore
    
    def test_complete_dry_run_workflow(self, mock_client_with_realistic_responses):
        """Test complete dry run workflow"""
        summary_rows = [
            {
                'schema_name': 'doc',
                'table_name': 'test_table',
                'partition_values': None,
                'partition_ident': None,
                'problematic_replica_shards': 1,
                'max_translog_uncommitted_mb': 800.0,
                'total_primary_shards': 1,
                'total_replica_shards': 1,
                'current_replicas': 1
            }
        ]
        
        maintenance = MaintenanceCommands(mock_client_with_realistic_responses)
        maintenance._filter_tables_by_percentage = Mock(return_value=summary_rows)
        
        # Execute dry run
        result = maintenance._execute_autoexec(summary_rows, True, 200, 60, "console")  # dry_run=True
        
        assert result is True
        # In dry run, should not execute actual database operations
        mock_client_with_realistic_responses.execute_query.assert_not_called()


if __name__ == '__main__':
    pytest.main([__file__])