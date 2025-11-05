"""
Integration tests for the autoexec functionality with real database scenarios.

This module provides integration tests that simulate real-world database conditions
and test the complete autoexec workflow end-to-end.
"""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock
import tempfile
import json

from src.xmover.commands.maintenance import MaintenanceCommands, TableResetProcessor, TableResetState
from src.xmover.database import CrateDBClient


class MockCrateDBClient:
    """Mock CrateDB client that simulates realistic database behavior"""
    
    def __init__(self):
        self.query_log = []
        self.lease_check_count = {}
        self.replica_states = {}
        self.connection_failures = 0
        self.should_fail_operations = False
        
    def test_connection(self):
        return True
    
    def execute_query(self, sql, params=None):
        """Execute query with realistic simulation"""
        self.query_log.append({'sql': sql, 'params': params, 'timestamp': time.time()})
        
        # Simulate connection failures
        if self.connection_failures > 0:
            self.connection_failures -= 1
            raise Exception("Connection lost")
        
        # Simulate operation failures
        if self.should_fail_operations:
            return {'success': False, 'error': 'Simulated failure'}
        
        # Handle different query types
        if "SET" in sql and "number_of_replicas" in sql:
            return self._handle_replica_set(sql)
        elif "array_length(retention_leases" in sql:
            return self._handle_lease_check(sql)
        elif "SELECT" in sql and "information_schema" in sql:
            return self._handle_table_info_query(sql)
        else:
            return {'success': True, 'rows': []}
    
    def _handle_replica_set(self, sql):
        """Handle ALTER TABLE SET number_of_replicas commands"""
        # Extract table name and replica count
        if 'SET ("number_of_replicas" = 0)' in sql:
            # Setting to zero
            return {'success': True}
        elif 'SET ("number_of_replicas" =' in sql:
            # Restoring replicas
            return {'success': True}
        return {'success': True}
    
    def _handle_lease_check(self, sql):
        """Handle retention lease monitoring queries"""
        # Extract table identifier from SQL
        table_key = "default_table"  # Simplified for testing
        
        if table_key not in self.lease_check_count:
            self.lease_check_count[table_key] = 0
        
        self.lease_check_count[table_key] += 1
        
        # Simulate lease count decreasing over time
        if self.lease_check_count[table_key] <= 2:
            # First few checks: 2 leases (not cleared)
            return {'rows': [[2], [2]]}
        else:
            # Later checks: 1 lease per shard (cleared)
            return {'rows': [[1], [1]]}
    
    def _handle_table_info_query(self, sql):
        """Handle table metadata queries"""
        if "number_of_replicas" in sql:
            return {'rows': [['doc', 'test_table', 2]]}  # schema, table, replicas
        return {'rows': []}
    
    def reset_state(self):
        """Reset mock state for new test"""
        self.query_log.clear()
        self.lease_check_count.clear()
        self.replica_states.clear()
        self.connection_failures = 0
        self.should_fail_operations = False


@pytest.fixture
def mock_db_client():
    """Provide a fresh mock database client for each test"""
    client = MockCrateDBClient()
    yield client
    client.reset_state()


@pytest.fixture
def sample_problematic_tables():
    """Sample problematic tables data for testing"""
    return [
        {
            'schema_name': 'doc',
            'table_name': 'large_table',
            'partition_values': None,
            'partition_ident': None,
            'problematic_replica_shards': 2,
            'max_translog_uncommitted_mb': 1500.0,
            'total_primary_shards': 2,
            'total_replica_shards': 4,
            'current_replicas': 2
        },
        {
            'schema_name': 'analytics',
            'table_name': 'events',
            'partition_values': "(date='2024-01-01')",
            'partition_ident': 'partition_20240101',
            'problematic_replica_shards': 1,
            'max_translog_uncommitted_mb': 800.0,
            'total_primary_shards': 1,
            'total_replica_shards': 1,
            'current_replicas': 1
        },
        {
            'schema_name': 'logs',
            'table_name': 'application_logs',
            'partition_values': None,
            'partition_ident': None,
            'problematic_replica_shards': 3,
            'max_translog_uncommitted_mb': 2000.0,
            'total_primary_shards': 3,
            'total_replica_shards': 6,
            'current_replicas': 2
        }
    ]


class TestAutoexecRealWorldScenarios:
    """Test autoexec with realistic database scenarios"""
    
    def test_successful_single_table_reset(self, mock_db_client):
        """Test successful reset of a single problematic table"""
        table_info = {
            'schema_name': 'doc',
            'table_name': 'test_table',
            'partition_values': None,
            'partition_ident': None,
            'total_primary_shards': 1,
            'current_replicas': 2
        }
        
        processor = TableResetProcessor(table_info, mock_db_client, max_wait=30)
        
        with patch('time.sleep'):  # Speed up test
            result = processor.process()
        
        assert result is True
        assert processor.state == TableResetState.COMPLETED
        
        # Verify correct sequence of operations
        queries = [entry['sql'] for entry in mock_db_client.query_log]
        assert any('SET ("number_of_replicas" = 0)' in q for q in queries)
        assert any('retention_leases' in q for q in queries)
        assert any('SET ("number_of_replicas" = 2)' in q for q in queries)
    
    def test_successful_partitioned_table_reset(self, mock_db_client):
        """Test successful reset of a partitioned table"""
        table_info = {
            'schema_name': 'analytics',
            'table_name': 'events',
            'partition_values': "(date='2024-01-01')",
            'partition_ident': 'partition_20240101',
            'total_primary_shards': 1,
            'current_replicas': 1
        }
        
        processor = TableResetProcessor(table_info, mock_db_client, max_wait=30)
        
        with patch('time.sleep'):
            result = processor.process()
        
        assert result is True
        
        # Verify partition clause included in SQL
        queries = [entry['sql'] for entry in mock_db_client.query_log]
        partition_queries = [q for q in queries if "PARTITION (date='2024-01-01')" in q]
        assert len(partition_queries) >= 2  # Set to 0 and restore
    
    def test_connection_failure_during_operation(self, mock_db_client):
        """Test handling of connection failures"""
        mock_db_client.connection_failures = 2  # Fail first 2 operations
        
        table_info = {
            'schema_name': 'doc',
            'table_name': 'test_table',
            'partition_values': None,
            'partition_ident': None,
            'total_primary_shards': 1,
            'current_replicas': 2
        }
        
        processor = TableResetProcessor(table_info, mock_db_client, max_wait=30)
        result = processor.process()
        
        # Should fail due to connection issues
        assert result is False
        assert processor.state == TableResetState.FAILED
        assert "Connection lost" in processor.error_message
    
    def test_retention_lease_timeout(self, mock_db_client):
        """Test timeout when retention leases don't clear"""
        # Configure mock to never clear leases
        def always_return_leases(*args, **kwargs):
            if "retention_leases" in args[0]:
                return {'rows': [[2], [2]]}  # Always 2 leases
            return mock_db_client.execute_query(*args, **kwargs)
        
        mock_db_client.execute_query = always_return_leases
        
        table_info = {
            'schema_name': 'doc',
            'table_name': 'stuck_table',
            'partition_values': None,
            'partition_ident': None,
            'total_primary_shards': 1,
            'current_replicas': 2
        }
        
        processor = TableResetProcessor(table_info, mock_db_client, max_wait=20)
        
        with patch('time.sleep'):
            result = processor.process()
        
        assert result is False
        assert processor.state == TableResetState.FAILED
        assert "Timeout" in processor.error_message
    
    def test_dry_run_complete_workflow(self, mock_db_client):
        """Test complete workflow in dry run mode"""
        table_info = {
            'schema_name': 'doc',
            'table_name': 'test_table',
            'partition_values': None,
            'partition_ident': None,
            'total_primary_shards': 1,
            'current_replicas': 3
        }
        
        processor = TableResetProcessor(table_info, mock_db_client, dry_run=True, max_wait=30)
        result = processor.process()
        
        assert result is True
        assert processor.state == TableResetState.COMPLETED
        
        # Should not execute any actual database operations
        assert len(mock_db_client.query_log) == 0
    
    def test_multiple_tables_with_mixed_results(self, mock_db_client, sample_problematic_tables):
        """Test processing multiple tables with some failures"""
        maintenance = MaintenanceCommands(mock_db_client)
        
        # Configure first table to fail, others to succeed
        original_execute = mock_db_client.execute_query
        
        def selective_failure(sql, params=None):
            if "large_table" in sql and "number_of_replicas" in sql:
                return {'success': False, 'error': 'Table locked'}
            return original_execute(sql, params)
        
        mock_db_client.execute_query = selective_failure
        
        # Mock the filtering and replica count lookup
        maintenance._filter_tables_by_percentage = Mock(return_value=sample_problematic_tables[:2])
        maintenance._get_current_replica_count = Mock(side_effect=[2, 1])  # Return different values
        
        with patch('time.sleep'):
            result = maintenance._execute_autoexec(sample_problematic_tables[:2], False, 200, 60, "console")
        
        assert result is False  # Some failures
        assert maintenance._get_autoexec_exit_code() == 3  # Partial failure
    
    def test_percentage_filtering_logic(self, mock_db_client, sample_problematic_tables):
        """Test the percentage-based filtering logic"""
        maintenance = MaintenanceCommands(mock_db_client)
        maintenance._get_current_replica_count = Mock(return_value=2)
        
        # Test with 200% threshold (should include tables > 1024MB)
        filtered_200 = maintenance._filter_tables_by_percentage(sample_problematic_tables, 200)
        table_names_200 = [t['table_name'] for t in filtered_200]
        
        # large_table (1500MB) and application_logs (2000MB) should be included
        assert 'large_table' in table_names_200
        assert 'application_logs' in table_names_200
        # events (800MB) should not be included
        assert 'events' not in table_names_200
        
        # Test with 100% threshold (should include tables > 512MB)
        filtered_100 = maintenance._filter_tables_by_percentage(sample_problematic_tables, 100)
        assert len(filtered_100) == 3  # All tables should be included
    
    def test_json_logging_configuration(self, mock_db_client, sample_problematic_tables):
        """Test JSON logging configuration for container environments"""
        maintenance = MaintenanceCommands(mock_db_client)
        # Return tables so JSON logging configuration is triggered
        maintenance._filter_tables_by_percentage = Mock(return_value=sample_problematic_tables)
        
        with patch('src.xmover.commands.maintenance.logger') as mock_logger, \
             patch('src.xmover.commands.maintenance.TableResetProcessor') as mock_processor:
            
            # Mock processor to avoid actual processing
            mock_processor.return_value.process.return_value = True
            mock_processor.return_value.get_table_display_name.return_value = "test.table"
            
            result = maintenance._execute_autoexec(sample_problematic_tables, False, 200, 720, "json")
            
            # Should configure loguru for JSON logging
            mock_logger.remove.assert_called_once()
            mock_logger.add.assert_called_once()
            
            # Verify JSON serialization is enabled
            add_call_kwargs = mock_logger.add.call_args[1]
            assert add_call_kwargs.get('serialize') is True
    
    def test_exit_code_scenarios(self, mock_db_client):
        """Test different exit code scenarios"""
        maintenance = MaintenanceCommands(mock_db_client)
        
        # Test success scenario
        maintenance._filter_tables_by_percentage = Mock(return_value=[])
        result = maintenance._execute_autoexec([], False, 200, 720, "console")
        assert result is True
        assert maintenance._get_autoexec_exit_code() == 0
        
        # Test partial failure scenario
        sample_table = {
            'schema_name': 'doc',
            'table_name': 'test',
            'partition_values': None,
            'partition_ident': None,
            'total_primary_shards': 1,
            'current_replicas': 1
        }
        
        with patch('src.xmover.commands.maintenance.TableResetProcessor') as mock_processor:
            # First processor succeeds, second fails
            mock_instances = [Mock(), Mock()]
            mock_instances[0].process.return_value = True
            mock_instances[0].get_table_display_name.return_value = "doc.test1"
            mock_instances[1].process.return_value = False
            mock_instances[1].get_table_display_name.return_value = "doc.test2"
            mock_processor.side_effect = mock_instances
            
            maintenance._filter_tables_by_percentage = Mock(return_value=[sample_table, sample_table])
            
            result = maintenance._execute_autoexec([sample_table, sample_table], False, 200, 720, "console")
            assert result is False
            assert maintenance._get_autoexec_exit_code() == 3  # Partial failure


class TestAutoexecErrorRecovery:
    """Test error recovery and rollback scenarios"""
    
    def test_rollback_after_monitoring_failure(self, mock_db_client):
        """Test rollback when monitoring phase fails"""
        table_info = {
            'schema_name': 'doc',
            'table_name': 'test_table',
            'partition_values': None,
            'partition_ident': None,
            'total_primary_shards': 1,
            'current_replicas': 2
        }
        
        # Configure to fail during monitoring (leases never clear)
        original_execute = mock_db_client.execute_query
        def timeout_scenario(sql, params=None):
            if "retention_leases" in sql:
                return {'rows': [[2], [2]]}  # Never clears
            return original_execute(sql, params)
        
        mock_db_client.execute_query = timeout_scenario
        
        processor = TableResetProcessor(table_info, mock_db_client, max_wait=15)  # Short timeout
        
        with patch('time.sleep'):
            result = processor.process()
        
        assert result is False
        
        # Should have attempted rollback - verify restore replicas was called
        queries = [entry['sql'] for entry in mock_db_client.query_log]
        restore_queries = [q for q in queries if 'SET ("number_of_replicas" = 2)' in q]
        assert len(restore_queries) >= 1  # At least one restore attempt
    
    def test_critical_failure_during_restore(self, mock_db_client):
        """Test handling of critical failure during replica restoration"""
        table_info = {
            'schema_name': 'doc',
            'table_name': 'test_table',
            'partition_values': None,
            'partition_ident': None,
            'total_primary_shards': 1,
            'current_replicas': 2
        }
        
        # Configure to fail during restore phase
        original_execute = mock_db_client.execute_query
        def restore_failure(sql, params=None):
            if 'SET ("number_of_replicas" = 2)' in sql:
                return {'success': False, 'error': 'Table dropped during operation'}
            return original_execute(sql, params)
        
        mock_db_client.execute_query = restore_failure
        
        processor = TableResetProcessor(table_info, mock_db_client, max_wait=30)
        
        with patch('time.sleep'):
            result = processor.process()
        
        assert result is False
        assert processor.state == TableResetState.FAILED
        assert "CRITICAL" in processor.error_message
        assert "Table dropped" in processor.error_message
    
    def test_state_transitions_logged_correctly(self, mock_db_client):
        """Test that state transitions are logged in correct order"""
        table_info = {
            'schema_name': 'doc',
            'table_name': 'test_table',
            'partition_values': None,
            'partition_ident': None,
            'total_primary_shards': 1,
            'current_replicas': 1
        }
        
        processor = TableResetProcessor(table_info, mock_db_client, max_wait=30)
        
        # Capture log calls
        log_calls = []
        original_log_info = processor._log_info
        
        def capture_log(message):
            log_calls.append(message)
            original_log_info(message)
        
        processor._log_info = capture_log
        
        with patch('time.sleep'):
            result = processor.process()
        
        assert result is True
        
        # Verify state transition sequence
        transition_logs = [log for log in log_calls if '→' in log]
        assert len(transition_logs) >= 4  # At least 4 state transitions
        
        # Should see progression through states
        all_logs_text = ' '.join(log_calls)
        assert 'detected → setting_replicas_zero' in all_logs_text
        assert 'setting_replicas_zero → monitoring_leases' in all_logs_text
        assert 'monitoring_leases → restoring_replicas' in all_logs_text
        assert 'restoring_replicas → completed' in all_logs_text


class TestAutoexecPerformanceAndScalability:
    """Test performance characteristics and scalability"""
    
    def test_backoff_delay_calculation(self, mock_db_client):
        """Test that backoff delays are calculated correctly"""
        table_info = {
            'schema_name': 'doc',
            'table_name': 'test_table',
            'partition_values': None,
            'partition_ident': None,
            'total_primary_shards': 1,
            'current_replicas': 1
        }
        
        processor = TableResetProcessor(table_info, mock_db_client, max_wait=120)
        delays = processor._get_backoff_delays()
        
        # Verify delay progression
        assert delays[0] == 10  # First delay
        assert delays[1] == 15  # Second delay
        assert sum(delays) <= 120  # Total within max_wait
        assert len(delays) >= 3  # Multiple retry attempts
        
        # Test with very short timeout
        processor_short = TableResetProcessor(table_info, mock_db_client, max_wait=25)
        short_delays = processor_short._get_backoff_delays()
        assert sum(short_delays) <= 25
        assert all(delay > 0 for delay in short_delays)
    
    def test_large_number_of_tables(self, mock_db_client):
        """Test processing many tables efficiently"""
        # Create 20 problematic tables
        many_tables = []
        for i in range(20):
            many_tables.append({
                'schema_name': f'schema_{i}',
                'table_name': f'table_{i}',
                'partition_values': None,
                'partition_ident': None,
                'problematic_replica_shards': 1,
                'max_translog_uncommitted_mb': 1000.0,
                'total_primary_shards': 1,
                'total_replica_shards': 1,
                'current_replicas': 1
            })
        
        maintenance = MaintenanceCommands(mock_db_client)
        maintenance._filter_tables_by_percentage = Mock(return_value=many_tables)
        
        start_time = time.time()
        
        with patch('time.sleep'):  # Speed up test
            result = maintenance._execute_autoexec(many_tables, False, 200, 60, "console")
        
        end_time = time.time()
        
        assert result is True
        # Should process efficiently (< 1 second with mocked sleep)
        assert end_time - start_time < 1.0
        
        # Verify all tables were processed
        assert len([entry for entry in mock_db_client.query_log if "SET" in entry['sql']]) >= 40  # 2 per table
    
    def test_memory_usage_with_large_datasets(self, mock_db_client):
        """Test memory efficiency with large result sets"""
        # Simulate large query results
        def large_result_query(sql, params=None):
            if "retention_leases" in sql:
                # Return large result set
                return {'rows': [[1]] * 1000}  # 1000 shards with 1 lease each
            return mock_db_client.execute_query(sql, params)
        
        mock_db_client.execute_query = large_result_query
        
        table_info = {
            'schema_name': 'doc',
            'table_name': 'huge_table',
            'partition_values': None,
            'partition_ident': None,
            'total_primary_shards': 1000,
            'current_replicas': 1
        }
        
        processor = TableResetProcessor(table_info, mock_db_client, max_wait=30)
        
        with patch('time.sleep'):
            result = processor.process()
        
        assert result is True
        # Should handle large result sets without issues


if __name__ == '__main__':
    pytest.main([__file__, '-v'])