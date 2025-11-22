"""
Tests for AutoExec functionality - automatic replica reset for problematic translogs.

This module tests the core business scenarios for the --autoexec feature:
- Automatic replica reset workflow (set to 0, monitor leases, restore)
- Dry-run simulation without actual database changes
- Percentage-based filtering using adaptive thresholds
- CLI flag validation and mutual exclusivity
- Error handling and rollback scenarios
"""

import pytest
from unittest.mock import Mock, patch
from click.testing import CliRunner

from cratedb_xlens.commands.maintenance import MaintenanceCommands
from cratedb_xlens.commands.maintenance.problematic_translogs.autoexec import TableResetProcessor, TableResetState
from cratedb_xlens.database import CrateDBClient
from cratedb_xlens.cli import main


# ============================================================================
# Fixtures
# ============================================================================

@pytest.fixture
def mock_client():
    """Mock CrateDB client with realistic query responses"""
    client = Mock(spec=CrateDBClient)
    client.test_connection.return_value = True

    # Default successful responses
    def execute_query(sql, params=None):
        if "SET" in sql and "number_of_replicas" in sql:
            # ALTER TABLE commands succeed
            return {'success': True}
        elif "array_length(retention_leases" in sql:
            # Lease monitoring - return 1 lease (cleared)
            return {'rows': [[1]]}
        elif "information_schema.tables" in sql and "flush_threshold_size" in sql:
            # Return default 512MB threshold
            return {'rows': [['doc', 'test_table', 536870912]]}
        else:
            return {'success': True, 'rows': []}

    client.execute_query.side_effect = execute_query
    return client


@pytest.fixture
def sample_table():
    """Basic table info for testing"""
    return {
        'schema_name': 'doc',
        'table_name': 'test_table',
        'partition_values': None,
        'partition_ident': None,
        'current_replicas': 2,
        'total_primary_shards': 1,
        'max_translog_uncommitted_mb': 1200.0
    }


@pytest.fixture
def partitioned_table():
    """Partitioned table info for testing"""
    return {
        'schema_name': 'analytics',
        'table_name': 'events',
        'partition_values': "(date='2024-01-01')",
        'partition_ident': 'partition_20240101',
        'current_replicas': 1,
        'total_primary_shards': 1,
        'max_translog_uncommitted_mb': 800.0
    }


# ============================================================================
# Core Replica Reset Workflow Tests
# ============================================================================

class TestReplicaResetWorkflow:
    """Test the core replica reset workflow for the autoexec feature"""

    @patch('time.sleep')
    def test_successful_replica_reset_regular_table(self, mock_sleep, mock_client, sample_table):
        """Test successful replica reset for regular (non-partitioned) table"""
        processor = TableResetProcessor(sample_table, mock_client, max_wait=60)
        result = processor.process()

        assert result is True
        assert processor.state == TableResetState.COMPLETED
        assert processor.error_message is None

        # Verify SQL operations were called
        calls = [call[0][0] for call in mock_client.execute_query.call_args_list]

        # Should set replicas to 0
        assert any('SET ("number_of_replicas" = 0)' in sql for sql in calls)
        # Should monitor retention leases
        assert any('retention_leases' in sql for sql in calls)
        # Should restore original replica count
        assert any('SET ("number_of_replicas" = 2)' in sql for sql in calls)

    @patch('time.sleep')
    def test_successful_replica_reset_partitioned_table(self, mock_sleep, mock_client, partitioned_table):
        """Test successful replica reset for partitioned table"""
        processor = TableResetProcessor(partitioned_table, mock_client, max_wait=60)
        result = processor.process()

        assert result is True
        assert processor.state == TableResetState.COMPLETED

        # Verify partition clause is included in SQL
        calls = [call[0][0] for call in mock_client.execute_query.call_args_list]
        partition_sqls = [sql for sql in calls if "PARTITION (date='2024-01-01')" in sql]

        # Should have partition clause in both set-to-0 and restore commands
        assert len(partition_sqls) >= 2

    @patch('time.sleep')
    def test_replica_reset_timeout_results_in_failure(self, mock_sleep, mock_client, sample_table):
        """Test that timeout during lease monitoring results in failure"""
        # Configure client to never clear leases (timeout scenario)
        def timeout_query(sql, params=None):
            if "retention_leases" in sql:
                return {'rows': [[2]]}  # Always 2 leases - never clears
            elif "SET" in sql and "number_of_replicas" in sql:
                return {'success': True}
            return {'success': True, 'rows': []}

        mock_client.execute_query.side_effect = timeout_query

        processor = TableResetProcessor(sample_table, mock_client, max_wait=20)
        result = processor.process()

        assert result is False
        assert processor.state == TableResetState.FAILED
        assert 'Timeout' in processor.error_message or 'retention leases not cleared' in processor.error_message

    @patch('time.sleep')
    def test_failure_during_set_replicas_to_zero(self, mock_sleep, mock_client, sample_table):
        """Test handling of failure when setting replicas to zero"""
        def failing_query(sql, params=None):
            if 'SET ("number_of_replicas" = 0)' in sql:
                return {'success': False, 'error': 'Permission denied'}
            return {'success': True, 'rows': []}

        mock_client.execute_query.side_effect = failing_query

        processor = TableResetProcessor(sample_table, mock_client)
        result = processor.process()

        assert result is False
        assert processor.state == TableResetState.FAILED
        assert 'Permission denied' in processor.error_message

    @patch('time.sleep')
    def test_critical_failure_during_replica_restore(self, mock_sleep, mock_client, sample_table):
        """Test critical failure during replica restoration phase"""
        def restore_fails(sql, params=None):
            if 'SET ("number_of_replicas" = 2)' in sql:
                return {'success': False, 'error': 'Table no longer exists'}
            elif "retention_leases" in sql:
                return {'rows': [[1]]}
            return {'success': True}

        mock_client.execute_query.side_effect = restore_fails

        processor = TableResetProcessor(sample_table, mock_client, max_wait=30)
        result = processor.process()

        assert result is False
        assert processor.state == TableResetState.FAILED
        assert 'CRITICAL' in processor.error_message


# ============================================================================
# Dry-Run Tests
# ============================================================================

class TestDryRunMode:
    """Test dry-run simulation mode that doesn't modify database"""

    def test_dry_run_completes_without_database_calls(self, mock_client, sample_table):
        """Test that dry-run mode simulates workflow without actual database operations"""
        processor = TableResetProcessor(sample_table, mock_client, dry_run=True, max_wait=60)
        result = processor.process()

        assert result is True
        assert processor.state == TableResetState.COMPLETED

        # Should not execute any database operations in dry-run mode
        mock_client.execute_query.assert_not_called()

    def test_dry_run_with_partitioned_table(self, mock_client, partitioned_table):
        """Test dry-run mode works with partitioned tables"""
        processor = TableResetProcessor(partitioned_table, mock_client, dry_run=True)
        result = processor.process()

        assert result is True
        assert processor.state == TableResetState.COMPLETED
        mock_client.execute_query.assert_not_called()


# ============================================================================
# Percentage Filtering & Adaptive Thresholds Tests
# ============================================================================

class TestPercentageFilteringWithAdaptiveThresholds:
    """Test percentage-based filtering using adaptive table thresholds"""

    def test_filter_uses_adaptive_thresholds_not_hardcoded(self, mock_client):
        """Test that filtering uses actual table thresholds, not hardcoded 563MB"""
        maintenance = MaintenanceCommands(mock_client)

        # Mock tables with adaptive thresholds already applied
        tables_with_adaptive = [
            {
                'schema_name': 'doc',
                'table_name': 'large_config_table',
                'max_translog_uncommitted_mb': 2048.0,
                'adaptive_threshold_mb': 1126.4,  # 1GB + 10% buffer
                'adaptive_config_mb': 1024.0,
                'current_replicas': 2
            },
            {
                'schema_name': 'analytics',
                'table_name': 'small_config_table',
                'max_translog_uncommitted_mb': 512.0,
                'adaptive_threshold_mb': 281.6,   # 256MB + 10% buffer
                'adaptive_config_mb': 256.0,
                'current_replicas': 1
            },
            {
                'schema_name': 'logs',
                'table_name': 'default_table',
                'max_translog_uncommitted_mb': 600.0,
                'adaptive_threshold_mb': 563.2,   # 512MB default + 10%
                'adaptive_config_mb': 512.0,
                'current_replicas': 1
            }
        ]

        # Test with 150% threshold
        filtered = maintenance._filter_tables_by_percentage(tables_with_adaptive, 150)

        # Calculate expected percentages using adaptive thresholds:
        # large_config: 2048 / 1126.4 = 181.8% (should be included)
        # small_config: 512 / 281.6 = 181.8% (should be included)
        # default: 600 / 563.2 = 106.5% (should NOT be included)

        assert len(filtered) == 2
        table_names = [t['table_name'] for t in filtered]
        assert 'large_config_table' in table_names
        assert 'small_config_table' in table_names
        assert 'default_table' not in table_names

    def test_adaptive_thresholds_fetched_from_information_schema(self, mock_client):
        """Test that adaptive thresholds are fetched from information_schema.tables"""
        maintenance = MaintenanceCommands(mock_client)

        # Configure mock to return specific flush thresholds
        def threshold_query(sql, params=None):
            if "information_schema.tables" in sql and "flush_threshold_size" in sql:
                return {
                    'rows': [
                        ['doc', 'events', 1073741824],  # 1GB
                        ['analytics', 'metrics', 268435456],  # 256MB
                    ]
                }
            return {'rows': []}

        mock_client.execute_query.side_effect = threshold_query

        mock_shards = [
            {'schema_name': 'doc', 'table_name': 'events', 'partition_values': ''},
            {'schema_name': 'analytics', 'table_name': 'metrics', 'partition_values': ''},
        ]

        thresholds = maintenance._get_table_flush_thresholds(mock_shards)

        # Verify thresholds were calculated with 10% buffer
        assert thresholds['doc.events']['config_mb'] == 1024.0
        assert thresholds['doc.events']['threshold_mb'] == pytest.approx(1126.4, rel=1e-3)

        assert thresholds['analytics.metrics']['config_mb'] == 256.0
        assert thresholds['analytics.metrics']['threshold_mb'] == pytest.approx(281.6, rel=1e-3)


# ============================================================================
# Multiple Tables & Partial Failure Tests
# ============================================================================

class TestMultipleTableProcessing:
    """Test processing multiple tables with mixed success/failure scenarios"""

    @patch('cratedb_xlens.commands.maintenance.TableResetProcessor')
    def test_partial_failure_returns_correct_exit_code(self, mock_processor_class, mock_client):
        """Test that partial failures (some succeed, some fail) return exit code 3"""
        # Create mock processors - first succeeds, second fails
        mock_processor1 = Mock()
        mock_processor1.process.return_value = True
        mock_processor1.get_table_display_name.return_value = "doc.table1"

        mock_processor2 = Mock()
        mock_processor2.process.return_value = False
        mock_processor2.get_table_display_name.return_value = "doc.table2"

        mock_processor_class.side_effect = [mock_processor1, mock_processor2]

        maintenance = MaintenanceCommands(mock_client)

        tables = [
            {'schema_name': 'doc', 'table_name': 'table1', 'current_replicas': 2,
             'adaptive_threshold_mb': 563.2, 'max_translog_uncommitted_mb': 1200.0},
            {'schema_name': 'doc', 'table_name': 'table2', 'current_replicas': 2,
             'adaptive_threshold_mb': 563.2, 'max_translog_uncommitted_mb': 1000.0}
        ]

        # Mock _filter_tables_by_percentage to return all tables
        with patch.object(maintenance, '_filter_tables_by_percentage', return_value=tables):
            result = maintenance._execute_autoexec(tables, False, 200, 720, "console")

        assert result is False
        assert maintenance._get_autoexec_exit_code() == 3  # Partial failure

    @patch('cratedb_xlens.commands.maintenance.TableResetProcessor')
    def test_complete_failure_returns_exit_code_2(self, mock_processor_class, mock_client):
        """Test that complete failure (all tables fail) returns exit code 2"""
        mock_processor = Mock()
        mock_processor.process.return_value = False
        mock_processor.get_table_display_name.return_value = "doc.table"
        mock_processor_class.return_value = mock_processor

        maintenance = MaintenanceCommands(mock_client)

        tables = [
            {'schema_name': 'doc', 'table_name': 'table1', 'current_replicas': 2,
             'adaptive_threshold_mb': 563.2, 'max_translog_uncommitted_mb': 1200.0}
        ]

        # Mock _filter_tables_by_percentage to return all tables
        with patch.object(maintenance, '_filter_tables_by_percentage', return_value=tables):
            result = maintenance._execute_autoexec(tables, False, 200, 720, "console")

        assert result is False
        assert maintenance._get_autoexec_exit_code() == 2  # Complete failure

    def test_no_tables_after_filtering_succeeds(self, mock_client):
        """Test that no tables to process (after filtering) is treated as success"""
        maintenance = MaintenanceCommands(mock_client)

        # Empty table list after filtering
        result = maintenance._execute_autoexec([], False, 200, 720, "console")

        assert result is True
        assert maintenance._get_autoexec_exit_code() == 0


# ============================================================================
# CLI Integration Tests
# ============================================================================

class TestCLIIntegration:
    """Test CLI flag validation and integration"""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_autoexec_and_execute_flags_mutually_exclusive(self, runner):
        """Test that --autoexec and --execute cannot be used together"""
        with patch('cratedb_xlens.cli.CrateDBClient') as mock_client_class:
            mock_client_class.return_value.test_connection.return_value = True

            result = runner.invoke(main, [
                'problematic-translogs',
                '--autoexec',
                '--execute'
            ])

            assert result.exit_code == 1
            assert 'mutually exclusive' in result.output

    def test_dry_run_requires_autoexec(self, runner):
        """Test that --dry-run flag requires --autoexec"""
        with patch('cratedb_xlens.cli.CrateDBClient') as mock_client_class:
            mock_client_class.return_value.test_connection.return_value = True

            result = runner.invoke(main, [
                'problematic-translogs',
                '--dry-run'
            ])

            assert result.exit_code == 1
            assert '--dry-run can only be used with --autoexec' in result.output

    def test_autoexec_basic_invocation(self, runner):
        """Test basic --autoexec invocation with default parameters"""
        with patch('cratedb_xlens.cli.CrateDBClient') as mock_client_class, \
             patch('cratedb_xlens.commands.maintenance.MaintenanceCommands') as mock_maintenance_class:

            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client

            mock_maintenance = Mock()
            mock_maintenance_class.return_value = mock_maintenance

            result = runner.invoke(main, ['problematic-translogs', '--autoexec'])

            assert result.exit_code == 0

            # Verify maintenance command was called with autoexec=True
            call_args = mock_maintenance.problematic_translogs.call_args[0]
            assert call_args[2] is True  # autoexec parameter

    def test_dry_run_with_autoexec(self, runner):
        """Test --autoexec with --dry-run flag combination"""
        with patch('cratedb_xlens.cli.CrateDBClient') as mock_client_class, \
             patch('cratedb_xlens.commands.maintenance.MaintenanceCommands') as mock_maintenance_class:

            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client

            mock_maintenance = Mock()
            mock_maintenance_class.return_value = mock_maintenance

            result = runner.invoke(main, [
                'problematic-translogs',
                '--autoexec',
                '--dry-run'
            ])

            assert result.exit_code == 0

            # Verify dry_run was passed correctly
            call_args = mock_maintenance.problematic_translogs.call_args[0]
            assert call_args[2] is True   # autoexec
            assert call_args[3] is True   # dry_run


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
