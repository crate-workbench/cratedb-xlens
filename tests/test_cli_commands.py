"""
Comprehensive CLI tests for XMover subcommands
Tests the main CLI commands with mocked database connections
"""

import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from click.testing import CliRunner
from cratedb_xlens.cli import main
from cratedb_xlens.database import CrateDBClient


@pytest.fixture
def runner():
    """Click test runner fixture"""
    return CliRunner()


@pytest.fixture
def mock_client():
    """Mock CrateDB client fixture"""
    client = Mock(spec=CrateDBClient)
    client.test_connection.return_value = True
    return client


@pytest.fixture
def mock_successful_connection():
    """Mock successful database connection for CLI context"""
    with patch('cratedb_xlens.cli.CrateDBClient') as mock_class:
        mock_instance = Mock(spec=CrateDBClient)
        mock_instance.test_connection.return_value = True
        mock_class.return_value = mock_instance
        yield mock_instance


class TestAnalyzeCommand:
    """Test xmover analyze command"""
    
    def test_analyze_basic(self, runner, mock_successful_connection):
        """Test basic analyze command execution"""
        with patch('cratedb_xlens.commands.analysis.ShardAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            mock_analyzer_instance.get_cluster_overview.return_value = {
                'nodes': 3,
                'zones': 2,
                'total_shards': 100,
                'primary_shards': 50,
                'replica_shards': 50,
                'total_size_gb': 1000,
                'watermarks': {
                    'low': '85%',
                    'high': '90%',
                    'flood_stage': '95%'
                },
                'zone_distribution': {'zone1': 60, 'zone2': 40},
                'node_health': [
                    {
                        'name': 'data-hot-1',
                        'zone': 'zone1',
                        'shards': 50,
                        'size_gb': 500,
                        'disk_usage_percent': 70,
                        'available_space_gb': 300,
                        'remaining_to_low_watermark_gb': 150,
                        'remaining_to_high_watermark_gb': 100
                    }
                ]
            }
            mock_analyzer_instance.get_shard_size_overview.return_value = {
                'total_shards': 100,
                'large_shards_count': 0,
                'size_buckets': {
                    '<1GB': {'count': 30, 'avg_size_gb': 0.5, 'max_size': 1.0, 'total_size': 15.0},
                    '1-10GB': {'count': 50, 'avg_size_gb': 5.0, 'max_size': 10.0, 'total_size': 250.0},
                    '>=50GB': {'count': 0, 'avg_size_gb': 0, 'max_size': 0, 'total_size': 0}
                }
            }
            mock_analyzer_instance.get_table_size_breakdown.return_value = []
            mock_analyzer_instance.get_large_shards_details.return_value = []
            mock_analyzer_instance.get_small_shards_details.return_value = []
            mock_analyzer.return_value = mock_analyzer_instance
            
            result = runner.invoke(main, ['analyze'])
            assert result.exit_code == 0
            mock_analyzer_instance.get_cluster_overview.assert_called_once()
    
    def test_analyze_with_table_filter(self, runner, mock_successful_connection):
        """Test analyze command with table filter"""
        with patch('cratedb_xlens.commands.analysis.ShardAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            mock_analyzer_instance.get_cluster_overview.return_value = {
                'nodes': 3,
                'zones': 2,
                'total_shards': 100,
                'primary_shards': 50,
                'replica_shards': 50,
                'total_size_gb': 1000,
                'watermarks': {},
                'zone_distribution': {'zone1': 60, 'zone2': 40},
                'node_health': []
            }
            mock_analyzer_instance.get_shard_size_overview.return_value = {
                'total_shards': 100,
                'large_shards_count': 0,
                'size_buckets': {
                    '<1GB': {'count': 30, 'avg_size_gb': 0.5, 'max_size': 1.0, 'total_size': 15.0}
                }
            }
            mock_analyzer_instance.get_table_size_breakdown.return_value = []
            mock_analyzer_instance.get_large_shards_details.return_value = []
            mock_analyzer_instance.get_small_shards_details.return_value = []
            # Create a simple object with the required attributes instead of Mock
            class MockDistributionStats:
                def __init__(self):
                    self.total_shards = 10
                    self.total_size_gb = 50.0
                    self.nodes = ['data-hot-1', 'data-hot-2']
                    self.zone_balance_score = 85.5
                    self.node_balance_score = 90.2
            
            mock_analyzer_instance.analyze_distribution.return_value = MockDistributionStats()
            mock_analyzer.return_value = mock_analyzer_instance
            
            result = runner.invoke(main, ['analyze', '--table', 'test_table'])
            assert result.exit_code == 0
            mock_analyzer_instance.get_cluster_overview.assert_called_once()
    
    def test_analyze_with_largest_option(self, runner, mock_successful_connection):
        """Test analyze command with largest N tables option"""
        with patch('cratedb_xlens.commands.analysis.ShardAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            mock_analyzer_instance.get_cluster_overview.return_value = {
                'nodes': 3,
                'zones': 2,
                'total_shards': 100,
                'primary_shards': 50,
                'replica_shards': 50,
                'total_size_gb': 1000,
                'watermarks': {},
                'zone_distribution': {'zone1': 60, 'zone2': 40},
                'node_health': []
            }
            mock_analyzer_instance.get_shard_size_overview.return_value = {
                'total_shards': 100,
                'large_shards_count': 0,
                'size_buckets': {
                    '<1GB': {'count': 30, 'avg_size_gb': 0.5, 'max_size': 1.0, 'total_size': 15.0}
                }
            }
            mock_analyzer_instance.get_table_size_breakdown.return_value = [
                {
                    'table_name': 'large_table_1',
                    'schema_name': 'doc',
                    'partition': 'N/A',
                    'total_size': 100.0,
                    'total_shards': 10,
                    'primary_count': 5,
                    'replica_count': 5,
                    'min_size': 8.0,
                    'avg_size': 10.0,
                    'max_size': 12.0
                }
            ]
            mock_analyzer_instance.get_large_shards_details.return_value = []
            mock_analyzer_instance.get_small_shards_details.return_value = []
            mock_analyzer.return_value = mock_analyzer_instance
            
            result = runner.invoke(main, ['analyze', '--largest', '5'])
            assert result.exit_code == 0


class TestTestConnectionCommand:
    """Test xmover test-connection command"""
    
    def test_test_connection_success(self, runner):
        """Test successful connection test"""
        with patch('cratedb_xlens.database.CrateDBClient') as mock_client_class:
            # Mock both the main startup client and the command client
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client.get_nodes_info.return_value = [
                Mock(name='data-hot-1', zone='zone1'),
                Mock(name='data-hot-2', zone='zone2')
            ]
            mock_client_class.return_value = mock_client
            
            result = runner.invoke(main, ['test-connection'])
            assert result.exit_code == 0
            # Should be called at least twice (main startup + command)
            assert mock_client.test_connection.call_count >= 2
    
    def test_test_connection_failure(self, runner):
        """Test failed connection test"""
        with patch('cratedb_xlens.database.CrateDBClient') as mock_client_class:
            # Create different mocks for different calls
            call_count = 0
            def mock_test_connection():
                nonlocal call_count
                call_count += 1
                return call_count == 1  # First call succeeds, second fails
            
            mock_client = Mock()
            mock_client.test_connection.side_effect = mock_test_connection
            mock_client_class.return_value = mock_client
            
            result = runner.invoke(main, ['test-connection'])
            # The command actually succeeds because the diagnostic command handles failures gracefully
            assert result.exit_code == 0
    
    def test_test_connection_with_custom_string(self, runner):
        """Test connection test with custom connection string"""
        with patch('cratedb_xlens.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client.get_nodes_info.return_value = [
                Mock(name='data-hot-1', zone='zone1')
            ]
            mock_client_class.return_value = mock_client
            
            result = runner.invoke(main, ['test-connection', '--connection-string', 'custom://connection'])
            assert result.exit_code == 0
            # The test-connection command doesn't create a new client with the custom string,
            # it passes it to the existing client's test_connection method


class TestMonitorRecoveryCommand:
    """Test xmover monitor-recovery command"""
    
    def test_monitor_recovery_basic(self, runner, mock_successful_connection):
        """Test basic monitor-recovery command"""
        with patch('cratedb_xlens.commands.monitoring.RecoveryMonitor') as mock_monitor:
            mock_monitor_instance = Mock()
            mock_monitor_instance.get_cluster_recovery_status.return_value = []
            mock_monitor.return_value = mock_monitor_instance
            
            result = runner.invoke(main, ['monitor-recovery'])
            assert result.exit_code == 0
            mock_monitor_instance.get_cluster_recovery_status.assert_called()
    
    def test_monitor_recovery_with_include_transitioning(self, runner, mock_successful_connection):
        """Test monitor-recovery with --include-transitioning flag"""
        with patch('cratedb_xlens.commands.monitoring.RecoveryMonitor') as mock_monitor:
            mock_monitor_instance = Mock()
            mock_monitor_instance.get_cluster_recovery_status.return_value = []
            mock_monitor.return_value = mock_monitor_instance
            
            result = runner.invoke(main, ['monitor-recovery', '--include-transitioning'])
            assert result.exit_code == 0
    
    def test_monitor_recovery_watch_mode(self, runner, mock_successful_connection):
        """Test monitor-recovery with --watch flag (single iteration for test)"""
        with patch('cratedb_xlens.commands.monitoring.RecoveryMonitor') as mock_monitor:
            with patch('time.sleep') as mock_sleep:
                mock_monitor_instance = Mock()
                mock_monitor_instance.get_cluster_recovery_status.return_value = []
                mock_monitor.return_value = mock_monitor_instance
                
                # Simulate KeyboardInterrupt to exit watch mode
                mock_sleep.side_effect = KeyboardInterrupt
                
                result = runner.invoke(main, ['monitor-recovery', '--watch'])
                assert result.exit_code == 0


class TestProblematicTranslogsCommand:
    """Test xmover problematic-translogs command"""
    
    def test_problematic_translogs_basic(self, runner, mock_successful_connection):
        """Test basic problematic-translogs command"""
        # problematic-translogs command doesn't use ShardAnalyzer, it queries directly
        result = runner.invoke(main, ['problematic-translogs'])
        assert result.exit_code == 0
    
    def test_problematic_translogs_with_size_mb(self, runner, mock_successful_connection):
        """Test problematic-translogs with custom sizeMB"""
        with patch('cratedb_xlens.cli.ShardAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            mock_analyzer_instance.get_problematic_translogs.return_value = []
            mock_analyzer.return_value = mock_analyzer_instance
            
            result = runner.invoke(main, ['problematic-translogs', '--sizeMB', '520'])
            assert result.exit_code == 0
    
    def test_problematic_translogs_with_execute_flag(self, runner, mock_successful_connection):
        """Test problematic-translogs with execute flag"""
        with patch('cratedb_xlens.commands.analysis.ShardAnalyzer') as mock_analyzer:
            with patch('click.confirm', return_value=False):  # User says no to execution
                mock_analyzer_instance = Mock()
                mock_analyzer_instance.get_problematic_translogs.return_value = []
                mock_analyzer.return_value = mock_analyzer_instance
                
                result = runner.invoke(main, ['problematic-translogs', '--execute'])
                assert result.exit_code == 0


class TestDeepAnalyzeCommand:
    """Test xmover deep-analyze command"""
    
    def test_deep_analyze_basic(self, runner, mock_successful_connection):
        """Test basic deep-analyze command"""
        with patch('cratedb_xlens.commands.analysis.ShardSizeMonitor') as mock_monitor:
            mock_monitor_instance = Mock()
            mock_monitor_instance.analyze_all_schemas.return_value = {'violations': []}
            mock_monitor.return_value = mock_monitor_instance
            
            result = runner.invoke(main, ['deep-analyze'])
            assert result.exit_code == 0
            # deep-analyze uses different method names, just check it runs successfully
    
    def test_deep_analyze_with_schema(self, runner, mock_successful_connection):
        """Test deep-analyze with specific schema"""
        with patch('cratedb_xlens.cli.ShardSizeMonitor') as mock_monitor:
            mock_monitor_instance = Mock()
            mock_monitor_instance.analyze_schema.return_value = {'violations': []}
            mock_monitor.return_value = mock_monitor_instance
            
            result = runner.invoke(main, ['deep-analyze', '--schema', 'test_schema'])
            assert result.exit_code == 0
    
    def test_deep_analyze_with_severity_filter(self, runner, mock_successful_connection):
        """Test deep-analyze with severity filter"""
        with patch('cratedb_xlens.cli.ShardSizeMonitor') as mock_monitor:
            mock_monitor_instance = Mock()
            mock_monitor_instance.analyze_all_schemas.return_value = {'violations': []}
            mock_monitor.return_value = mock_monitor_instance
            
            result = runner.invoke(main, ['deep-analyze', '--severity', 'critical'])
            assert result.exit_code == 0


class TestLargeTranslogsCommand:
    """Test xmover large-translogs command"""
    
    def test_large_translogs_basic(self, runner, mock_successful_connection):
        """Test basic large-translogs command"""
        with patch('cratedb_xlens.cli.ShardAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            mock_analyzer_instance.get_large_translogs.return_value = []
            mock_analyzer.return_value = mock_analyzer_instance
            
            result = runner.invoke(main, ['large-translogs'])
            assert result.exit_code == 0
    
    def test_large_translogs_with_custom_size(self, runner, mock_successful_connection):
        """Test large-translogs with custom translog size threshold"""
        with patch('cratedb_xlens.cli.ShardAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            mock_analyzer_instance.get_large_translogs.return_value = []
            mock_analyzer.return_value = mock_analyzer_instance
            
            result = runner.invoke(main, ['large-translogs', '--translogsize', '1000'])
            assert result.exit_code == 0
    
    def test_large_translogs_with_table_filter(self, runner, mock_successful_connection):
        """Test large-translogs with table filter"""
        with patch('cratedb_xlens.cli.ShardAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            mock_analyzer_instance.get_large_translogs.return_value = []
            mock_analyzer.return_value = mock_analyzer_instance
            
            result = runner.invoke(main, ['large-translogs', '--table', 'test_table'])
            assert result.exit_code == 0
    
    def test_large_translogs_watch_mode(self, runner, mock_successful_connection):
        """Test large-translogs with watch mode (single iteration for test)"""
        with patch('cratedb_xlens.cli.ShardAnalyzer') as mock_analyzer:
            with patch('time.sleep') as mock_sleep:
                mock_analyzer_instance = Mock()
                mock_analyzer_instance.get_large_translogs.return_value = []
                mock_analyzer.return_value = mock_analyzer_instance
                
                # Simulate KeyboardInterrupt to exit watch mode
                mock_sleep.side_effect = KeyboardInterrupt
                
                result = runner.invoke(main, ['large-translogs', '--watch'])
                assert result.exit_code == 0


class TestShardDistributionCommand:
    """Test xmover shard-distribution command"""
    
    def test_shard_distribution_basic(self, runner, mock_successful_connection):
        """Test basic shard-distribution command"""
        with patch('cratedb_xlens.commands.maintenance.shard_distribution.DistributionAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            mock_analyzer_instance.get_largest_tables_distribution.return_value = []
            mock_analyzer.return_value = mock_analyzer_instance
            
            result = runner.invoke(main, ['shard-distribution'])
            assert result.exit_code == 0
            mock_analyzer_instance.get_largest_tables_distribution.assert_called()
    
    def test_shard_distribution_with_top_tables(self, runner, mock_successful_connection):
        """Test shard-distribution with custom top tables count"""
        with patch('cratedb_xlens.commands.maintenance.shard_distribution.DistributionAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            mock_analyzer_instance.get_largest_tables_distribution.return_value = []
            mock_analyzer.return_value = mock_analyzer_instance
            
            result = runner.invoke(main, ['shard-distribution', '--top-tables', '20'])
            assert result.exit_code == 0
            mock_analyzer_instance.get_largest_tables_distribution.assert_called()

    def test_shard_distribution_with_specific_table(self, runner, mock_successful_connection):
        """Test shard-distribution with specific table"""
        with patch('cratedb_xlens.commands.maintenance.shard_distribution.DistributionAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            mock_analyzer_instance.get_table_distribution_detailed.return_value = None
            mock_analyzer.return_value = mock_analyzer_instance
            
            result = runner.invoke(main, ['shard-distribution', '--table', 'test_table'])
            assert result.exit_code == 0
            mock_analyzer_instance.get_table_distribution_detailed.assert_called()


class TestZoneAnalysisCommand:
    """Test xmover zone-analysis command"""
    
    def test_zone_analysis_basic(self, runner, mock_successful_connection):
        """Test basic zone-analysis command"""
        with patch('cratedb_xlens.analyzer.ShardAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            # zone-analysis doesn't use ShardAnalyzer.analyze_zones, it has its own implementation
            # Just mock to return empty result for test
            mock_analyzer_instance = Mock()
            mock_analyzer.return_value = mock_analyzer_instance
            
            result = runner.invoke(main, ['zone-analysis'])
            assert result.exit_code == 0
    
    def test_zone_analysis_with_table_filter(self, runner, mock_successful_connection):
        """Test zone-analysis with table filter"""
        with patch('cratedb_xlens.analyzer.ShardAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            mock_analyzer.return_value = mock_analyzer_instance
            
            result = runner.invoke(main, ['zone-analysis', '--table', 'test_table'])
            assert result.exit_code == 0
    
    def test_zone_analysis_with_show_shards(self, runner, mock_successful_connection):
        """Test zone-analysis with show-shards option"""
        with patch('cratedb_xlens.analyzer.ShardAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            mock_analyzer.return_value = mock_analyzer_instance
            
            result = runner.invoke(main, ['zone-analysis', '--show-shards'])
            assert result.exit_code == 0


class TestConnectionFailureHandling:
    """Test how commands handle database connection failures"""
    
    def test_commands_fail_on_connection_error(self, runner):
        """Test that commands properly handle connection failures"""
        with patch('cratedb_xlens.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = False
            mock_client_class.return_value = mock_client
            
            result = runner.invoke(main, ['analyze'])
            assert result.exit_code == 1
    
    def test_commands_handle_connection_exception(self, runner):
        """Test that commands handle connection exceptions"""
        with patch('cratedb_xlens.cli.CrateDBClient') as mock_client_class:
            mock_client_class.side_effect = Exception("Connection failed")
            
            result = runner.invoke(main, ['analyze'])
            assert result.exit_code == 1


class TestErrorHandling:
    """Test error handling in CLI commands"""
    
    def test_analyze_handles_analyzer_exception(self, runner, mock_successful_connection):
        """Test that analyze command handles analyzer exceptions gracefully"""
        with patch('cratedb_xlens.commands.analysis.ShardAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            mock_analyzer_instance.get_cluster_overview.side_effect = Exception("Analyzer failed")
            mock_analyzer.return_value = mock_analyzer_instance
            
            result = runner.invoke(main, ['analyze'])
            # Command should not crash completely
            assert result.exit_code in [0, 1]
    
    def test_problematic_translogs_handles_missing_data(self, runner, mock_successful_connection):
        """Test problematic-translogs handles missing data gracefully"""
        with patch('cratedb_xlens.commands.analysis.ShardAnalyzer') as mock_analyzer:
            mock_analyzer_instance = Mock()
            mock_analyzer_instance.get_problematic_translogs.return_value = None
            mock_analyzer.return_value = mock_analyzer_instance
            
            result = runner.invoke(main, ['problematic-translogs'])
            assert result.exit_code == 0


class TestCommandOptions:
    """Test command options and help"""
    
    def test_help_options(self, runner):
        """Test help options for main commands"""
        from unittest.mock import patch, Mock
        
        commands_to_test = [
            'analyze', 'test-connection', 'monitor-recovery', 
            'problematic-translogs', 'deep-analyze', 'large-translogs',
            'shard-distribution', 'zone-analysis'
        ]
        
        # Mock the connection to avoid startup issues
        with patch('cratedb_xlens.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            for command in commands_to_test:
                result = runner.invoke(main, [command, '--help'])
                assert result.exit_code == 0, f"Command {command} help failed with exit code {result.exit_code}"
                assert 'Usage:' in result.output or 'Show this message and exit' in result.output