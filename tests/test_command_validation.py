"""
Command validation tests for XMover CLI
Simple validation that all commands can be invoked and parse arguments correctly
"""

import pytest
from unittest.mock import Mock, patch
from click.testing import CliRunner
from xmover.cli import main


class TestCommandValidation:
    """Test that all XMover commands can be invoked without crashing"""
    
    @pytest.fixture
    def runner(self):
        """Click test runner fixture"""
        return CliRunner()
    
    @pytest.fixture
    def mock_successful_connection(self):
        """Mock successful database connection"""
        with patch('xmover.cli.CrateDBClient') as mock_class:
            mock_instance = Mock()
            mock_instance.test_connection.return_value = True
            mock_class.return_value = mock_instance
            yield mock_instance
    
    def test_main_help(self, runner):
        """Test main command help"""
        result = runner.invoke(main, ['--help'])
        assert result.exit_code == 0
        assert 'XMover - CrateDB Shard Analyzer' in result.output
    
    def test_main_version(self, runner):
        """Test version command"""
        result = runner.invoke(main, ['--version'])
        assert result.exit_code == 0
    
    def test_analyze_help(self, runner):
        """Test analyze command help"""
        result = runner.invoke(main, ['analyze', '--help'])
        assert result.exit_code == 0
        assert 'Analyze current shard distribution' in result.output
    
    def test_test_connection_help(self, runner):
        """Test test-connection command help"""
        result = runner.invoke(main, ['test-connection', '--help'])
        assert result.exit_code == 0
        assert 'Test connection to CrateDB cluster' in result.output
    
    def test_monitor_recovery_help(self, runner):
        """Test monitor-recovery command help"""
        result = runner.invoke(main, ['monitor-recovery', '--help'])
        assert result.exit_code == 0
        assert 'Monitor active shard recovery' in result.output
    
    def test_problematic_translogs_help(self, runner):
        """Test problematic-translogs command help"""
        result = runner.invoke(main, ['problematic-translogs', '--help'])
        assert result.exit_code == 0
        assert 'Find tables with problematic translog sizes' in result.output
    
    def test_deep_analyze_help(self, runner):
        """Test deep-analyze command help"""
        result = runner.invoke(main, ['deep-analyze', '--help'])
        assert result.exit_code == 0
    
    def test_large_translogs_help(self, runner):
        """Test large-translogs command help"""
        result = runner.invoke(main, ['large-translogs', '--help'])
        assert result.exit_code == 0
        assert 'Monitor shards with large translog' in result.output
    
    def test_shard_distribution_help(self, runner):
        """Test shard-distribution command help"""
        result = runner.invoke(main, ['shard-distribution', '--help'])
        assert result.exit_code == 0
        assert 'Analyze shard distribution anomalies' in result.output
    
    def test_zone_analysis_help(self, runner):
        """Test zone-analysis command help"""
        result = runner.invoke(main, ['zone-analysis', '--help'])
        assert result.exit_code == 0
        assert 'Detailed analysis of zone distribution' in result.output
    
    def test_analyze_dry_run(self, runner, mock_successful_connection):
        """Test analyze command executes without errors"""
        with patch('xmover.cli.ShardAnalyzer') as mock_analyzer:
            mock_instance = Mock()
            mock_instance.get_cluster_overview.return_value = {
                'nodes': 3, 'zones': 2, 'total_shards': 100, 'primary_shards': 50,
                'replica_shards': 50, 'total_size_gb': 1000, 'watermarks': {},
                'zone_distribution': {'zone1': 60, 'zone2': 40}, 'node_health': []
            }
            mock_instance.get_shard_size_overview.return_value = {
                'total_shards': 100,
                'large_shards_count': 0,
                'size_buckets': {
                    '<1GB': {'count': 30, 'avg_size_gb': 0.5, 'max_size': 1.0, 'total_size': 15.0}
                }
            }
            mock_instance.get_table_size_breakdown.return_value = []
            mock_instance.get_large_shards_details.return_value = []
            mock_instance.get_small_shards_details.return_value = []
            mock_analyzer.return_value = mock_instance
            
            result = runner.invoke(main, ['analyze'])
            assert result.exit_code == 0
    
    def test_test_connection_dry_run(self, runner):
        """Test test-connection command executes without errors"""
        with patch('xmover.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            result = runner.invoke(main, ['test-connection'])
            assert result.exit_code == 0
    
    def test_problematic_translogs_dry_run(self, runner, mock_successful_connection):
        """Test problematic-translogs command executes without errors"""
        with patch('xmover.cli.ShardAnalyzer') as mock_analyzer:
            mock_instance = Mock()
            mock_instance.get_problematic_translogs.return_value = []
            mock_analyzer.return_value = mock_instance
            
            result = runner.invoke(main, ['problematic-translogs', '--sizeMB', '520'])
            assert result.exit_code == 0
    
    def test_monitor_recovery_dry_run(self, runner, mock_successful_connection):
        """Test monitor-recovery command executes without errors"""
        with patch('xmover.cli.RecoveryMonitor') as mock_monitor:
            mock_instance = Mock()
            mock_instance.get_recovery_status.return_value = []
            mock_monitor.return_value = mock_instance
            
            result = runner.invoke(main, ['monitor-recovery', '--include-transitioning'])
            assert result.exit_code == 0
    
    def test_deep_analyze_dry_run(self, runner, mock_successful_connection):
        """Test deep-analyze command executes without errors"""
        with patch('xmover.cli.ShardSizeMonitor') as mock_monitor:
            mock_instance = Mock()
            mock_instance.analyze_all_schemas.return_value = {'violations': []}
            mock_monitor.return_value = mock_instance
            
            result = runner.invoke(main, ['deep-analyze'])
            assert result.exit_code == 0
    
    def test_large_translogs_dry_run(self, runner, mock_successful_connection):
        """Test large-translogs command executes without errors"""
        with patch('xmover.cli.ShardAnalyzer') as mock_analyzer:
            mock_instance = Mock()
            mock_instance.get_large_translogs.return_value = []
            mock_analyzer.return_value = mock_instance
            
            result = runner.invoke(main, ['large-translogs'])
            assert result.exit_code == 0
    
    def test_shard_distribution_dry_run(self, runner, mock_successful_connection):
        """Test shard-distribution command executes without errors"""
        with patch('xmover.cli.DistributionAnalyzer') as mock_analyzer:
            mock_instance = Mock()
            mock_instance.analyze_distribution.return_value = {'analysis': {}}
            mock_analyzer.return_value = mock_instance
            
            result = runner.invoke(main, ['shard-distribution'])
            assert result.exit_code == 0
    
    def test_zone_analysis_dry_run(self, runner, mock_successful_connection):
        """Test zone-analysis command executes without errors"""
        with patch('xmover.cli.ShardAnalyzer') as mock_analyzer:
            mock_instance = Mock()
            mock_instance.analyze_zones.return_value = {'zones': {}}
            mock_analyzer.return_value = mock_instance
            
            result = runner.invoke(main, ['zone-analysis'])
            assert result.exit_code == 0
    
    def test_invalid_command(self, runner):
        """Test that invalid commands show help"""
        result = runner.invoke(main, ['nonexistent-command'])
        assert result.exit_code != 0
    
    def test_connection_failure_handling(self, runner):
        """Test that connection failures are handled gracefully"""
        with patch('xmover.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = False
            mock_client_class.return_value = mock_client
            
            result = runner.invoke(main, ['analyze'])
            assert result.exit_code == 1
            assert 'Could not connect to CrateDB' in result.output
    
    def test_argument_validation(self, runner, mock_successful_connection):
        """Test that invalid arguments are caught"""
        # Test invalid sizeMB value
        result = runner.invoke(main, ['problematic-translogs', '--sizeMB', 'invalid'])
        assert result.exit_code != 0
        
        # Test invalid severity value
        result = runner.invoke(main, ['deep-analyze', '--severity', 'invalid'])
        assert result.exit_code != 0
        
        # Test invalid recovery type
        result = runner.invoke(main, ['monitor-recovery', '--recovery-type', 'invalid'])
        assert result.exit_code != 0


class TestSpecificCommandOptions:
    """Test specific command options and their validation"""
    
    @pytest.fixture
    def runner(self):
        return CliRunner()
    
    @pytest.fixture
    def mock_successful_connection(self):
        with patch('xmover.cli.CrateDBClient') as mock_class:
            mock_instance = Mock()
            mock_instance.test_connection.return_value = True
            mock_class.return_value = mock_instance
            yield mock_instance
    
    def test_analyze_options_validation(self, runner, mock_successful_connection):
        """Test analyze command option combinations"""
        with patch('xmover.cli.ShardAnalyzer') as mock_analyzer:
            mock_instance = Mock()
            mock_instance.get_cluster_overview.return_value = {
                'nodes': 3, 'zones': 2, 'total_shards': 100, 'primary_shards': 50,
                'replica_shards': 50, 'total_size_gb': 1000, 'watermarks': {},
                'zone_distribution': {'zone1': 60, 'zone2': 40}, 'node_health': []
            }
            mock_instance.get_shard_size_overview.return_value = {
                'total_shards': 100,
                'large_shards_count': 0,
                'size_buckets': {
                    '<1GB': {'count': 30, 'avg_size_gb': 0.5, 'max_size': 1.0, 'total_size': 15.0}
                }
            }
            mock_instance.get_table_size_breakdown.return_value = []
            mock_instance.get_large_shards_details.return_value = []
            mock_instance.get_small_shards_details.return_value = []
            
            # Create a simple object with the required attributes instead of Mock
            class MockDistributionStats:
                def __init__(self):
                    self.total_shards = 10
                    self.total_size_gb = 50.0
                    self.nodes = ['data-hot-1']
                    self.zone_balance_score = 85.5
                    self.node_balance_score = 90.2
            
            mock_instance.analyze_distribution.return_value = MockDistributionStats()
            mock_analyzer.return_value = mock_instance
            
            # Test valid combinations
            valid_options = [
                ['analyze', '--table', 'test_table'],
                ['analyze', '--largest', '5'],
                ['analyze', '--smallest', '3'],
                ['analyze', '--no-zero-size'],
                ['analyze', '--largest', '5', '--table', 'test_table']
            ]
            
            for options in valid_options:
                result = runner.invoke(main, options)
                assert result.exit_code == 0, f"Failed with options: {options}"
    
    def test_problematic_translogs_options(self, runner, mock_successful_connection):
        """Test problematic-translogs command options"""
        with patch('xmover.cli.ShardAnalyzer') as mock_analyzer:
            mock_instance = Mock()
            mock_instance.get_problematic_translogs.return_value = []
            mock_analyzer.return_value = mock_instance
            
            # Test different sizeMB values
            size_values = ['100', '300', '520', '1000']
            for size in size_values:
                result = runner.invoke(main, ['problematic-translogs', '--sizeMB', size])
                assert result.exit_code == 0, f"Failed with sizeMB: {size}"
    
    def test_monitor_recovery_options(self, runner, mock_successful_connection):
        """Test monitor-recovery command options"""
        with patch('xmover.cli.RecoveryMonitor') as mock_monitor:
            mock_instance = Mock()
            mock_instance.get_recovery_status.return_value = []
            mock_monitor.return_value = mock_instance
            
            # Test recovery type options
            recovery_types = ['PEER', 'DISK', 'all']
            for rec_type in recovery_types:
                result = runner.invoke(main, ['monitor-recovery', '--recovery-type', rec_type])
                assert result.exit_code == 0, f"Failed with recovery-type: {rec_type}"
    
    def test_deep_analyze_severity_options(self, runner, mock_successful_connection):
        """Test deep-analyze severity options"""
        with patch('xmover.cli.ShardSizeMonitor') as mock_monitor:
            mock_instance = Mock()
            mock_instance.analyze_all_schemas.return_value = {'violations': []}
            mock_monitor.return_value = mock_instance
            
            # Test severity levels
            severity_levels = ['critical', 'warning', 'info']
            for severity in severity_levels:
                result = runner.invoke(main, ['deep-analyze', '--severity', severity])
                assert result.exit_code == 0, f"Failed with severity: {severity}"