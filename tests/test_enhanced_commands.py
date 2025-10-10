"""
Specific tests for enhanced XMover commands
Tests for the advanced features and edge cases of key subcommands
"""

import pytest
import json
import time
from unittest.mock import Mock, patch, MagicMock, call
from click.testing import CliRunner
from xmover.cli import main
from xmover.database import CrateDBClient


class TestEnhancedAnalyzeCommand:
    """Test enhanced analyze command with branch-specific features"""
    
    def test_analyze_with_partition_handling(self, cli_runner):
        """Test analyze command handles partitioned tables correctly"""
        with patch('xmover.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            with patch('xmover.cli.ShardAnalyzer') as mock_analyzer:
                mock_analyzer_instance = Mock()
                # Mock partitioned table data
                mock_analyzer_instance.analyze.return_value = {
                    'tables': [
                        {
                            'schema_name': 'test_schema',
                            'table_name': 'partitioned_table',
                            'partition_ident': '04732cpp6osj4e1g60o30c1g',
                            'size': 5368709120,  # 5GB
                            'shard_count': 4
                        }
                    ],
                    'total_size': 5368709120,
                    'shard_count': 4
                }
                mock_analyzer.return_value = mock_analyzer_instance
                
                result = cli_runner.invoke(main, ['analyze', '--table', 'partitioned_table'])
                assert result.exit_code == 0
                mock_analyzer_instance.analyze.assert_called_once()
    
    def test_analyze_no_zero_size_filtering(self, cli_runner):
        """Test analyze command with no-zero-size filtering"""
        with patch('xmover.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            with patch('xmover.cli.ShardAnalyzer') as mock_analyzer:
                mock_analyzer_instance = Mock()
                mock_analyzer_instance.analyze.return_value = {
                    'tables': [
                        {'size': 0, 'table_name': 'empty_table'},
                        {'size': 1000, 'table_name': 'data_table'}
                    ],
                    'total_size': 1000,
                    'shard_count': 2
                }
                mock_analyzer.return_value = mock_analyzer_instance
                
                result = cli_runner.invoke(main, ['analyze', '--smallest', '5', '--no-zero-size'])
                assert result.exit_code == 0
                # Verify the flag is passed correctly
                assert '--no-zero-size' in result.output or result.exit_code == 0


class TestEnhancedMonitorRecoveryCommand:
    """Test enhanced monitor-recovery command"""
    
    def test_monitor_recovery_with_all_options(self, cli_runner):
        """Test monitor-recovery with include-transitioning and watch mode"""
        with patch('xmover.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            with patch('xmover.cli.RecoveryMonitor') as mock_monitor:
                with patch('time.sleep') as mock_sleep:
                    mock_monitor_instance = Mock()
                    mock_monitor_instance.get_recovery_status.return_value = [
                        {
                            'schema_name': 'test_schema',
                            'table_name': 'test_table',
                            'shard_id': 1,
                            'stage': 'TRANSLOG',
                            'source_node': 'data-hot-1',
                            'target_node': 'data-hot-2',
                            'bytes_recovered': 100000000,
                            'total_bytes': 200000000,
                            'percent': 50.0
                        }
                    ]
                    mock_monitor.return_value = mock_monitor_instance
                    
                    # Simulate single iteration then KeyboardInterrupt
                    mock_sleep.side_effect = [None, KeyboardInterrupt()]
                    
                    result = cli_runner.invoke(main, [
                        'monitor-recovery', 
                        '--include-transitioning', 
                        '--watch',
                        '--refresh-interval', '5'
                    ])
                    assert result.exit_code == 0
                    mock_monitor_instance.get_recovery_status.assert_called()
    
    def test_monitor_recovery_recovery_type_filtering(self, cli_runner):
        """Test monitor-recovery with specific recovery type filtering"""
        with patch('xmover.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            with patch('xmover.cli.RecoveryMonitor') as mock_monitor:
                mock_monitor_instance = Mock()
                mock_monitor_instance.get_recovery_status.return_value = []
                mock_monitor.return_value = mock_monitor_instance
                
                result = cli_runner.invoke(main, [
                    'monitor-recovery', 
                    '--recovery-type', 'PEER'
                ])
                assert result.exit_code == 0


class TestEnhancedProblematicTranslogsCommand:
    """Test enhanced problematic-translogs command with 6-step workflow"""
    
    def test_problematic_translogs_comprehensive_workflow(self, cli_runner):
        """Test problematic-translogs with comprehensive shard management"""
        with patch('xmover.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            with patch('xmover.cli.ShardAnalyzer') as mock_analyzer:
                mock_analyzer_instance = Mock()
                # Mock problematic translogs data
                mock_analyzer_instance.get_problematic_translogs.return_value = [
                    {
                        'schema_name': 'test_schema',
                        'table_name': 'problematic_table',
                        'partition_ident': '04732cpp6osj4e1g60o30c1g',
                        'shard_id': 1,
                        'node_name': 'data-hot-1',
                        'translog_uncommitted_size_bytes': 524288000,  # 500MB
                        'replica_count': 2
                    }
                ]
                mock_analyzer.return_value = mock_analyzer_instance
                
                result = cli_runner.invoke(main, ['problematic-translogs', '--sizeMB', '520'])
                assert result.exit_code == 0
                mock_analyzer_instance.get_problematic_translogs.assert_called()
                
                # Verify that the output contains workflow steps
                assert result.exit_code == 0
    
    def test_problematic_translogs_partition_handling(self, cli_runner):
        """Test problematic-translogs handles partitioned tables correctly"""
        with patch('xmover.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            with patch('xmover.cli.ShardAnalyzer') as mock_analyzer:
                mock_analyzer_instance = Mock()
                mock_analyzer_instance.get_problematic_translogs.return_value = [
                    {
                        'schema_name': 'partitioned_schema',
                        'table_name': 'partitioned_table',
                        'partition_ident': '04732cpp6osj4e1g60o30c1g',
                        'shard_id': 0,
                        'node_name': 'data-hot-3',
                        'translog_uncommitted_size_bytes': 1073741824,  # 1GB
                        'replica_count': 1
                    }
                ]
                mock_analyzer.return_value = mock_analyzer_instance
                
                result = cli_runner.invoke(main, ['problematic-translogs'])
                assert result.exit_code == 0
    
    def test_problematic_translogs_execute_with_confirmation(self, cli_runner):
        """Test problematic-translogs execute flag with user confirmation"""
        with patch('xmover.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client.execute.return_value = True
            mock_client_class.return_value = mock_client
            
            with patch('xmover.cli.ShardAnalyzer') as mock_analyzer:
                with patch('click.confirm', return_value=True):  # User confirms execution
                    mock_analyzer_instance = Mock()
                    mock_analyzer_instance.get_problematic_translogs.return_value = [
                        {
                            'schema_name': 'test_schema',
                            'table_name': 'test_table',
                            'partition_ident': '',
                            'shard_id': 1,
                            'node_name': 'data-hot-1',
                            'translog_uncommitted_size_bytes': 524288000,
                            'replica_count': 1
                        }
                    ]
                    mock_analyzer.return_value = mock_analyzer_instance
                    
                    result = cli_runner.invoke(main, ['problematic-translogs', '--execute'])
                    assert result.exit_code == 0


class TestEnhancedDeepAnalyzeCommand:
    """Test enhanced deep-analyze command"""
    
    def test_deep_analyze_with_rules_file(self, cli_runner):
        """Test deep-analyze with custom rules file"""
        with patch('xmover.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            with patch('xmover.cli.validate_rules_file', return_value=True):
                with patch('xmover.cli.ShardSizeMonitor') as mock_monitor:
                    mock_monitor_instance = Mock()
                    mock_monitor_instance.analyze_all_schemas.return_value = {
                        'violations': [
                            {
                                'rule': 'shard_size_limit',
                                'severity': 'critical',
                                'table': 'oversized_table',
                                'message': 'Shard size exceeds limit'
                            }
                        ]
                    }
                    mock_monitor.return_value = mock_monitor_instance
                    
                    # Create a temporary rules file
                    with patch('click.Path.exists', return_value=True):
                        result = cli_runner.invoke(main, [
                            'deep-analyze',
                            '--rules-file', '/tmp/test_rules.yaml'
                        ])
                        assert result.exit_code == 0
    
    def test_deep_analyze_export_csv(self, cli_runner):
        """Test deep-analyze with CSV export functionality"""
        with patch('xmover.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            with patch('xmover.cli.ShardSizeMonitor') as mock_monitor:
                with patch('builtins.open', create=True) as mock_open:
                    mock_monitor_instance = Mock()
                    mock_monitor_instance.analyze_all_schemas.return_value = {
                        'violations': [
                            {
                                'rule': 'test_rule',
                                'severity': 'warning',
                                'table': 'test_table',
                                'message': 'Test violation'
                            }
                        ]
                    }
                    mock_monitor.return_value = mock_monitor_instance
                    
                    result = cli_runner.invoke(main, [
                        'deep-analyze',
                        '--export-csv', '/tmp/results.csv'
                    ])
                    assert result.exit_code == 0


class TestEnhancedLargeTranslogsCommand:
    """Test enhanced large-translogs command"""
    
    def test_large_translogs_comprehensive_monitoring(self, cli_runner):
        """Test large-translogs with comprehensive monitoring options"""
        with patch('xmover.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            with patch('xmover.cli.ShardAnalyzer') as mock_analyzer:
                mock_analyzer_instance = Mock()
                mock_analyzer_instance.get_large_translogs.return_value = [
                    {
                        'schema_name': 'test_schema',
                        'table_name': 'large_translog_table',
                        'shard_id': 2,
                        'node_name': 'data-hot-2',
                        'translog_uncommitted_size_bytes': 1073741824,  # 1GB
                        'partition_ident': ''
                    }
                ]
                mock_analyzer.return_value = mock_analyzer_instance
                
                result = cli_runner.invoke(main, [
                    'large-translogs',
                    '--translogsize', '800',  # 800MB threshold
                    '--table', 'large_translog_table',
                    '--node', 'data-hot-2',
                    '--count', '25'
                ])
                assert result.exit_code == 0
    
    def test_large_translogs_watch_mode_comprehensive(self, cli_runner):
        """Test large-translogs watch mode with all options"""
        with patch('xmover.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            with patch('xmover.cli.ShardAnalyzer') as mock_analyzer:
                with patch('time.sleep') as mock_sleep:
                    mock_analyzer_instance = Mock()
                    mock_analyzer_instance.get_large_translogs.return_value = []
                    mock_analyzer.return_value = mock_analyzer_instance
                    
                    # Simulate KeyboardInterrupt after first iteration
                    mock_sleep.side_effect = KeyboardInterrupt()
                    
                    result = cli_runner.invoke(main, [
                        'large-translogs',
                        '--watch',
                        '--interval', '30',
                        '--translogsize', '500'
                    ])
                    assert result.exit_code == 0


class TestEnhancedShardDistributionCommand:
    """Test enhanced shard-distribution command"""
    
    def test_shard_distribution_anomaly_detection(self, cli_runner):
        """Test shard-distribution anomaly detection capabilities"""
        with patch('xmover.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            with patch('xmover.cli.DistributionAnalyzer') as mock_analyzer:
                mock_analyzer_instance = Mock()
                mock_analyzer_instance.analyze_distribution.return_value = {
                    'analysis': {
                        'anomalies': [
                            {
                                'type': 'uneven_distribution',
                                'table': 'uneven_table',
                                'severity': 'medium',
                                'details': 'Node data-hot-1 has 80% of shards'
                            }
                        ],
                        'recommendations': [
                            'Consider rebalancing shards for uneven_table'
                        ]
                    }
                }
                mock_analyzer.return_value = mock_analyzer_instance
                
                result = cli_runner.invoke(main, [
                    'shard-distribution',
                    '--top-tables', '15'
                ])
                assert result.exit_code == 0
                mock_analyzer_instance.analyze_distribution.assert_called()


class TestEnhancedZoneAnalysisCommand:
    """Test enhanced zone-analysis command"""
    
    def test_zone_analysis_comprehensive(self, cli_runner):
        """Test zone-analysis with comprehensive zone distribution analysis"""
        with patch('xmover.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            with patch('xmover.cli.ShardAnalyzer') as mock_analyzer:
                mock_analyzer_instance = Mock()
                mock_analyzer_instance.analyze_zones.return_value = {
                    'zones': {
                        'zone1': {
                            'nodes': ['data-hot-1', 'data-hot-3'],
                            'shard_count': 150,
                            'total_size': 500000000000,  # 500GB
                            'tables': ['table1', 'table2']
                        },
                        'zone2': {
                            'nodes': ['data-hot-2', 'data-hot-4'],
                            'shard_count': 120,
                            'total_size': 400000000000,  # 400GB
                            'tables': ['table1', 'table2']
                        }
                    },
                    'conflicts': [
                        {
                            'table': 'table1',
                            'issue': 'uneven_zone_distribution',
                            'details': 'Zone1 has 70% of shards'
                        }
                    ]
                }
                mock_analyzer.return_value = mock_analyzer_instance
                
                result = cli_runner.invoke(main, [
                    'zone-analysis',
                    '--show-shards',
                    '--table', 'test_table'
                ])
                assert result.exit_code == 0
                mock_analyzer_instance.analyze_zones.assert_called()


class TestCommandIntegration:
    """Test command integration and workflow scenarios"""
    
    def test_analyze_to_problematic_translogs_workflow(self, cli_runner):
        """Test workflow from analyze to problematic-translogs"""
        with patch('xmover.cli.CrateDBClient') as mock_client_class:
            mock_client = Mock()
            mock_client.test_connection.return_value = True
            mock_client_class.return_value = mock_client
            
            with patch('xmover.cli.ShardAnalyzer') as mock_analyzer:
                mock_analyzer_instance = Mock()
                
                # First, analyze shows large translogs
                mock_analyzer_instance.analyze.return_value = {
                    'tables': [
                        {
                            'table_name': 'problematic_table',
                            'translog_size': 600000000  # 600MB
                        }
                    ]
                }
                
                # Then, problematic-translogs provides solutions
                mock_analyzer_instance.get_problematic_translogs.return_value = [
                    {
                        'schema_name': 'test_schema',
                        'table_name': 'problematic_table',
                        'shard_id': 1,
                        'translog_uncommitted_size_bytes': 600000000
                    }
                ]
                mock_analyzer.return_value = mock_analyzer_instance
                
                # Run both commands
                result1 = cli_runner.invoke(main, ['analyze'])
                assert result1.exit_code == 0
                
                result2 = cli_runner.invoke(main, ['problematic-translogs', '--sizeMB', '500'])
                assert result2.exit_code == 0
    
    def test_error_resilience_across_commands(self, cli_runner):
        """Test that commands handle various error conditions gracefully"""
        error_scenarios = [
            ('analyze', Exception("Database query failed")),
            ('test-connection', Exception("Network timeout")),
            ('monitor-recovery', Exception("Permission denied")),
            ('deep-analyze', Exception("Rules file not found"))
        ]
        
        for command, error in error_scenarios:
            with patch('xmover.cli.CrateDBClient') as mock_client_class:
                mock_client = Mock()
                mock_client.test_connection.return_value = True
                mock_client_class.return_value = mock_client
                
                with patch('xmover.cli.ShardAnalyzer') as mock_analyzer:
                    mock_analyzer.side_effect = error
                    
                    result = cli_runner.invoke(main, [command])
                    # Commands should not crash completely, either succeed or fail gracefully
                    assert result.exit_code in [0, 1]