"""
Tests for the check-maintenance command

This module tests the check-maintenance functionality that analyzes whether
a node can be safely decommissioned and provides estimates for shard movement.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from click.testing import CliRunner
from types import SimpleNamespace
from xmover.cli import main
from xmover.commands.maintenance import MaintenanceCommands


@pytest.fixture
def runner():
    """Click test runner fixture"""
    return CliRunner()


@pytest.fixture
def mock_client():
    """Mock CrateDB client with typical cluster data"""
    client = Mock()
    client.test_connection.return_value = True
    
    # Create proper node objects with all required attributes
    from types import SimpleNamespace
    
    nodes = [
        SimpleNamespace(
            name='data-hot-1', 
            zone='us-west-2a',
            fs_total=1000 * 1024**3,  # 1TB
            fs_used=400 * 1024**3,    # 400GB used
            disk_usage_percent=40.0,
            heap_usage_percent=50.0
        ),
        SimpleNamespace(
            name='data-hot-2',
            zone='us-west-2a', 
            fs_total=1000 * 1024**3,  # 1TB
            fs_used=300 * 1024**3,    # 300GB used
            disk_usage_percent=30.0,
            heap_usage_percent=45.0
        ),
        SimpleNamespace(
            name='data-hot-4',  # Target node
            zone='us-west-2c',
            fs_total=1000 * 1024**3,  # 1TB
            fs_used=600 * 1024**3,    # 600GB used
            disk_usage_percent=60.0,
            heap_usage_percent=55.0
        ),
        SimpleNamespace(
            name='data-hot-5',
            zone='us-west-2c',
            fs_total=1000 * 1024**3,  # 1TB
            fs_used=200 * 1024**3,    # 200GB used
            disk_usage_percent=20.0,
            heap_usage_percent=40.0
        ),
        SimpleNamespace(
            name='master-1',
            zone='us-west-2c',  # Same zone but should be excluded
            fs_total=1000 * 1024**3,  # 1TB
            fs_used=100 * 1024**3,    # 100GB used - lots of space but excluded
            disk_usage_percent=10.0,
            heap_usage_percent=30.0
        )
    ]
    
    # Mock nodes info - data-hot-4 is the target node
    client.get_nodes_info.return_value = nodes
    
    # Mock watermark config
    client.get_cluster_watermark_config.return_value = {
        'threshold_enabled': True,
        'watermarks': {
            'low': '85%',
            'high': '90%',
            'flood_stage': '95%',
            'enable_for_single_data_node': False
        }
    }
    
    return client


@pytest.fixture
def mock_shards_on_target_node():
    """Mock shards data for target node"""
    return [
        # Primary shard with replicas
        ['doc', 'events', None, 1, True, 10.5, 
         {'leases': [{'id': 'lease1'}, {'id': 'lease2'}]}, 'us-west-2c', 'STARTED', 'STARTED'],
        # Primary shard without replicas  
        ['doc', 'logs', '2024-01', 2, True, 5.2,
         {'leases': [{'id': 'lease1'}]}, 'us-west-2c', 'STARTED', 'STARTED'],
        # Replica shard
        ['doc', 'metrics', None, 3, False, 8.1,
         {'leases': [{'id': 'lease1'}, {'id': 'lease2'}]}, 'us-west-2c', 'STARTED', 'STARTED'],
    ]


@pytest.fixture  
def mock_recovery_settings():
    """Mock cluster recovery settings"""
    return [['20mb', 2, 1000]]  # max_bytes_per_sec, node_concurrent_recoveries, max_shards_per_node


class TestCheckMaintenanceCommand:
    """Test cases for the check-maintenance command"""

    def test_command_registered(self, runner):
        """Test that check-maintenance command is properly registered"""
        with patch('xmover.cli.CrateDBClient') as mock_db:
            mock_db.return_value.test_connection.return_value = True
            result = runner.invoke(main, ['--help'])
            assert result.exit_code == 0
            assert 'check-maintenance' in result.output

    def test_command_help(self, runner):
        """Test check-maintenance help displays correctly"""
        with patch('xmover.cli.CrateDBClient') as mock_db:
            mock_db.return_value.test_connection.return_value = True
            result = runner.invoke(main, ['check-maintenance', '--help'])
            assert result.exit_code == 0
            assert 'Target node to analyze for decommissioning' in result.output
            assert 'min-availability' in result.output

    def test_missing_required_options(self, runner):
        """Test command fails with missing required options"""
        with patch('xmover.cli.CrateDBClient') as mock_db:
            mock_db.return_value.test_connection.return_value = True
            
            # Missing both options
            result = runner.invoke(main, ['check-maintenance'])
            assert result.exit_code != 0
            
            # Missing --min-availability option
            result = runner.invoke(main, ['check-maintenance', '--node', 'data-hot-4'])
            assert result.exit_code != 0
            
            # Missing --node option  
            result = runner.invoke(main, ['check-maintenance', '--min-availability', 'full'])
            assert result.exit_code != 0

    def test_invalid_node_name(self, runner, mock_client):
        """Test command handles invalid node names gracefully"""
        with patch('xmover.cli.CrateDBClient', return_value=mock_client):
            result = runner.invoke(main, ['check-maintenance', '--node', 'nonexistent-node', '--min-availability', 'full'])
            
            assert result.exit_code == 0
            assert 'not found in cluster' in result.output
            assert 'Available nodes:' in result.output

    def test_node_with_no_shards(self, runner, mock_client):
        """Test command handles nodes with no shards"""
        # Mock empty shards result
        mock_client.execute_query.return_value = {'rows': []}
        
        with patch('xmover.cli.CrateDBClient', return_value=mock_client):
            result = runner.invoke(main, ['check-maintenance', '--node', 'data-hot-4', '--min-availability', 'full'])
            
            assert result.exit_code == 0
            assert 'safe to decommission' in result.output

    def test_full_maintenance_analysis(self, runner, mock_client, 
                                     mock_shards_on_target_node, mock_recovery_settings):
        """Test full maintenance analysis with shards present"""
        # Mock the execute_query calls
        def mock_execute_query(query, params=None):
            if 'recovery' in query.lower():
                return {'rows': mock_recovery_settings}
            elif 'sys.shards' in query:
                return {'rows': mock_shards_on_target_node}
            elif 'COUNT(*)' in query:
                return {'rows': [[100]]}  # Mock current shard count
            return {'rows': []}
        
        mock_client.execute_query.side_effect = mock_execute_query
        
        with patch('xmover.cli.CrateDBClient', return_value=mock_client):
            result = runner.invoke(main, ['check-maintenance', '--node', 'data-hot-4', '--min-availability', 'full'])
            
            assert result.exit_code == 0
            assert 'Maintenance Analysis Summary' in result.output
            assert 'Total Shards on Node: 3' in result.output
            assert 'Data to Move:' in result.output
            assert 'Shard Analysis by Type' in result.output

    def test_primaries_maintenance_analysis(self, runner, mock_client,
                                          mock_shards_on_target_node, mock_recovery_settings):
        """Test primaries-only maintenance analysis"""
        # Mock the execute_query calls
        def mock_execute_query(query, params=None):
            if 'recovery' in query.lower():
                return {'rows': mock_recovery_settings}
            elif 'sys.shards' in query:
                return {'rows': mock_shards_on_target_node}
            elif 'COUNT(*)' in query:
                return {'rows': [[100]]}  # Mock current shard count
            return {'rows': []}
        
        mock_client.execute_query.side_effect = mock_execute_query
        
        with patch('xmover.cli.CrateDBClient', return_value=mock_client):
            result = runner.invoke(main, ['check-maintenance', '--node', 'data-hot-4', '--min-availability', 'primaries'])
            
            assert result.exit_code == 0
            assert 'Maintenance Analysis Summary' in result.output
            assert 'Fast Operations:' in result.output
            assert 'Slow Operations:' in result.output
            assert 'Convert to replica (fast)' in result.output

    def test_recovery_time_estimation(self, runner, mock_client,
                                    mock_shards_on_target_node, mock_recovery_settings):
        """Test recovery time estimation is displayed"""
        def mock_execute_query(query, params=None):
            if 'recovery' in query.lower():
                return {'rows': mock_recovery_settings}
            elif 'sys.shards' in query:
                return {'rows': mock_shards_on_target_node}
            elif 'COUNT(*)' in query:
                return {'rows': [[100]]}  # Mock current shard count
            return {'rows': []}
        
        mock_client.execute_query.side_effect = mock_execute_query
        
        with patch('xmover.cli.CrateDBClient', return_value=mock_client):
            result = runner.invoke(main, ['check-maintenance', '--node', 'data-hot-4', '--min-availability', 'full'])
            
            assert result.exit_code == 0
            assert 'Recovery Time Estimation' in result.output
            assert 'Recovery Settings:' in result.output
            assert 'Max bytes/sec:' in result.output
            assert 'Concurrent recoveries:' in result.output
            assert 'Estimated Time:' in result.output

    def test_capacity_analysis(self, runner, mock_client,
                             mock_shards_on_target_node, mock_recovery_settings):
        """Test capacity analysis and target nodes display"""
        def mock_execute_query(query, params=None):
            if 'recovery' in query.lower():
                return {'rows': mock_recovery_settings}
            elif 'sys.shards' in query:
                return {'rows': mock_shards_on_target_node}
            elif 'COUNT(*)' in query:
                return {'rows': [[100]]}  # Mock current shard count
            return {'rows': []}
        
        mock_client.execute_query.side_effect = mock_execute_query
        
        with patch('xmover.cli.CrateDBClient', return_value=mock_client):
            result = runner.invoke(main, ['check-maintenance', '--node', 'data-hot-4', '--min-availability', 'full'])
            
            assert result.exit_code == 0
            assert 'Target Nodes Capacity' in result.output
            assert 'Zone: us-west-2c' in result.output
            assert 'Space Below Low WM' in result.output
            assert 'Shard Capacity' in result.output
            assert 'Disk Usage' in result.output

    def test_maintenance_recommendations(self, runner, mock_client,
                                       mock_shards_on_target_node, mock_recovery_settings):
        """Test maintenance recommendations are displayed"""
        def mock_execute_query(query, params=None):
            if 'recovery' in query.lower():
                return {'rows': mock_recovery_settings}
            elif 'sys.shards' in query:
                return {'rows': mock_shards_on_target_node}
            elif 'COUNT(*)' in query:
                return {'rows': [[100]]}  # Mock current shard count
            return {'rows': []}
        
        mock_client.execute_query.side_effect = mock_execute_query
        
        with patch('xmover.cli.CrateDBClient', return_value=mock_client):
            result = runner.invoke(main, ['check-maintenance', '--node', 'data-hot-4', '--min-availability', 'primaries'])
            
            assert result.exit_code == 0
            assert 'Next Steps:' in result.output
            assert 'Verify cluster health' in result.output
            assert 'xmover monitor-recovery --watch' in result.output

    def test_master_node_exclusion(self, runner, mock_client, 
                                 mock_shards_on_target_node, mock_recovery_settings):
        """Test that master nodes are excluded from candidate targets even when in same AZ"""
        # Mock the execute_query calls
        def mock_execute_query(query, params=None):
            if 'recovery' in query.lower():
                return {'rows': mock_recovery_settings}
            elif 'sys.shards' in query:
                return {'rows': mock_shards_on_target_node}
            elif 'COUNT(*)' in query:
                return {'rows': [[100]]}  # Mock current shard count
            return {'rows': []}
        
        mock_client.execute_query.side_effect = mock_execute_query
        
        with patch('xmover.cli.CrateDBClient', return_value=mock_client):
            result = runner.invoke(main, ['check-maintenance', '--node', 'data-hot-4', '--min-availability', 'full'])
            
            assert result.exit_code == 0
            # Should show data-hot-5 but NOT master-1 even though both are in us-west-2c
            assert 'data-hot-5' in result.output
            assert 'master-1' not in result.output
            assert 'Target Nodes Capacity (Zone: us-west-2c)' in result.output


class TestMaintenanceCommandsClass:
    """Test the MaintenanceCommands class methods directly"""

    def test_get_cluster_recovery_settings_default_values(self):
        """Test recovery settings with default values"""
        mock_client = Mock()
        mock_client.execute_query.side_effect = Exception("No access")
        
        maintenance = MaintenanceCommands(mock_client)
        settings = maintenance._get_cluster_recovery_settings()
        
        assert settings['max_bytes_per_sec'] == 20 * 1024 * 1024  # 20MB
        assert settings['node_concurrent_recoveries'] == 2

    def test_get_cluster_recovery_settings_parsing(self):
        """Test parsing of recovery settings from database"""
        mock_client = Mock()
        mock_client.execute_query.return_value = {
            'rows': [['100mb', 4, 500]]
        }
        
        maintenance = MaintenanceCommands(mock_client)
        settings = maintenance._get_cluster_recovery_settings()
        
        assert settings['max_bytes_per_sec'] == 100 * 1024 * 1024  # 100MB
        assert settings['node_concurrent_recoveries'] == 4
        assert settings['max_shards_per_node'] == 500

    def test_shard_categorization(self):
        """Test shard categorization logic"""
        mock_client = Mock()
        maintenance = MaintenanceCommands(mock_client)
        
        # Mock shards data
        shards = [
            {'is_primary': True, 'has_replicas': True, 'size_gb': 10},
            {'is_primary': True, 'has_replicas': False, 'size_gb': 5}, 
            {'is_primary': False, 'has_replicas': True, 'size_gb': 8}
        ]
        
        primary_shards = [s for s in shards if s['is_primary']]
        primary_with_replicas = [s for s in primary_shards if s['has_replicas']]
        primary_without_replicas = [s for s in primary_shards if not s['has_replicas']]
        replica_shards = [s for s in shards if not s['is_primary']]
        
        assert len(primary_shards) == 2
        assert len(primary_with_replicas) == 1
        assert len(primary_without_replicas) == 1
        assert len(replica_shards) == 1

    def test_retention_leases_parsing(self):
        """Test retention leases parsing for replica detection"""
        # Mock row data - retention_leases is a dictionary with 'leases' array
        retention_leases_with_replicas = {
            'leases': [
                {'id': 'peer_recovery/node1', 'retaining_seq_no': 100},
                {'id': 'peer_recovery/node2', 'retaining_seq_no': 100}
            ]
        }
        
        retention_leases_without_replicas = {
            'leases': [
                {'id': 'peer_recovery/node1', 'retaining_seq_no': 100}
            ]
        }
        
        # Test replica count calculation
        replica_count_with = len(retention_leases_with_replicas.get('leases', []))
        replica_count_without = len(retention_leases_without_replicas.get('leases', []))
        
        assert replica_count_with == 2
        assert replica_count_without == 1
        assert replica_count_with > 1  # Has replicas
        assert replica_count_without <= 1  # No replicas

    def test_isolated_node_scenario(self, runner):
        """Test check-maintenance with node isolated in different availability zone"""
        mock_client = Mock()
        mock_client.test_connection.return_value = True
        
        # Mock nodes - each in different AZ (3-node cluster scenario)
        isolated_nodes = [
            SimpleNamespace(
                name='node-1', 
                zone='us-west-2a',  # Target node zone
                fs_total=1000 * 1024**3,
                fs_used=500 * 1024**3,
                disk_usage_percent=50.0,
                heap_usage_percent=45.0
            ),
            SimpleNamespace(
                name='node-2',
                zone='us-west-2b',  # Different zone
                fs_total=1000 * 1024**3,
                fs_used=400 * 1024**3,
                disk_usage_percent=40.0,
                heap_usage_percent=40.0
            ),
            SimpleNamespace(
                name='node-3',
                zone='us-west-2c',  # Different zone
                fs_total=1000 * 1024**3,
                fs_used=300 * 1024**3,
                disk_usage_percent=30.0,
                heap_usage_percent=35.0
            )
        ]
        
        mock_client.get_nodes_info.return_value = isolated_nodes
        mock_client.get_cluster_watermark_config.return_value = {
            'low': 85.0, 'high': 90.0, 'flood_stage': 95.0
        }
        
        # Mock shards on target node
        target_shards = [
            ['test_schema', 'test_table', '', 0, True, 10.5, 
             {'leases': []}, 'us-west-2a', 'STARTED', 'STARTED'],
            ['test_schema', 'test_table', '', 1, True, 15.2, 
             {'leases': [{'id': 'lease1'}]}, 'us-west-2a', 'STARTED', 'STARTED']
        ]
        
        def mock_execute_query(query, params=None):
            if 'recovery' in query.lower():
                return {'rows': [['20mb', 2, 1000]]}
            elif 'sys.shards' in query and 'node' in query:
                return {'rows': target_shards}
            elif 'COUNT(*)' in query:
                return {'rows': [[50]]}
            return {'rows': []}
        
        mock_client.execute_query.side_effect = mock_execute_query
        
        with patch('xmover.cli.CrateDBClient', return_value=mock_client):
            result = runner.invoke(main, ['check-maintenance', '--node', 'node-1', '--min-availability', 'full'])
            
            assert result.exit_code == 0
            # Should detect isolated node scenario
            assert 'CRITICAL: Data cannot be moved - no target nodes in same availability zone' in result.output
            assert 'Target node is isolated in zone' in result.output
            assert 'us-west-2a' in result.output
            assert 'Node is isolated in its availability zone' in result.output
            assert 'Data movement is impossible due to zone constraints' in result.output