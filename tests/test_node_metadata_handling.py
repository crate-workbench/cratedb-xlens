"""
Tests for robust node metadata handling in XMover

This module tests XMover's ability to gracefully handle CrateDB nodes with
missing, corrupted, or NULL metadata attributes (heap, fs, attributes objects).

Tests the fix for 500 Internal Server Errors caused by NullPointerException
when accessing node metadata in sys.nodes queries.
"""

import pytest
from unittest.mock import Mock, patch, call
from io import StringIO
import sys

from cratedb_xlens.database import CrateDBClient, NodeInfo
from cratedb_xlens.commands.diagnostics import DiagnosticsCommands
from rich.console import Console


class TestNodeMetadataHandling:
    """Test handling of nodes with corrupted/missing metadata"""

    def test_get_nodes_info_with_corrupted_metadata(self):
        """Test that get_nodes_info gracefully handles nodes with NULL/missing metadata"""
        
        # Create real client instance and patch execute_query
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            # Mock the node names query (first query in get_nodes_info)
            mock_execute_query.side_effect = [
                # First call: get node names
                {
                    'rows': [
                        ['node1-id', 'data-hot-1'],
                        ['node2-id', 'data-hot-2'], 
                        ['node3-id', 'data-hot-3'],  # This will be problematic
                        ['node4-id', 'master-1']
                    ]
                },
                # Second call: detailed query for data-hot-1 (healthy)
                {
                    'rows': [
                        ['node1-id', 'data-hot-1', 'us-west-2a', 1000000000, 2000000000, 100000000000, 50000000000, 45000000000]
                    ]
                },
                # Third call: detailed query for data-hot-2 (healthy)  
                {
                    'rows': [
                        ['node2-id', 'data-hot-2', 'us-west-2b', 1500000000, 2000000000, 120000000000, 60000000000, 55000000000]
                    ]
                },
                # Fourth call: detailed query for data-hot-3 (corrupted - raises exception)
                Exception("NullPointerException: Cannot invoke \"java.util.Map.get(Object)\" because \"map\" is null"),
                # Fifth call: detailed query for master-1 (healthy)
                {
                    'rows': [
                        ['node4-id', 'master-1', 'us-west-2c', 500000000, 1000000000, 50000000000, 20000000000, 28000000000]
                    ]
                }
            ]
            
            # Capture stdout to check for warning messages
            captured_output = StringIO()
            with patch('sys.stdout', captured_output):
                # Execute the method
                nodes = client.get_nodes_info()
            
            # Verify all nodes are returned
            assert len(nodes) == 4
            
            # Verify healthy nodes have correct data
            healthy_nodes = [n for n in nodes if n.name in ['data-hot-1', 'data-hot-2', 'master-1']]
            assert len(healthy_nodes) == 3
            
            data_hot_1 = next(n for n in nodes if n.name == 'data-hot-1')
            assert data_hot_1.zone == 'us-west-2a'
            assert data_hot_1.heap_used == 1000000000
            assert data_hot_1.heap_max == 2000000000
            
            # Verify corrupted node has fallback values
            problematic_node = next(n for n in nodes if n.name == 'data-hot-3')
            assert problematic_node.zone == 'unknown'
            assert problematic_node.heap_used == 0
            assert problematic_node.heap_max == 1
            assert problematic_node.fs_total == 0
            assert problematic_node.fs_used == 0
            assert problematic_node.fs_available == 0
            
            # Verify warning message was printed
            output = captured_output.getvalue()
            assert "Warning: 1 node(s) have corrupted/missing metadata" in output
            assert "data-hot-3: Using default values" in output
            assert "check CrateDB logs for details" in output
            
            # Verify correct number of execute_query calls
            assert mock_execute_query.call_count == 5

    def test_get_nodes_info_multiple_corrupted_nodes(self):
        """Test handling multiple nodes with corrupted metadata"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            # Mock responses - two corrupted nodes
            mock_execute_query.side_effect = [
                # Node names query
                {
                    'rows': [
                        ['node1-id', 'data-hot-1'],
                        ['node2-id', 'data-hot-2'],
                        ['node3-id', 'data-hot-3']
                    ]
                },
                # data-hot-1: healthy
                {
                    'rows': [
                        ['node1-id', 'data-hot-1', 'us-west-2a', 1000000000, 2000000000, 100000000000, 50000000000, 45000000000]
                    ]
                },
                # data-hot-2: corrupted
                Exception("NullPointerException"),
                # data-hot-3: corrupted  
                Exception("500 Server Error")
            ]
            
            captured_output = StringIO()
            with patch('sys.stdout', captured_output):
                nodes = client.get_nodes_info()
            
            # Verify all nodes returned
            assert len(nodes) == 3
            
            # Verify one healthy, two with fallback values
            healthy_nodes = [n for n in nodes if n.heap_max > 1]
            corrupted_nodes = [n for n in nodes if n.heap_max == 1]
            
            assert len(healthy_nodes) == 1
            assert len(corrupted_nodes) == 2
            
            # Verify warning message mentions both nodes
            output = captured_output.getvalue()
            assert "Warning: 2 node(s) have corrupted/missing metadata" in output
            assert "data-hot-2" in output
            assert "data-hot-3" in output

    def test_get_nodes_info_no_corrupted_nodes(self):
        """Test normal operation when all nodes are healthy"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            mock_execute_query.side_effect = [
                # Node names query
                {
                    'rows': [
                        ['node1-id', 'data-hot-1'],
                        ['node2-id', 'data-hot-2']
                    ]
                },
                # Both nodes healthy
                {
                    'rows': [
                        ['node1-id', 'data-hot-1', 'us-west-2a', 1000000000, 2000000000, 100000000000, 50000000000, 45000000000]
                    ]
                },
                {
                    'rows': [
                        ['node2-id', 'data-hot-2', 'us-west-2b', 1500000000, 2000000000, 120000000000, 60000000000, 55000000000]
                    ]
                }
            ]
            
            captured_output = StringIO()
            with patch('sys.stdout', captured_output):
                nodes = client.get_nodes_info()
            
            # Verify no warning messages
            output = captured_output.getvalue()
            assert "Warning" not in output
            assert "corrupted" not in output
            
            # Verify all nodes have real data
            assert len(nodes) == 2
            assert all(n.heap_max > 1 for n in nodes)
            assert all(n.zone != 'unknown' for n in nodes)

    def test_get_nodes_info_empty_node_list(self):
        """Test handling when no nodes are returned"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            # Mock empty response
            mock_execute_query.return_value = {'rows': []}
            
            nodes = client.get_nodes_info()
            
            assert len(nodes) == 0
            assert mock_execute_query.call_count == 1

    def test_get_nodes_info_node_names_query_fails(self):
        """Test handling when the initial node names query fails"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            # Mock initial query failure
            mock_execute_query.side_effect = Exception("Connection timeout")
            
            nodes = client.get_nodes_info()
            
            # Should return empty list gracefully
            assert len(nodes) == 0
            assert mock_execute_query.call_count == 1

    def test_test_connection_with_verbose_corrupted_metadata(self):
        """Test test-connection --verbose command shows corrupted metadata warnings"""
        
        # Mock client and nodes
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'GREEN',
            'green_entities': 100,
            'yellow_entities': 0,
            'red_entities': 0,
            'total_tables': 50,
            'total_partitions': 10
        }
        
        # Mix of healthy and corrupted nodes
        mock_nodes = [
            NodeInfo(
                id='node1', name='data-hot-1', zone='us-west-2a',
                heap_used=1000000000, heap_max=2000000000,
                fs_total=100000000000, fs_used=50000000000, fs_available=45000000000
            ),
            NodeInfo(
                id='node2', name='data-hot-2', zone='unknown',
                heap_used=0, heap_max=1,  # Indicates corrupted metadata
                fs_total=0, fs_used=0, fs_available=0
            ),
            NodeInfo(
                id='node3', name='master-1', zone='us-west-2b',
                heap_used=500000000, heap_max=1000000000,
                fs_total=50000000000, fs_used=20000000000, fs_available=28000000000
            )
        ]
        mock_client.get_nodes_info.return_value = mock_nodes
        
        # Create console and capture output
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        # Run with verbose=True
        cmd.test_connection(verbose=True)
        
        output = console.file.getvalue()
        
        # Verify detailed node information is shown
        assert "📋 Detailed Node Information:" in output
        assert "data-hot-1" in output
        assert "data-hot-2" in output
        assert "master-1" in output
        
        # Verify corrupted node is marked appropriately
        assert "Metadata unavailable" in output
        assert "⚠️" in output
        
        # Verify healthy nodes show resource information
        assert "Heap" in output
        assert "Disk" in output
        assert "GB" in output

    def test_test_connection_without_verbose_no_detailed_info(self):
        """Test test-connection without --verbose doesn't show detailed node info"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'GREEN',
            'total_tables': 50,
            'total_partitions': 10
        }
        mock_client.get_nodes_info.return_value = [
            NodeInfo(id='node1', name='data-hot-1', zone='us-west-2a',
                    heap_used=1000000000, heap_max=2000000000,
                    fs_total=100000000000, fs_used=50000000000, fs_available=45000000000)
        ]
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        # Run with verbose=False (default)
        cmd.test_connection(verbose=False)
        
        output = console.file.getvalue()
        
        # Verify detailed information is NOT shown
        assert "📋 Detailed Node Information:" not in output
        assert "Heap" not in output or output.count("Heap") == 0  # No heap percentages shown

    @patch('builtins.print')
    def test_node_metadata_warning_format(self, mock_print):
        """Test the exact format of node metadata warning messages"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            mock_execute_query.side_effect = [
                {'rows': [['node1-id', 'problematic-node']]},  # Node names
                Exception("NullPointerException")  # Detailed query fails
            ]
            
            nodes = client.get_nodes_info()
            
            # Verify the exact warning message format
            expected_calls = [
                call("⚠️  Warning: 1 node(s) have corrupted/missing metadata:"),
                call("   • problematic-node: Using default values (heap, filesystem, zone data unavailable)"),
                call("   💡 This may indicate node issues - check CrateDB logs for details")
            ]
            
            mock_print.assert_has_calls(expected_calls)

    def test_fallback_node_values_are_safe(self):
        """Test that fallback values prevent division by zero and other errors"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            mock_execute_query.side_effect = [
                {'rows': [['node1-id', 'corrupted-node']]},
                Exception("Corrupted metadata")
            ]
            
            nodes = client.get_nodes_info()
            node = nodes[0]
            
            # Verify fallback values are safe for calculations
            assert node.heap_max > 0  # Prevents division by zero
            assert node.heap_used >= 0  # Non-negative
            assert node.fs_total >= 0  # Non-negative
            assert node.fs_used >= 0   # Non-negative
            assert node.fs_available >= 0  # Non-negative
            
            # Test common calculations don't crash
            heap_percentage = (node.heap_used / node.heap_max) * 100
            assert heap_percentage >= 0
            
            # Disk percentage calculation (when fs_total is 0, should handle gracefully)
            if node.fs_total > 0:
                disk_percentage = (node.fs_used / node.fs_total) * 100
                assert disk_percentage >= 0

    def test_node_info_properties_with_corrupted_data(self):
        """Test NodeInfo properties work correctly with corrupted data"""
        
        # Create a node with fallback values
        corrupted_node = NodeInfo(
            id='corrupted-id',
            name='corrupted-node',
            zone='unknown',
            heap_used=0,
            heap_max=1,
            fs_total=0,
            fs_used=0,
            fs_available=0
        )
        
        # Verify all properties are accessible and safe
        assert corrupted_node.id == 'corrupted-id'
        assert corrupted_node.name == 'corrupted-node'
        assert corrupted_node.zone == 'unknown'
        
        # Test calculated properties
        assert corrupted_node.disk_usage_percent == 0
        assert corrupted_node.available_space_gb == 0
        assert corrupted_node.heap_usage_percent == 0

    def test_mixed_healthy_and_corrupted_zones(self):
        """Test zone counting with mix of healthy and corrupted nodes"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'GREEN',
            'total_tables': 50,
            'total_partitions': 10
        }
        
        # Mix of nodes with real zones and unknown zones
        mock_nodes = [
            NodeInfo(id='1', name='healthy-1', zone='us-west-2a', heap_used=100, heap_max=200, fs_total=1000, fs_used=500, fs_available=500),
            NodeInfo(id='2', name='corrupted-1', zone='unknown', heap_used=0, heap_max=1, fs_total=0, fs_used=0, fs_available=0),
            NodeInfo(id='3', name='healthy-2', zone='us-west-2b', heap_used=100, heap_max=200, fs_total=1000, fs_used=500, fs_available=500),
            NodeInfo(id='4', name='corrupted-2', zone='unknown', heap_used=0, heap_max=1, fs_total=0, fs_used=0, fs_available=0),
        ]
        mock_client.get_nodes_info.return_value = mock_nodes
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection()
        output = console.file.getvalue()
        
        # Should only count real zones, not 'unknown' zones from corrupted nodes
        assert "Zones: 2 (us-west-2a, us-west-2b)" in output
        assert "Nodes: 4" in output


class TestClusterHealthSummary:
    """Test cluster health summary functionality"""

    def test_get_cluster_health_summary_success(self):
        """Test successful cluster health summary retrieval"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            # Mock the cluster health query response
            mock_execute_query.return_value = {
                'rows': [
                    ['GREEN', 95, 0, 3, 0, 2, 0, 0, 0, 150, 45]  # health, green, green_under, yellow, yellow_under, red, red_under, other, other_under, tables, partitions
                ]
            }
            
            health = client.get_cluster_health_summary()
            
            # Verify the method was called
            mock_execute_query.assert_called_once()
            
            # Verify returned data structure
            assert health['cluster_health'] == 'GREEN'
            assert health['green_entities'] == 95
            assert health['green_underreplicated_shards'] == 0
            assert health['yellow_entities'] == 3
            assert health['yellow_underreplicated_shards'] == 0
            assert health['red_entities'] == 2
            assert health['red_underreplicated_shards'] == 0
            assert health['other_entities'] == 0
            assert health['other_underreplicated_shards'] == 0
            assert health['total_tables'] == 150
            assert health['total_partitions'] == 45

    def test_get_cluster_health_summary_with_issues(self):
        """Test cluster health summary with RED/YELLOW entities"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            mock_execute_query.return_value = {
                'rows': [
                    ['YELLOW', 85, 0, 10, 5, 5, 3, 0, 0, 120, 30]
                ]
            }
            
            health = client.get_cluster_health_summary()
            
            assert health['cluster_health'] == 'YELLOW'
            assert health['red_entities'] == 5
            assert health['red_underreplicated_shards'] == 3
            assert health['yellow_entities'] == 10
            assert health['yellow_underreplicated_shards'] == 5
            assert health['green_entities'] == 85
            assert health['green_underreplicated_shards'] == 0

    def test_get_cluster_health_summary_query_failure(self):
        """Test handling of cluster health query failure"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            mock_execute_query.side_effect = Exception("Health query failed")
            
            health = client.get_cluster_health_summary()
            
            # Should return None on failure
            assert health is None

    def test_test_connection_displays_cluster_health(self):
        """Test that test-connection command displays cluster health correctly"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'GREEN',
            'green_entities': 100,
            'yellow_entities': 0,
            'red_entities': 0,
            'total_tables': 75,
            'total_partitions': 25
        }
        mock_client.get_nodes_info.return_value = []
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection()
        output = console.file.getvalue()
        
        assert "🏥 Cluster Health: GREEN" in output
        assert "Tables: 75, Partitions: 25" in output

    def test_test_connection_displays_health_issues(self):
        """Test that test-connection shows health issues when present"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'YELLOW',
            'green_entities': 85,
            'yellow_entities': 10,
            'red_entities': 5,
            'total_tables': 100,
            'total_partitions': 50
        }
        mock_client.get_nodes_info.return_value = []
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection()
        output = console.file.getvalue()
        
        assert "🏥 Cluster Health: YELLOW" in output
        assert "Issues: 5 RED, 10 YELLOW entities" in output

    def test_test_connection_handles_health_query_failure(self):
        """Test graceful handling of health query failure"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = None  # Simulates query failure
        mock_client.get_nodes_info.return_value = []
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection()
        output = console.file.getvalue()
        
        # Should not crash and should not show health info
        assert "🏥 Cluster Health:" not in output
        assert "✅ Successfully connected" in output


class TestVerboseDiagnostics:
    """Test verbose diagnostic output"""

    def test_verbose_shows_node_resource_details(self):
        """Test that --verbose shows detailed node resource information"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'GREEN',
            'total_tables': 50,
            'total_partitions': 10
        }
        
        # Node with high resource usage
        high_usage_node = NodeInfo(
            id='high-node', name='data-hot-high', zone='us-west-2a',
            heap_used=1800000000, heap_max=2000000000,  # 90% heap
            fs_total=100000000000, fs_used=92000000000, fs_available=8000000000  # 92% disk
        )
        
        # Node with normal usage
        normal_node = NodeInfo(
            id='normal-node', name='data-hot-normal', zone='us-west-2b',
            heap_used=500000000, heap_max=2000000000,  # 25% heap
            fs_total=100000000000, fs_used=50000000000, fs_available=50000000000  # 50% disk
        )
        
        mock_client.get_nodes_info.return_value = [high_usage_node, normal_node]
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection(verbose=True)
        output = console.file.getvalue()
        
        # Verify detailed information is shown
        assert "📋 Detailed Node Information:" in output
        assert "data-hot-high" in output
        assert "data-hot-normal" in output
        
        # Verify resource percentages are calculated and displayed
        assert "90.0%" in output  # High heap usage
        assert "92.0%" in output  # High disk usage
        assert "25.0%" in output  # Normal heap usage
        assert "50.0%" in output  # Normal disk usage
        
        # Verify status indicators for high usage
        assert "🔥" in output or "⚠️" in output  # High resource indicators
        assert "💾" in output or "📁" in output  # Disk usage indicators

    def test_verbose_shows_status_indicators(self):
        """Test that verbose mode shows appropriate status indicators"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {'cluster_health': 'GREEN', 'total_tables': 1, 'total_partitions': 1}
        
        # Critical node (>90% heap, >90% disk)
        critical_node = NodeInfo(
            id='critical', name='critical-node', zone='us-west-2a',
            heap_used=1950000000, heap_max=2000000000,  # 97.5% heap
            fs_total=100000000000, fs_used=95000000000, fs_available=5000000000  # 95% disk
        )
        
        # Warning node (>75% heap, >85% disk)
        warning_node = NodeInfo(
            id='warning', name='warning-node', zone='us-west-2b',
            heap_used=1600000000, heap_max=2000000000,  # 80% heap
            fs_total=100000000000, fs_used=87000000000, fs_available=13000000000  # 87% disk
        )
        
        # Healthy node
        healthy_node = NodeInfo(
            id='healthy', name='healthy-node', zone='us-west-2c',
            heap_used=500000000, heap_max=2000000000,  # 25% heap
            fs_total=100000000000, fs_used=30000000000, fs_available=70000000000  # 30% disk
        )
        
        mock_client.get_nodes_info.return_value = [critical_node, warning_node, healthy_node]
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection(verbose=True)
        output = console.file.getvalue()
        
        # Verify status indicators appear
        assert "🔥" in output  # Critical heap indicator
        assert "💾" in output  # Critical disk indicator  
        assert "⚠️" in output  # Warning heap indicator
        assert "📁" in output  # Warning disk indicator
        assert "✅" in output  # Healthy indicator

    def test_verbose_handles_metadata_unavailable_nodes(self):
        """Test verbose mode properly handles nodes with unavailable metadata"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {'cluster_health': 'GREEN', 'total_tables': 1, 'total_partitions': 1}
        
        # Healthy node
        healthy_node = NodeInfo(
            id='healthy', name='healthy-node', zone='us-west-2a',
            heap_used=1000000000, heap_max=2000000000,
            fs_total=100000000000, fs_used=50000000000, fs_available=50000000000
        )
        
        # Node with corrupted metadata (fallback values)
        corrupted_node = NodeInfo(
            id='corrupted', name='corrupted-node', zone='unknown',
            heap_used=0, heap_max=1, fs_total=0, fs_used=0, fs_available=0
        )
        
        mock_client.get_nodes_info.return_value = [healthy_node, corrupted_node]
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection(verbose=True)
        output = console.file.getvalue()
        
        # Verify healthy node shows normal info
        assert "healthy-node" in output
        assert "50.0%" in output  # Heap and disk percentages
        
        # Verify corrupted node shows metadata unavailable message
        assert "corrupted-node" in output
        assert "Metadata unavailable" in output
        assert "⚠️" in output

    def test_non_verbose_does_not_show_detailed_info(self):
        """Test that non-verbose mode doesn't show detailed node information"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {'cluster_health': 'GREEN', 'total_tables': 1, 'total_partitions': 1}
        
        nodes = [
            NodeInfo(id='1', name='node1', zone='us-west-2a', heap_used=1000000000, heap_max=2000000000,
                    fs_total=100000000000, fs_used=50000000000, fs_available=50000000000)
        ]
        mock_client.get_nodes_info.return_value = nodes
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection(verbose=False)
        output = console.file.getvalue()
        
        # Should not show detailed node information
        assert "📋 Detailed Node Information:" not in output
        assert "50.0%" not in output  # No percentage details
        assert "GB free" not in output  # No GB details
        
        # Should still show basic cluster info
        assert "Nodes: 1" in output
        assert "Zones: 1 (us-west-2a)" in output


class TestErrorHandlingRobustness:
    """Test comprehensive error handling scenarios"""

    def test_connection_failure_graceful_handling(self):
        """Test graceful handling of connection failures"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = False
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection()
        output = console.file.getvalue()
        
        assert "❌ Failed to connect to CrateDB cluster" in output
        assert "💡 Check your connection configuration" in output

    def test_nodes_info_partial_failure_handling(self):
        """Test handling when nodes info partially fails"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {'cluster_health': 'GREEN', 'total_tables': 1, 'total_partitions': 1}
        mock_client.get_nodes_info.side_effect = Exception("Nodes query failed")
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection()
        output = console.file.getvalue()
        
        # Should handle gracefully
        assert "✅ Successfully connected" in output
        assert "⚠️  Basic cluster info unavailable" in output

    def test_get_nodes_info_individual_node_query_timeout(self):
        """Test handling of individual node query timeouts"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            # First query returns node names successfully  
            # Second query times out for one node
            mock_execute_query.side_effect = [
                {'rows': [['node1', 'healthy-node'], ['node2', 'timeout-node']]},  # Node names
                {'rows': [['node1', 'healthy-node', 'us-west-2a', 1000, 2000, 100000, 50000, 45000]]},  # Healthy node data
                Exception("Query timeout")  # Timeout node fails
            ]
            
            captured_output = StringIO()
            with patch('sys.stdout', captured_output):
                nodes = client.get_nodes_info()
            
            # Should return both nodes, one with fallback data
            assert len(nodes) == 2
            
            # Healthy node should have real data
            healthy_node = next(n for n in nodes if n.name == 'healthy-node')
            assert healthy_node.zone == 'us-west-2a'
            assert healthy_node.heap_max == 2000
            
            # Timeout node should have fallback data
            timeout_node = next(n for n in nodes if n.name == 'timeout-node')
            assert timeout_node.zone == 'unknown'
            assert timeout_node.heap_max == 1
            
            # Should log warning
            output = captured_output.getvalue()
            assert "Warning: 1 node(s) have corrupted/missing metadata" in output
            assert "timeout-node" in output

    def test_exception_during_test_connection_main_flow(self):
        """Test exception handling in main test_connection flow"""
        
        # Mock client that throws during initialization
        with patch('cratedb_xlens.database.CrateDBClient') as mock_client_class:
            mock_client_class.side_effect = Exception("Database initialization failed")
            
            console = Console(file=StringIO(), width=120, force_terminal=False)
            cmd = DiagnosticsCommands(None)
            cmd.console = console
            
            # Should not crash
            cmd.test_connection(connection_string="invalid://connection")
            
            output = console.file.getvalue()
            assert "Error in testing connection" in output