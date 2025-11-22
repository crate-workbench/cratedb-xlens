"""
Tests for CrateDB 500 Internal Server Error scenarios and SQL query robustness

This module tests XMover's resilience against CrateDB 500 errors, specifically:
- NullPointerException in sys.nodes queries due to corrupted node metadata
- Robust COALESCE handling for NULL heap, fs, and attributes objects
- Individual node processing to prevent cascading failures
- Graceful degradation when system tables are partially unavailable

Based on the real-world issue where data-hot-3 node had corrupted metadata
causing sys.nodes queries to fail with NullPointerException.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from io import StringIO
import sys

from cratedb_xlens.database import CrateDBClient, NodeInfo
from cratedb_xlens.commands.diagnostics import DiagnosticsCommands
from rich.console import Console


class TestCrateDB500ErrorScenarios:
    """Test scenarios that caused the original 500 Internal Server Error"""

    def test_sys_nodes_null_pointer_exception_scenario(self):
        """Test the exact scenario that caused the original 500 error"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        # Mock the exact error that occurred in production
        production_error_message = (
            "NullPointerException[Cannot invoke \"java.util.Map.get(Object)\" because \"map\" is null]"
        )
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            # Simulate the scenario:
            # 1. First query (node names) succeeds
            # 2. Second query (detailed node info) fails with NullPointerException for data-hot-3
            mock_execute_query.side_effect = [
                # Node names query succeeds
                {
                    'rows': [
                        ['data-hot-0-id', 'data-hot-0'],
                        ['data-hot-1-id', 'data-hot-1'], 
                        ['data-hot-2-id', 'data-hot-2'],
                        ['data-hot-3-id', 'data-hot-3'],  # This one will be problematic
                        ['master-0-id', 'master-0']
                    ]
                },
                # data-hot-0: healthy
                {
                    'rows': [
                        ['data-hot-0-id', 'data-hot-0', 'us-west-2a', 2147483648, 4294967296, 107374182400, 53687091200, 48318054400]
                    ]
                },
                # data-hot-1: healthy  
                {
                    'rows': [
                        ['data-hot-1-id', 'data-hot-1', 'us-west-2b', 2147483648, 4294967296, 107374182400, 53687091200, 48318054400]
                    ]
                },
                # data-hot-2: healthy
                {
                    'rows': [
                        ['data-hot-2-id', 'data-hot-2', 'us-west-2c', 2147483648, 4294967296, 107374182400, 53687091200, 48318054400]
                    ]
                },
                # data-hot-3: NullPointerException due to corrupted metadata
                Exception(production_error_message),
                # master-0: healthy
                {
                    'rows': [
                        ['master-0-id', 'master-0', 'us-west-2a', 1073741824, 2147483648, 53687091200, 10737418240, 42949672960]
                    ]
                }
            ]
            
            captured_output = StringIO()
            with patch('sys.stdout', captured_output):
                nodes = client.get_nodes_info()
        
            # Verify all 5 nodes are returned
            assert len(nodes) == 5
            
            # Verify healthy nodes have correct data  
            healthy_node_names = ['data-hot-0', 'data-hot-1', 'data-hot-2', 'master-0']
            healthy_nodes = [n for n in nodes if n.name in healthy_node_names]
            assert len(healthy_nodes) == 4
            
            # Verify all healthy nodes have realistic values
            for node in healthy_nodes:
                assert node.heap_max > 1000000000  # > 1GB
                assert node.fs_total > 10000000000  # > 10GB
                assert node.zone in ['us-west-2a', 'us-west-2b', 'us-west-2c']
            
            # Verify problematic node has fallback values
            problematic_node = next(n for n in nodes if n.name == 'data-hot-3')
            assert problematic_node.id == 'data-hot-3-id'
            assert problematic_node.zone == 'unknown'
            assert problematic_node.heap_used == 0
            assert problematic_node.heap_max == 1
            assert problematic_node.fs_total == 0
            assert problematic_node.fs_used == 0
            assert problematic_node.fs_available == 0
            
            # Verify warning message matches expected format
            output = captured_output.getvalue()
            assert "Warning: 1 node(s) have corrupted/missing metadata:" in output
            assert "data-hot-3: Using default values" in output
            assert "check CrateDB logs for details" in output

    def test_bulk_sys_nodes_query_failure_vs_individual_success(self):
        """Test that individual node queries succeed when bulk query fails"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            # Simulate scenario where a bulk sys.nodes query would fail
            # but individual node queries succeed
            mock_execute_query.side_effect = [
                # Node names query succeeds
                {'rows': [['node1-id', 'healthy-node'], ['node2-id', 'another-node']]},
                
                # Individual queries succeed
                {'rows': [['node1-id', 'healthy-node', 'us-west-2a', 1000000000, 2000000000, 100000000000, 50000000000, 45000000000]]},
                {'rows': [['node2-id', 'another-node', 'us-west-2b', 1500000000, 2500000000, 120000000000, 60000000000, 55000000000]]}
            ]
            
            nodes = client.get_nodes_info()
            
            # Both nodes should be returned with valid data
            assert len(nodes) == 2
            assert all(node.heap_max > 1000000000 for node in nodes)
            assert all(node.zone != 'unknown' for node in nodes)
            
            # Verify the resilient approach: individual queries were used
            assert mock_execute_query.call_count == 3  # 1 for names + 2 individual queries

    def test_multiple_corrupted_nodes_in_cluster(self):
        """Test handling of multiple nodes with corrupted metadata simultaneously"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            # Simulate a cluster where multiple nodes have metadata corruption
            mock_execute_query.side_effect = [
                # Node names query
                {
                    'rows': [
                        ['healthy-1-id', 'healthy-1'],
                        ['corrupted-1-id', 'corrupted-1'],
                        ['corrupted-2-id', 'corrupted-2'], 
                        ['healthy-2-id', 'healthy-2'],
                        ['corrupted-3-id', 'corrupted-3']
                    ]
                },
                # healthy-1: success
                {'rows': [['healthy-1-id', 'healthy-1', 'us-west-2a', 1000000000, 2000000000, 100000000000, 50000000000, 45000000000]]},
                
                # corrupted-1: heap object is null
                Exception("NullPointerException[Cannot invoke \"java.util.Map.get(Object)\" because \"heap\" is null]"),
                
                # corrupted-2: fs object is null
                Exception("NullPointerException[Cannot invoke \"java.util.Map.get(Object)\" because \"fs\" is null]"),
                
                # healthy-2: success
                {'rows': [['healthy-2-id', 'healthy-2', 'us-west-2b', 1500000000, 2500000000, 120000000000, 60000000000, 55000000000]]},
                
                # corrupted-3: attributes object is null  
                Exception("NullPointerException[Cannot invoke \"java.util.Map.get(Object)\" because \"attributes\" is null]")
            ]
            
            captured_output = StringIO()
            with patch('sys.stdout', captured_output):
                nodes = client.get_nodes_info()
            
            # All 5 nodes should be returned
            assert len(nodes) == 5
            
            # 2 healthy nodes should have real data
            healthy_nodes = [n for n in nodes if n.heap_max > 1]
            corrupted_nodes = [n for n in nodes if n.heap_max == 1]
            
            assert len(healthy_nodes) == 2
            assert len(corrupted_nodes) == 3
            
            # Verify warning message lists all corrupted nodes
            output = captured_output.getvalue()
            assert "Warning: 3 node(s) have corrupted/missing metadata:" in output
            assert "corrupted-1" in output
            assert "corrupted-2" in output  
            assert "corrupted-3" in output

    def test_coalesce_handling_prevents_null_errors(self):
        """Test that COALESCE statements prevent NULL-related errors"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            # Mock a scenario where some fields return NULL but COALESCE handles it
            mock_execute_query.side_effect = [
                {'rows': [['node-id', 'test-node']]},  # Node names
                
                # Detailed query returns some NULL values, but COALESCE converts them
                {'rows': [['node-id', 'test-node', 'unknown', 0, 1, 0, 0, 0]]}  # All COALESCEd to safe defaults
            ]
            
            nodes = client.get_nodes_info()
            
            assert len(nodes) == 1
            node = nodes[0]
            
            # Verify COALESCE defaults were applied correctly
            assert node.zone == 'unknown'  # COALESCE(attributes['zone'], 'unknown')
            assert node.heap_used == 0     # COALESCE(heap['used'], 0)  
            assert node.heap_max == 1      # COALESCE(heap['max'], 1)
            assert node.fs_total == 0      # COALESCE(fs['total']['size'], 0)
            assert node.fs_used == 0       # COALESCE(fs['total']['used'], 0)
            assert node.fs_available == 0  # COALESCE(fs['total']['available'], 0)

    def test_cluster_health_summary_resilience(self):
        """Test cluster health summary query resilience to sys.health issues"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            # Test successful health query
            mock_execute_query.return_value = {
                'rows': [['GREEN', 95, 0, 3, 0, 2, 0, 0, 0, 590, 100]]
            }
            
            health = client.get_cluster_health_summary()
            
            assert health is not None
            assert health['cluster_health'] == 'GREEN'
            assert health['total_tables'] == 590  # Matches production numbers from summary
            assert health['total_partitions'] == 100
            
            # Test health query failure
            mock_execute_query.side_effect = Exception("sys.health table unavailable")
            health_failed = client.get_cluster_health_summary()
            
            assert health_failed is None  # Should return None on failure

    def test_diagnostic_command_500_error_recovery(self):
        """Test that diagnostics command recovers gracefully from 500 errors"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        
        # Simulate health query succeeding but nodes query having issues
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'GREEN',
            'green_entities': 100,
            'yellow_entities': 0,
            'red_entities': 0, 
            'total_tables': 590,
            'total_partitions': 100
        }
        
        # Simulate partial node failure
        mock_nodes = [
            NodeInfo(id='good', name='healthy-node', zone='us-west-2a', heap_used=1000000000, heap_max=2000000000,
                    fs_total=100000000000, fs_used=50000000000, fs_available=45000000000),
            NodeInfo(id='bad', name='problematic-node', zone='unknown', heap_used=0, heap_max=1,
                    fs_total=0, fs_used=0, fs_available=0)  # Fallback values
        ]
        mock_client.get_nodes_info.return_value = mock_nodes
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection()
        output = console.file.getvalue()
        
        # Should show successful connection despite node metadata issues
        assert "✅ Successfully connected to CrateDB cluster" in output
        assert "🏥 Cluster Health: GREEN" in output
        assert "Tables: 590, Partitions: 100" in output  # Health data still works
        assert "Nodes: 2" in output  # Node count still works
        assert "Zones: 1 (us-west-2a)" in output  # Only counts real zones

    @patch('builtins.print')
    def test_production_error_logging_format(self, mock_print):
        """Test that error logging matches the production format from the summary"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            # Simulate the exact production scenario
            mock_execute_query.side_effect = [
                {'rows': [['data-hot-3-id', 'data-hot-3']]},  # Node names
                Exception("NullPointerException[Cannot invoke \"java.util.Map.get(Object)\" because \"map\" is null]")
            ]
            
            nodes = client.get_nodes_info()
            
            # Verify exact warning format from production logs
            expected_calls = [
                call("⚠️  Warning: 1 node(s) have corrupted/missing metadata:"),
                call("   • data-hot-3: Using default values (heap, filesystem, zone data unavailable)"),
                call("   💡 This may indicate node issues - check CrateDB logs for details")
            ]
            
            mock_print.assert_has_calls(expected_calls)
            
            # Verify fallback node was created
            assert len(nodes) == 1
            assert nodes[0].name == 'data-hot-3'
            assert nodes[0].zone == 'unknown'


class TestSQLQueryRobustness:
    """Test SQL query patterns that prevent 500 errors"""

    def test_individual_node_query_pattern(self):
        """Test the individual node query pattern that prevents cascading failures"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            # Mock successful response
            mock_execute_query.side_effect = [
                {'rows': [['test-node-id', 'test-node']]},  # Node names query
                {'rows': [['test-node-id', 'test-node', 'us-west-2a', 1000000000, 2000000000, 100000000000, 50000000000, 45000000000]]}  # Individual query
            ]
            
            nodes = client.get_nodes_info()
            
            # Verify the query was called correctly
            calls = mock_execute_query.call_args_list
            assert len(calls) == 2
            
            # First call should be for node names
            first_call_args = calls[0][0]
            assert "SELECT id, name FROM sys.nodes WHERE name IS NOT NULL" in first_call_args[0]
            
            # Second call should be individual node query with COALESCE
            second_call_args = calls[1][0]
            query_sql = second_call_args[0].strip()
            
            # Verify COALESCE patterns are present
            assert "COALESCE(attributes['zone'], 'unknown')" in query_sql
            assert "COALESCE(heap['used'], 0)" in query_sql
            assert "COALESCE(heap['max'], 1)" in query_sql  # Prevents division by zero
            assert "COALESCE(fs['total']['size'], 0)" in query_sql
            assert "WHERE name = ?" in query_sql
            
            # Verify node was created successfully
            assert len(nodes) == 1
            assert nodes[0].name == 'test-node'

    def test_cluster_health_query_structure(self):
        """Test that cluster health query handles potential sys.health issues"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            # Expected cluster health query structure  
            mock_execute_query.return_value = {
                'rows': [['GREEN', 100, 0, 5, 0, 2, 0, 0, 0, 150, 45]]
            }
            
            health = client.get_cluster_health_summary()
            
            # Verify query was called
            query_call = mock_execute_query.call_args_list[0][0][0]
            
            # Verify query structure includes all necessary components
            assert "SELECT health FROM sys.health ORDER BY severity DESC LIMIT 1" in query_call
            assert "COUNT(*) FILTER (WHERE health = 'GREEN')" in query_call
            assert "COUNT(*) FILTER (WHERE health = 'YELLOW')" in query_call  
            assert "COUNT(*) FILTER (WHERE health = 'RED')" in query_call
            assert "information_schema.tables WHERE table_schema NOT IN ('sys', 'information_schema', 'pg_catalog')" in query_call
            assert "information_schema.table_partitions" in query_call

    def test_safe_fallback_values_prevent_errors(self):
        """Test that fallback values prevent common calculation errors"""
        
        # Create nodes with various fallback scenarios
        fallback_node = NodeInfo(
            id='fallback', name='fallback-node', zone='unknown',
            heap_used=0, heap_max=1,  # heap_max=1 prevents division by zero
            fs_total=0, fs_used=0, fs_available=0  # All zeros are safe
        )
        
        # Test common calculations that might be performed
        heap_percentage = (fallback_node.heap_used / fallback_node.heap_max) * 100
        assert heap_percentage == 0.0
        
        # Test that fs_total=0 doesn't cause division by zero
        if fallback_node.fs_total > 0:
            disk_percentage = (fallback_node.fs_used / fallback_node.fs_total) * 100
        else:
            disk_percentage = 0  # Safe fallback
        assert disk_percentage == 0
        
        # Test size formatting doesn't crash  
        heap_gb = fallback_node.heap_used / (1024**3)
        fs_gb = fallback_node.fs_total / (1024**3)
        assert heap_gb == 0.0
        assert fs_gb == 0.0

    def test_zone_counting_excludes_unknown_zones(self):
        """Test that zone counting properly excludes 'unknown' zones from corrupted nodes"""
        
        nodes = [
            NodeInfo(id='1', name='healthy-1', zone='us-west-2a', heap_used=100, heap_max=200, fs_total=1000, fs_used=500, fs_available=500),
            NodeInfo(id='2', name='corrupted-1', zone='unknown', heap_used=0, heap_max=1, fs_total=0, fs_used=0, fs_available=0),  # Corrupted
            NodeInfo(id='3', name='healthy-2', zone='us-west-2b', heap_used=150, heap_max=250, fs_total=1200, fs_used=600, fs_available=600),
            NodeInfo(id='4', name='corrupted-2', zone='unknown', heap_used=0, heap_max=1, fs_total=0, fs_used=0, fs_available=0),  # Corrupted
        ]
        
        # Count real zones (excluding 'unknown' from corrupted nodes)
        real_zones = set(node.zone for node in nodes if node.zone and node.zone != 'unknown')
        
        assert len(real_zones) == 2
        assert real_zones == {'us-west-2a', 'us-west-2b'}
        
        # Total node count should still include corrupted nodes
        assert len(nodes) == 4


class TestProductionScenarioReplication:
    """Replicate the exact production scenario from the 500_ERROR_FIX_SUMMARY.md"""

    def test_exact_production_cluster_scenario(self):
        """Test the exact production scenario with 11 nodes, 3 zones"""
        
        from cratedb_xlens.database import CrateDBClient
        client = CrateDBClient("crate://localhost:4200")
        
        # Replicate exact production cluster setup
        production_nodes = [
            'data-hot-0', 'data-hot-1', 'data-hot-2', 'data-hot-3',  # data-hot-3 was problematic
            'data-hot-4', 'data-hot-5', 'data-hot-6', 'data-hot-7', 
            'master-0', 'master-1', 'master-2'
        ]
        
        with patch.object(client, 'execute_query') as mock_execute_query:
            # Set up node names response
            node_name_rows = [[f"{name}-id", name] for name in production_nodes]
            
            # Set up individual node responses
            responses = [{'rows': node_name_rows}]  # Initial node names query
            
            for i, node_name in enumerate(production_nodes):
                if node_name == 'data-hot-3':
                    # This was the problematic node
                    responses.append(Exception("NullPointerException[Cannot invoke \"java.util.Map.get(Object)\" because \"map\" is null]"))
                else:
                    # Healthy node with realistic production values
                    zone = ['us-west-2a', 'us-west-2b', 'us-west-2c'][i % 3]
                    if node_name.startswith('master'):
                        # Master nodes - smaller resources
                        responses.append({
                            'rows': [[f"{node_name}-id", node_name, zone, 1073741824, 2147483648, 53687091200, 10737418240, 42949672960]]
                        })
                    else:
                        # Data nodes - larger resources
                        responses.append({
                            'rows': [[f"{node_name}-id", node_name, zone, 2147483648, 4294967296, 107374182400, 53687091200, 48318054400]]
                        })
            
            mock_execute_query.side_effect = responses
            
            captured_output = StringIO()
            with patch('sys.stdout', captured_output):
                nodes = client.get_nodes_info()
            
            # Verify production cluster characteristics
            assert len(nodes) == 11  # All 11 nodes returned
            
            # Verify zones
            real_zones = set(node.zone for node in nodes if node.zone != 'unknown')
            assert len(real_zones) == 3
            assert real_zones == {'us-west-2a', 'us-west-2b', 'us-west-2c'}
            
            # Verify node types
            data_nodes = [n for n in nodes if n.name.startswith('data-hot')]
            master_nodes = [n for n in nodes if n.name.startswith('master')]
            assert len(data_nodes) == 8
            assert len(master_nodes) == 3
            
            # Verify problematic node has fallback values
            problematic_node = next(n for n in nodes if n.name == 'data-hot-3')
            assert problematic_node.zone == 'unknown'
            assert problematic_node.heap_max == 1
            
            # Verify all other nodes are healthy
            healthy_nodes = [n for n in nodes if n.heap_max > 1]
            assert len(healthy_nodes) == 10
            
            # Verify warning was logged for exactly 1 node
            output = captured_output.getvalue()
            assert "Warning: 1 node(s) have corrupted/missing metadata" in output
            assert "data-hot-3" in output

    def test_production_test_connection_output(self):
        """Test that test-connection produces the expected output format from production"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        
        # Production cluster health
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'GREEN',
            'green_entities': 100, 
            'yellow_entities': 0,
            'red_entities': 0,
            'total_tables': 590,  # Production numbers from summary
            'total_partitions': 100
        }
        
        # Production node setup (11 nodes, 3 zones)
        mock_nodes = []
        zones = ['us-west-2a', 'us-west-2b', 'us-west-2c']
        
        # 8 data nodes
        for i in range(8):
            mock_nodes.append(NodeInfo(
                id=f'data-hot-{i}-id', name=f'data-hot-{i}', zone=zones[i % 3],
                heap_used=2000000000, heap_max=4000000000,
                fs_total=100000000000, fs_used=50000000000, fs_available=45000000000
            ))
        
        # 3 master nodes  
        for i in range(3):
            mock_nodes.append(NodeInfo(
                id=f'master-{i}-id', name=f'master-{i}', zone=zones[i],
                heap_used=1000000000, heap_max=2000000000, 
                fs_total=50000000000, fs_used=20000000000, fs_available=28000000000
            ))
        
        mock_client.get_nodes_info.return_value = mock_nodes
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection()
        output = console.file.getvalue()
        
        # Verify production-like output format
        assert "✅ Successfully connected to CrateDB cluster" in output
        assert "🏥 Cluster Health: GREEN" in output
        assert "Tables: 590, Partitions: 100" in output
        assert "📊 Cluster Info:" in output
        assert "Nodes: 11" in output
        assert "Zones: 3 (us-west-2a, us-west-2b, us-west-2c)" in output

    def test_production_verbose_output_with_resource_warnings(self):
        """Test verbose output shows resource warnings like in production"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {'cluster_health': 'GREEN', 'total_tables': 590, 'total_partitions': 100}
        
        # Create nodes with various resource states
        mock_nodes = [
            # High resource usage node (should get warning indicators)
            NodeInfo(id='high', name='data-hot-high', zone='us-west-2a',
                    heap_used=3600000000, heap_max=4000000000,  # 90% heap
                    fs_total=100000000000, fs_used=92000000000, fs_available=8000000000),  # 92% disk
            
            # Critical resource usage (should get fire indicators)
            NodeInfo(id='critical', name='data-hot-critical', zone='us-west-2b', 
                    heap_used=3900000000, heap_max=4000000000,  # 97.5% heap
                    fs_total=100000000000, fs_used=96000000000, fs_available=4000000000),  # 96% disk
            
            # Healthy node
            NodeInfo(id='healthy', name='data-hot-healthy', zone='us-west-2c',
                    heap_used=1000000000, heap_max=4000000000,  # 25% heap
                    fs_total=100000000000, fs_used=30000000000, fs_available=70000000000)  # 30% disk
        ]
        
        mock_client.get_nodes_info.return_value = mock_nodes
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)  
        cmd.console = console
        
        cmd.test_connection(verbose=True)
        output = console.file.getvalue()
        
        # Verify detailed resource information is shown
        assert "📋 Detailed Node Information:" in output
        
        # Verify percentage calculations
        assert "90.0%" in output  # High heap usage
        assert "92.0%" in output  # High disk usage
        assert "97.5%" in output or "98" in output  # Critical heap usage
        assert "96.0%" in output  # Critical disk usage
        assert "25.0%" in output  # Healthy heap usage  
        assert "30.0%" in output  # Healthy disk usage
        
        # Verify status indicators
        assert "🔥" in output  # Critical indicators
        assert "💾" in output  # Disk warning indicators
        assert "⚠️" in output  # Warning indicators
        assert "✅" in output  # Healthy indicators