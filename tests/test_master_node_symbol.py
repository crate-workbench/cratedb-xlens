"""
Tests for master node crown symbol functionality in verbose test-connection output

This module tests the master node crown symbol (👑) feature that was added to the
--verbose output of the test-connection command, including:
- Crown symbol appears next to the master node
- Legend includes master node symbol when available
- Graceful handling when master node information is unavailable
- Integration with existing severity sorting and health indicators
"""

import pytest
from unittest.mock import Mock, patch
from io import StringIO

from cratedb_xlens.database import CrateDBClient, NodeInfo
from cratedb_xlens.commands.diagnostics import DiagnosticsCommands
from rich.console import Console


class TestMasterNodeSymbolFunctionality:
    """Test the master node crown symbol functionality"""

    def test_master_node_symbol_display_in_verbose_output(self):
        """Test that the master node gets a crown symbol in verbose output"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        
        # Mock cluster health
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'GREEN',
            'green_entities': 100,
            'yellow_entities': 0,
            'red_entities': 0,
            'total_tables': 10,
            'total_partitions': 50
        }
        
        # Mock nodes - simulate the example from user's request
        mock_nodes = [
            NodeInfo(id='N3aztGjmRnWPQpD5mKr2NA', name='data-hot-3', zone='us-west-2a',
                    heap_used=1000000000, heap_max=2000000000,
                    fs_total=100000000000, fs_used=50000000000, fs_available=50000000000),
            NodeInfo(id='zhMDxEagTgapM34lDaXk1g', name='master-1', zone='us-west-2c',
                    heap_used=500000000, heap_max=2000000000,
                    fs_total=100000000000, fs_used=30000000000, fs_available=70000000000),
            NodeInfo(id='jhIGADfoQau4O7HUWwz47A', name='master-0', zone='us-west-2d',
                    heap_used=600000000, heap_max=2000000000,
                    fs_total=100000000000, fs_used=35000000000, fs_available=65000000000),
        ]
        
        mock_client.get_nodes_info.return_value = mock_nodes
        
        # Mock master node ID - zhMDxEagTgapM34lDaXk1g is master-1 as per user's example
        mock_client.get_master_node_id.return_value = 'zhMDxEagTgapM34lDaXk1g'
        
        # Create diagnostics command instance
        cmd = DiagnosticsCommands(mock_client)
        
        # Capture console output
        console_output = StringIO()
        cmd.console = Console(file=console_output, width=120, legacy_windows=False)
        
        # Call test_connection with verbose=True
        cmd.test_connection(None, verbose=True)
        
        output = console_output.getvalue()
        
        # Verify legend includes master node symbol
        assert "👑 Master node" in output
        
        # Verify master-1 node has crown symbol
        assert "master-1" in output and "👑" in output
        
        # Verify other nodes don't have crown symbol inappropriately
        lines = output.split('\n')
        for line in lines:
            if "data-hot-3" in line or "master-0" in line:
                assert "👑" not in line

    def test_master_node_symbol_unavailable(self):
        """Test behavior when master node information is unavailable"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        
        # Mock cluster health
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'GREEN',
            'green_entities': 100,
            'yellow_entities': 0,
            'red_entities': 0,
            'total_tables': 10,
            'total_partitions': 50
        }
        
        # Mock nodes
        mock_nodes = [
            NodeInfo(id='node1', name='test-node-1', zone='us-west-2a',
                    heap_used=1000000000, heap_max=2000000000,
                    fs_total=100000000000, fs_used=50000000000, fs_available=50000000000),
        ]
        
        mock_client.get_nodes_info.return_value = mock_nodes
        
        # Mock master node ID as None (unavailable)
        mock_client.get_master_node_id.return_value = None
        
        # Create diagnostics command instance
        cmd = DiagnosticsCommands(mock_client)
        
        # Capture console output
        console_output = StringIO()
        cmd.console = Console(file=console_output, width=120, legacy_windows=False)
        
        # Call test_connection with verbose=True
        cmd.test_connection(None, verbose=True)
        
        output = console_output.getvalue()
        
        # Verify legend doesn't include master node symbol when unavailable
        assert "👑 Master node" not in output
        
        # Verify no nodes have crown symbol
        assert "👑" not in output

    def test_master_node_symbol_with_mixed_severity_nodes(self):
        """Test master node symbol with nodes of different severity levels"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        
        # Mock cluster health with some issues
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'YELLOW',
            'green_entities': 80,
            'yellow_entities': 15,
            'red_entities': 5,
            'total_tables': 25,
            'total_partitions': 100
        }
        
        # Mock nodes with various severity levels
        mock_nodes = [
            # Critical node (heap > 90%)
            NodeInfo(id='critical1', name='data-critical-1', zone='us-west-2a',
                    heap_used=3700000000, heap_max=4000000000,  # 92.5% heap
                    fs_total=1000000000000, fs_used=950000000000, fs_available=50000000000),  # 95% disk
            
            # Warning node (heap > 75%)
            NodeInfo(id='warning1', name='data-warning-1', zone='us-west-2b',
                    heap_used=3200000000, heap_max=4000000000,  # 80% heap
                    fs_total=1000000000000, fs_used=800000000000, fs_available=200000000000),  # 80% disk
            
            # Healthy master node
            NodeInfo(id='master123', name='master-healthy', zone='us-west-2c',
                    heap_used=800000000, heap_max=2000000000,  # 40% heap
                    fs_total=500000000000, fs_used=200000000000, fs_available=300000000000),  # 40% disk
            
            # Healthy data node
            NodeInfo(id='healthy1', name='data-healthy-1', zone='us-west-2d',
                    heap_used=1000000000, heap_max=4000000000,  # 25% heap
                    fs_total=1000000000000, fs_used=500000000000, fs_available=500000000000),  # 50% disk
        ]
        
        mock_client.get_nodes_info.return_value = mock_nodes
        
        # Mock master node ID
        mock_client.get_master_node_id.return_value = 'master123'
        
        # Create diagnostics command instance
        cmd = DiagnosticsCommands(mock_client)
        
        # Capture console output
        console_output = StringIO()
        cmd.console = Console(file=console_output, width=120, legacy_windows=False)
        
        # Call test_connection with verbose=True
        cmd.test_connection(None, verbose=True)
        
        output = console_output.getvalue()
        
        # Verify legend includes master node symbol
        assert "👑 Master node" in output
        
        # Verify master-healthy node has crown symbol regardless of severity sorting
        lines = output.split('\n')
        master_line_found = False
        for line in lines:
            if "master-healthy" in line:
                assert "👑" in line
                master_line_found = True
            elif any(name in line for name in ['data-critical-1', 'data-warning-1', 'data-healthy-1']):
                assert "👑" not in line
        
        assert master_line_found, "master-healthy node not found in output"

    def test_production_cluster_scenario(self):
        """Test with the exact production cluster scenario from user's example"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        
        # Mock cluster health
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'GREEN',
            'green_entities': 100,
            'yellow_entities': 0,
            'red_entities': 0,
            'total_tables': 50,
            'total_partitions': 200
        }
        
        # Mock nodes - exact replica of user's cluster output
        mock_nodes = [
            NodeInfo(id='N3aztGjmRnWPQpD5mKr2NA', name='data-hot-3', zone='us-west-2a',
                    heap_used=1200000000, heap_max=4000000000,
                    fs_total=1000000000000, fs_used=650000000000, fs_available=350000000000),
            NodeInfo(id='nz_qzqTwTTmoBuE-NWXEwQ', name='data-hot-6', zone='us-west-2b',
                    heap_used=1800000000, heap_max=4000000000,
                    fs_total=1000000000000, fs_used=720000000000, fs_available=280000000000),
            NodeInfo(id='YFkx_psZQSelm9edw7UaVg', name='data-hot-2', zone='us-west-2c',
                    heap_used=1100000000, heap_max=4000000000,
                    fs_total=1000000000000, fs_used=600000000000, fs_available=400000000000),
            NodeInfo(id='ZH6fBanGSjanGqeSh-sw0A', name='data-hot-1', zone='us-west-2a',
                    heap_used=1300000000, heap_max=4000000000,
                    fs_total=1000000000000, fs_used=680000000000, fs_available=320000000000),
            NodeInfo(id='zhMDxEagTgapM34lDaXk1g', name='master-1', zone='us-west-2d',
                    heap_used=800000000, heap_max=2000000000,
                    fs_total=500000000000, fs_used=200000000000, fs_available=300000000000),
            NodeInfo(id='jcatFdmLQ4SBMbM8kfs0iQ', name='data-hot-5', zone='us-west-2e',
                    heap_used=1400000000, heap_max=4000000000,
                    fs_total=1000000000000, fs_used=700000000000, fs_available=300000000000),
            NodeInfo(id='jhIGADfoQau4O7HUWwz47A', name='master-0', zone='us-west-2f',
                    heap_used=600000000, heap_max=2000000000,
                    fs_total=500000000000, fs_used=150000000000, fs_available=350000000000),
            NodeInfo(id='uzkAeMyHTiS8x-vfPVKQxw', name='master-2', zone='us-west-2g',
                    heap_used=700000000, heap_max=2000000000,
                    fs_total=500000000000, fs_used=180000000000, fs_available=320000000000),
            NodeInfo(id='cL2YkspiTfakITNbVNb4Dg', name='data-hot-4', zone='us-west-2h',
                    heap_used=1600000000, heap_max=4000000000,
                    fs_total=1000000000000, fs_used=750000000000, fs_available=250000000000),
            NodeInfo(id='9B2QwiRdT22vdfWF8Pa7mw', name='data-hot-0', zone='us-west-2i',
                    heap_used=1500000000, heap_max=4000000000,
                    fs_total=1000000000000, fs_used=800000000000, fs_available=200000000000),
            NodeInfo(id='gpUhkbAYRNe45fyVKk-TFA', name='data-hot-7', zone='us-west-2j',
                    heap_used=1700000000, heap_max=4000000000,
                    fs_total=1000000000000, fs_used=850000000000, fs_available=150000000000),
        ]
        
        mock_client.get_nodes_info.return_value = mock_nodes
        
        # Mock master node ID - zhMDxEagTgapM34lDaXk1g is master-1 as per user's sys.cluster query
        mock_client.get_master_node_id.return_value = 'zhMDxEagTgapM34lDaXk1g'
        
        # Create diagnostics command instance
        cmd = DiagnosticsCommands(mock_client)
        
        # Capture console output
        console_output = StringIO()
        cmd.console = Console(file=console_output, width=120, legacy_windows=False)
        
        # Call test_connection with verbose=True
        cmd.test_connection(None, verbose=True)
        
        output = console_output.getvalue()
        
        # Verify legend includes master node symbol
        assert "👑 Master node" in output
        
        # Verify master-1 node has crown symbol
        lines = output.split('\n')
        master_1_found = False
        for line in lines:
            if "master-1" in line and "👑" in line:
                master_1_found = True
            elif any(name in line for name in ['data-hot-', 'master-0', 'master-2']) and "👑" in line:
                pytest.fail(f"Non-master node incorrectly has crown symbol: {line}")
        
        assert master_1_found, "master-1 node with crown symbol not found"

    def test_master_node_method_error_handling(self):
        """Test that get_master_node_id method handles errors gracefully"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        
        # Mock cluster health
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'GREEN',
            'total_tables': 10,
            'total_partitions': 50
        }
        
        # Mock nodes
        mock_nodes = [
            NodeInfo(id='node1', name='test-node-1', zone='us-west-2a',
                    heap_used=1000000000, heap_max=2000000000,
                    fs_total=100000000000, fs_used=50000000000, fs_available=50000000000),
        ]
        
        mock_client.get_nodes_info.return_value = mock_nodes
        
        # Mock get_master_node_id to raise an exception
        mock_client.get_master_node_id.side_effect = Exception("Database error")
        
        # Create diagnostics command instance
        cmd = DiagnosticsCommands(mock_client)
        
        # Capture console output
        console_output = StringIO()
        cmd.console = Console(file=console_output, width=120, legacy_windows=False)
        
        # Call test_connection with verbose=True - should not raise exception
        cmd.test_connection(None, verbose=True)
        
        output = console_output.getvalue()
        
        # Should complete without errors and not show master node symbol
        assert "👑" not in output
        assert "Successfully connected to CrateDB cluster" in output


class TestGetMasterNodeIdMethod:
    """Test the get_master_node_id method directly"""

    def test_get_master_node_id_success(self):
        """Test successful retrieval of master node ID"""
        
        with patch('cratedb_xlens.database.os.getenv') as mock_getenv:
            mock_getenv.return_value = 'postgresql://test:test@localhost:4200'
            
            client = CrateDBClient()
            
            # Mock the execute_query method
            with patch.object(client, 'execute_query') as mock_execute:
                mock_execute.return_value = {
                    'rows': [['zhMDxEagTgapM34lDaXk1g']]
                }
                
                master_id = client.get_master_node_id()
                assert master_id == 'zhMDxEagTgapM34lDaXk1g'
                
                # Verify correct query was executed
                expected_query = "\n        SELECT master_node FROM sys.cluster\n        "
                mock_execute.assert_called_once_with(expected_query)

    def test_get_master_node_id_no_results(self):
        """Test handling when no master node results are returned"""
        
        with patch('cratedb_xlens.database.os.getenv') as mock_getenv:
            mock_getenv.return_value = 'postgresql://test:test@localhost:4200'
            
            client = CrateDBClient()
            
            # Mock the execute_query method to return empty results
            with patch.object(client, 'execute_query') as mock_execute:
                mock_execute.return_value = {'rows': []}
                
                master_id = client.get_master_node_id()
                assert master_id is None

    def test_get_master_node_id_null_result(self):
        """Test handling when master node result is null"""
        
        with patch('cratedb_xlens.database.os.getenv') as mock_getenv:
            mock_getenv.return_value = 'postgresql://test:test@localhost:4200'
            
            client = CrateDBClient()
            
            # Mock the execute_query method to return null result
            with patch.object(client, 'execute_query') as mock_execute:
                mock_execute.return_value = {'rows': [[None]]}
                
                master_id = client.get_master_node_id()
                assert master_id is None

    def test_get_master_node_id_exception_handling(self):
        """Test handling when query execution raises an exception"""
        
        with patch('cratedb_xlens.database.os.getenv') as mock_getenv:
            mock_getenv.return_value = 'postgresql://test:test@localhost:4200'
            
            client = CrateDBClient()
            
            # Mock the execute_query method to raise an exception
            with patch.object(client, 'execute_query') as mock_execute:
                mock_execute.side_effect = Exception("Database connection error")
                
                master_id = client.get_master_node_id()
                assert master_id is None