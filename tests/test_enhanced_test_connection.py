"""
Tests for enhanced test-connection command with verbose flag support

This module tests the enhanced test-connection command that was implemented
to handle the 500 error scenarios, including:
- Verbose flag support for detailed node information
- Graceful handling of corrupted node metadata
- Resource usage indicators and warnings
- Comprehensive cluster health reporting
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

from cratedb_xlens.database import CrateDBClient, NodeInfo
from cratedb_xlens.commands.diagnostics import DiagnosticsCommands
from rich.console import Console


class TestEnhancedTestConnectionMethods:
    """Test the enhanced test-connection method functionality directly"""

    def test_test_connection_basic_functionality(self):
        """Test basic test-connection method without verbose flag"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'GREEN',
            'total_tables': 50,
            'total_partitions': 10
        }
        mock_client.get_nodes_info.return_value = [
            NodeInfo(id='1', name='node1', zone='us-west-2a', heap_used=1000000000, heap_max=2000000000,
                    fs_total=100000000000, fs_used=50000000000, fs_available=45000000000)
        ]
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection(verbose=False)
        output = console.file.getvalue()
        
        assert "✅ Successfully connected to CrateDB cluster" in output
        assert "🏥 Cluster Health: GREEN" in output
        assert "📊 Cluster Info:" in output
        assert "Nodes: 1" in output
        
        # Should NOT show detailed node information without verbose
        assert "📋 Detailed Node Information:" not in output

    def test_test_connection_with_verbose_flag(self):
        """Test test-connection method with verbose=True shows detailed info"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'GREEN',
            'total_tables': 100,
            'total_partitions': 25
        }
        
        # Mock nodes with different resource states
        mock_client.get_nodes_info.return_value = [
            NodeInfo(id='1', name='healthy-node', zone='us-west-2a', 
                    heap_used=1000000000, heap_max=4000000000,  # 25% heap
                    fs_total=100000000000, fs_used=30000000000, fs_available=70000000000),  # 30% disk
            NodeInfo(id='2', name='warning-node', zone='us-west-2b',
                    heap_used=3200000000, heap_max=4000000000,  # 80% heap  
                    fs_total=100000000000, fs_used=87000000000, fs_available=13000000000)  # 87% disk
        ]
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection(verbose=True)
        output = console.file.getvalue()
        
        assert "✅ Successfully connected to CrateDB cluster" in output
        
        # Should show detailed node information with verbose
        assert "📋 Detailed Node Information:" in output
        assert "healthy-node" in output
        assert "warning-node" in output
        
        # Should show resource percentages
        assert "25.0%" in output  # Healthy heap
        assert "30.0%" in output  # Healthy disk
        assert "80.0%" in output  # Warning heap
        assert "87.0%" in output  # Warning disk
        
        # Should show resource amounts in GB
        assert "GB" in output

    def test_test_connection_with_custom_connection_string(self):
        """Test test-connection with custom connection string"""
        
        mock_client = Mock(spec=CrateDBClient)
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        with patch('cratedb_xlens.database.CrateDBClient') as mock_client_class:
            mock_new_client = Mock(spec=CrateDBClient)
            mock_client_class.return_value = mock_new_client
            
            mock_new_client.test_connection.return_value = True
            mock_new_client.get_cluster_health_summary.return_value = {'cluster_health': 'GREEN', 'total_tables': 1, 'total_partitions': 1}
            mock_new_client.get_nodes_info.return_value = []
            
            custom_connection = "crate://custom-host:4200"
            cmd.test_connection(connection_string=custom_connection, verbose=False)
            
            # Verify custom connection string was used
            mock_client_class.assert_called_with(custom_connection)

    def test_test_connection_verbose_with_corrupted_metadata(self):
        """Test verbose output properly handles nodes with corrupted metadata"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {'cluster_health': 'GREEN', 'total_tables': 1, 'total_partitions': 1}
        
        # Mix of healthy and corrupted nodes
        mock_client.get_nodes_info.return_value = [
            NodeInfo(id='1', name='healthy-node', zone='us-west-2a', 
                    heap_used=1000000000, heap_max=2000000000,
                    fs_total=100000000000, fs_used=50000000000, fs_available=45000000000),
            NodeInfo(id='2', name='corrupted-node', zone='unknown',
                    heap_used=0, heap_max=1,  # Fallback values
                    fs_total=0, fs_used=0, fs_available=0)
        ]
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection(verbose=True)
        output = console.file.getvalue()
        
        # Should show both nodes
        assert "healthy-node" in output
        assert "corrupted-node" in output
        
        # Should show metadata unavailable message for corrupted node
        assert "Metadata unavailable" in output
        assert "⚠️" in output

    def test_test_connection_verbose_status_indicators(self):
        """Test that verbose mode shows appropriate status indicators"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {'cluster_health': 'GREEN', 'total_tables': 1, 'total_partitions': 1}
        
        # Nodes with different resource usage levels
        mock_client.get_nodes_info.return_value = [
            # Critical node (>90% heap and disk)
            NodeInfo(id='critical', name='critical-node', zone='us-west-2a',
                    heap_used=3800000000, heap_max=4000000000,  # 95% heap
                    fs_total=100000000000, fs_used=95000000000, fs_available=5000000000),  # 95% disk
            
            # Warning node (>75% heap, >85% disk)
            NodeInfo(id='warning', name='warning-node', zone='us-west-2b',
                    heap_used=3200000000, heap_max=4000000000,  # 80% heap
                    fs_total=100000000000, fs_used=87000000000, fs_available=13000000000),  # 87% disk
            
            # Healthy node
            NodeInfo(id='healthy', name='healthy-node', zone='us-west-2c',
                    heap_used=1000000000, heap_max=4000000000,  # 25% heap
                    fs_total=100000000000, fs_used=30000000000, fs_available=70000000000)  # 30% disk
        ]
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection(verbose=True)
        output = console.file.getvalue()
        
        # Should show status indicators
        assert "🔥" in output  # Critical indicator
        assert "💾" in output  # Critical disk indicator
        assert "⚠️" in output  # Warning indicator
        assert "📁" in output  # Disk warning indicator
        assert "✅" in output  # Healthy indicator

    def test_test_connection_connection_failure(self):
        """Test test-connection handles connection failures gracefully"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = False
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection(verbose=False)
        output = console.file.getvalue()
        
        assert "❌ Failed to connect to CrateDB cluster" in output
        assert "💡 Check your connection configuration" in output

    def test_test_connection_health_query_failure(self):
        """Test handling when health query fails but connection succeeds"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.side_effect = Exception("Health query failed")
        mock_client.get_nodes_info.return_value = []
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection(verbose=False)
        output = console.file.getvalue()
        
        assert "✅ Successfully connected to CrateDB cluster" in output
        assert "⚠️  Cluster health unavailable" in output

    def test_test_connection_nodes_query_failure(self):
        """Test handling when nodes query fails but connection succeeds"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {'cluster_health': 'GREEN', 'total_tables': 1, 'total_partitions': 1}
        mock_client.get_nodes_info.side_effect = Exception("Nodes query failed")
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection(verbose=False)
        output = console.file.getvalue()
        
        assert "✅ Successfully connected to CrateDB cluster" in output
        assert "⚠️  Basic cluster info unavailable" in output

    def test_test_connection_verbose_severity_sorting_and_legend(self):
        """Test that verbose mode sorts nodes by severity and displays legend"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'GREEN', 
            'green_entities': 100,
            'yellow_entities': 0,
            'red_entities': 0,
            'total_tables': 1, 
            'total_partitions': 1
        }
        
        # Create nodes with different severity levels for sorting test
        mock_client.get_nodes_info.return_value = [
            # Healthy node (should appear last)
            NodeInfo(id='healthy', name='aaaa-healthy', zone='us-west-2a',
                    heap_used=1000000000, heap_max=4000000000,  # 25% heap
                    fs_total=100000000000, fs_used=30000000000, fs_available=70000000000),  # 30% disk
            
            # Critical node (should appear first due to severity)
            NodeInfo(id='critical', name='zzzz-critical', zone='us-west-2b',
                    heap_used=3800000000, heap_max=4000000000,  # 95% heap
                    fs_total=100000000000, fs_used=95000000000, fs_available=5000000000),  # 95% disk
            
            # Warning node (should appear in middle)
            NodeInfo(id='warning', name='mmmm-warning', zone='us-west-2c',
                    heap_used=3200000000, heap_max=4000000000,  # 80% heap
                    fs_total=100000000000, fs_used=50000000000, fs_available=50000000000),  # 50% disk
            
            # Corrupted node (should appear first with highest priority)
            NodeInfo(id='corrupted', name='bbbb-corrupted', zone='unknown',
                    heap_used=0, heap_max=1, fs_total=0, fs_used=0, fs_available=0)
        ]
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection(verbose=True)
        output = console.file.getvalue()
        
        # Verify legend is displayed (account for potential line breaks)
        assert "Legend:" in output
        assert "🔥 Critical (>90% heap)" in output
        assert "⚠️ Warning (>75% heap)" in output
        assert "💾 Disk Critical (>90%)" in output
        assert "📁 Disk Warning (>85%)" in output
        assert "✅" in output and "Healthy" in output
        
        # Verify severity summary is displayed
        assert "Summary:" in output
        assert "1 Critical" in output
        assert "1 Warning" in output
        assert "1 Healthy" in output
        assert "1 Corrupted" in output
        
        # Verify nodes are sorted by severity first (corrupted highest, then critical, warning, healthy)
        # Find positions of node names in output
        corrupted_pos = output.find("bbbb-corrupted")
        critical_pos = output.find("zzzz-critical") 
        warning_pos = output.find("mmmm-warning")
        healthy_pos = output.find("aaaa-healthy")
        
        # Ensure all nodes were found
        assert corrupted_pos != -1
        assert critical_pos != -1
        assert warning_pos != -1
        assert healthy_pos != -1
        
        # Verify sorting: corrupted first, then critical, then warning, then healthy
        assert corrupted_pos < critical_pos
        assert critical_pos < warning_pos
        assert warning_pos < healthy_pos
        
        # Verify status indicators
        assert "🔥" in output  # Critical indicator
        assert "💾" in output  # Critical disk indicator
        assert "⚠️" in output  # Warning/corrupted indicators
        assert "✅" in output  # Healthy indicator

    def test_test_connection_verbose_color_coding_by_severity(self):
        """Test that verbose mode uses appropriate colors based on severity"""
        
        mock_client = Mock(spec=CrateDBClient)
        mock_client.test_connection.return_value = True
        mock_client.get_cluster_health_summary.return_value = {
            'cluster_health': 'GREEN',
            'green_entities': 100, 
            'yellow_entities': 0,
            'red_entities': 0,
            'total_tables': 1,
            'total_partitions': 1
        }
        
        # Create nodes with different severities to test color coding
        mock_client.get_nodes_info.return_value = [
            # Critical severity node (should be red)
            NodeInfo(id='critical', name='critical-node', zone='us-west-2a',
                    heap_used=3800000000, heap_max=4000000000,  # 95% heap
                    fs_total=100000000000, fs_used=50000000000, fs_available=50000000000),
            
            # Warning severity node (should be yellow)
            NodeInfo(id='warning', name='warning-node', zone='us-west-2b',
                    heap_used=3200000000, heap_max=4000000000,  # 80% heap
                    fs_total=100000000000, fs_used=50000000000, fs_available=50000000000),
            
            # Healthy node (should be green)
            NodeInfo(id='healthy', name='healthy-node', zone='us-west-2c',
                    heap_used=1000000000, heap_max=4000000000,  # 25% heap
                    fs_total=100000000000, fs_used=30000000000, fs_available=70000000000)
        ]
        
        console = Console(file=StringIO(), width=120, force_terminal=False)
        cmd = DiagnosticsCommands(mock_client)
        cmd.console = console
        
        cmd.test_connection(verbose=True)
        output = console.file.getvalue()
        
        # Verify appropriate color coding is applied
        # Note: We can't easily test the actual colors in console output, but we can verify the nodes appear
        assert "critical-node" in output
        assert "warning-node" in output
        assert "healthy-node" in output