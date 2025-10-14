"""
Pytest tests for critical partition bug fixes in XMover

This test suite verifies that the critical partition bugs have been fixed:
1. Zone conflict detection is partition-aware
2. ShardInfo data model supports partition identification
3. Database queries include partition filtering
4. Partition isolation is working correctly

These tests ensure XMover can safely handle partitioned CrateDB tables.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

from xmover.database import CrateDBClient, ShardInfo
from xmover.analyzer import ShardAnalyzer, MoveRecommendation


class TestPartitionBugFixes:
    """Test suite for partition bug fixes"""

    @pytest.mark.partition
    def test_shard_info_partition_support(self):
        """Test that ShardInfo supports partition_ident field and utility methods"""
        
        # Test creating ShardInfo with partition
        shard_with_partition = ShardInfo(
            table_name="events",
            schema_name="doc",
            shard_id=0,
            node_id="node1",
            node_name="Node A",
            zone="zone1",
            is_primary=True,
            size_bytes=1024*1024*1024,
            size_gb=1.0,
            num_docs=1000,
            state="STARTED",
            routing_state="STARTED",
            partition_ident="04732d202401"
        )
        
        # Test creating ShardInfo without partition
        shard_without_partition = ShardInfo(
            table_name="users",
            schema_name="doc",
            shard_id=1,
            node_id="node2",
            node_name="Node B",
            zone="zone2",
            is_primary=False,
            size_bytes=512*1024*1024,
            size_gb=0.5,
            num_docs=500,
            state="STARTED",
            routing_state="STARTED"
            # partition_ident defaults to None
        )
        
        # Verify the properties work correctly
        assert shard_with_partition.full_table_identifier == "events[04732d202401]"
        assert shard_without_partition.full_table_identifier == "users"
        assert "04732d202401" in shard_with_partition.unique_shard_key
        assert shard_with_partition.unique_shard_key == "events[04732d202401]:shard_0:P"
        assert shard_without_partition.unique_shard_key == "users:shard_1:R"

    @pytest.mark.partition
    def test_move_recommendation_partition_support(self):
        """Test that MoveRecommendation supports partition_ident field"""
        
        # Test creating MoveRecommendation with partition
        move_with_partition = MoveRecommendation(
            table_name="events",
            schema_name="doc",
            shard_id=0,
            from_node="Node A",
            to_node="Node C",
            from_zone="zone1",
            to_zone="zone3",
            shard_type="PRIMARY",
            size_gb=5.0,
            reason="Balance zones",
            partition_ident="04732d202401"
        )
        
        # Test creating MoveRecommendation without partition
        move_without_partition = MoveRecommendation(
            table_name="users",
            schema_name="doc",
            shard_id=1,
            from_node="Node B",
            to_node="Node D",
            from_zone="zone2",
            to_zone="zone4",
            shard_type="REPLICA",
            size_gb=2.0,
            reason="Rebalance load"
            # partition_ident defaults to None
        )
        
        # Verify the properties work correctly
        assert move_with_partition.full_table_identifier == "events[04732d202401]"
        assert move_without_partition.full_table_identifier == "users"

    @pytest.mark.partition
    def test_move_recommendation_to_sql_partition_support(self):
        """Test that MoveRecommendation.to_sql() generates correct SQL for partitioned tables"""
        
        # Test partitioned table - should include PARTITION clause
        partitioned_rec = MoveRecommendation(
            table_name="shipmentFormFieldData",
            schema_name="TURVO",
            shard_id=4,
            from_node="data-hot-6",
            to_node="data-hot-5",
            from_zone="zone1",
            to_zone="zone2",
            shard_type="PRIMARY",
            size_gb=45.5,
            reason="Balancing",
            partition_ident="04732d1234abcd",
            partition_values='("id_ts_month"=1754006400000)'
        )
        
        sql = partitioned_rec.to_sql()
        expected = 'ALTER TABLE "TURVO"."shipmentFormFieldData" PARTITION ("id_ts_month"=1754006400000) REROUTE MOVE SHARD 4 FROM \'data-hot-6\' TO \'data-hot-5\';'
        assert sql == expected, f"Expected: {expected}, Got: {sql}"
        
        # Test non-partitioned table - should NOT include PARTITION clause
        non_partitioned_rec = MoveRecommendation(
            table_name="users",
            schema_name="doc",
            shard_id=1,
            from_node="node-a",
            to_node="node-b",
            from_zone="zone1",
            to_zone="zone2",
            shard_type="REPLICA",
            size_gb=10.0,
            reason="Load balance",
            partition_ident=None,
            partition_values=None
        )
        
        sql = non_partitioned_rec.to_sql()
        expected = 'ALTER TABLE "doc"."users" REROUTE MOVE SHARD 1 FROM \'node-a\' TO \'node-b\';'
        assert sql == expected, f"Expected: {expected}, Got: {sql}"
        
        # Test empty partition_values - should behave like non-partitioned
        empty_partition_rec = MoveRecommendation(
            table_name="testTable",
            schema_name="doc",
            shard_id=2,
            from_node="node-x",
            to_node="node-y",
            from_zone="zone1",
            to_zone="zone2",
            shard_type="PRIMARY",
            size_gb=5.0,
            reason="Test",
            partition_ident="some_ident",
            partition_values=""  # Empty string
        )
        
        sql = empty_partition_rec.to_sql()
        expected = 'ALTER TABLE "doc"."testTable" REROUTE MOVE SHARD 2 FROM \'node-x\' TO \'node-y\';'
        assert sql == expected, f"Expected: {expected}, Got: {sql}"

    @pytest.mark.partition
    def test_get_shards_info_includes_partition_ident(self):
        """Test that get_shards_info query includes partition_ident"""
        
        client = CrateDBClient("http://localhost:4200")
        
        # Mock query result with partition data
        mock_result = {
            'rows': [
                # events table with partitions - need 14 columns including routing_state
                ["events", "doc", 0, "04732d202401", "=('2024-01')", "node1", "Node A", "zone1", True, 1024*1024*1024, 1.0, 1000, "STARTED", "STARTED"],
                ["events", "doc", 0, "04732d202401", "=('2024-01')", "node2", "Node B", "zone2", False, 1024*1024*1024, 1.0, 1000, "STARTED", "STARTED"],
                ["events", "doc", 0, "04732d202402", "=('2024-02')", "node1", "Node A", "zone1", True, 512*1024*1024, 0.5, 500, "STARTED", "STARTED"],
                # users table without partitions
                ["users", "doc", 1, None, None, "node3", "Node C", "zone3", True, 256*1024*1024, 0.25, 250, "STARTED", "STARTED"]
            ]
        }
        
        with patch.object(client, 'execute_query', return_value=mock_result) as mock_execute:
            # Test get_shards_info method
            shards = client.get_shards_info()
            
            # Verify the query was called
            assert mock_execute.called
            call_args = mock_execute.call_args[0]
            query = call_args[0]
            
            # Verify partition_ident is included in the query
            assert "s.partition_ident" in query, "Query should include partition_ident field"
            assert "s.partition_ident, s.id" in query, "Query should order by partition_ident"
            
            # Verify we get ShardInfo objects with partition_ident
            assert len(shards) == 4
            assert shards[0].partition_ident == "04732d202401"
            assert shards[2].partition_ident == "04732d202402"
            assert shards[3].partition_ident is None

    @pytest.mark.partition
    @pytest.mark.safety
    def test_zone_conflict_detection_partition_aware(self):
        """Test that zone conflict detection is partition-aware"""
        
        # Create mock shards representing partitioned table
        mock_shards = [
            # events[2024-02] partition - shard 0 (this is the one we want to test moving)
            ShardInfo("events", "doc", 0, "node1", "Node A", "zone1", True, 512*1024*1024, 0.5, 500, "STARTED", "STARTED", "04732d202402"),
            
            # events[2024-01] partition - shard 0 (different partition, should not interfere)
            ShardInfo("events", "doc", 0, "node2", "Node B", "zone2", True, 1024*1024*1024, 1.0, 1000, "STARTED", "STARTED", "04732d202401"),
            ShardInfo("events", "doc", 0, "node3", "Node C", "zone1", False, 1024*1024*1024, 1.0, 1000, "STARTED", "STARTED", "04732d202401"),
        ]
        
        # Create actual analyzer with mocked client
        mock_client = Mock(spec=CrateDBClient)
        analyzer = ShardAnalyzer(mock_client)
        
        # Manually set the shards and nodes (since we're testing the zone conflict logic)
        analyzer.shards = mock_shards
        analyzer.nodes = self._create_mock_nodes()
        
        # Test case 1: Move within same partition should be detected correctly
        # Try to move events[2024-02] shard 0 from Node A to Node C (both in zone1)
        recommendation = MoveRecommendation(
            table_name="events",
            schema_name="doc",
            shard_id=0,
            from_node="Node A",
            to_node="Node C",
            from_zone="zone1",
            to_zone="zone1",
            shard_type="PRIMARY",
            size_gb=0.5,
            reason="Test partition awareness",
            partition_ident="04732d202402"
        )
        
        # Mock the database query response for partition-aware conflict check
        # This should return only the events[2024-02] shard 0, not events[2024-01] shard 0
        mock_partition_query_result = {
            'rows': [
                ["node1", "Node A", "zone1", True, "STARTED", "STARTED", "04732d202402"]
            ]
        }
        
        # Mock the zone allocation query (second query call)
        mock_zone_allocation_result = {
            'rows': [
                ["zone1", 1]  # events[2024-02] exists only in zone1
            ]
        }
        
        # Set up side_effect to return different results for different queries
        mock_client.execute_query.side_effect = [
            mock_partition_query_result,  # First call - partition conflict check
            mock_zone_allocation_result   # Second call - zone allocation check
        ]
        
        # Check for zone conflict
        conflict = analyzer._check_zone_conflict(recommendation)
        
        # Verify the query was called with partition parameters
        assert mock_client.execute_query.called
        call_args = mock_client.execute_query.call_args
        query = call_args[0][0]
        params = call_args[0][1]
        
        # Verify partition-aware query
        assert "s.partition_ident" in query, "Query should include partition_ident"
        assert "AND (s.partition_ident = ? OR (s.partition_ident IS NULL AND ? IS NULL))" in query, "Query should filter by partition"
        
        # Verify partition parameter is included (appears twice due to NULL handling)
        partition_count = params.count("04732d202402")
        assert partition_count >= 2, f"Query parameters should include partition_ident twice (for NULL handling), got: {params}"
        
        # Since target zone (zone1) already has this partition, should detect conflict
        assert conflict is not None, "Zone conflict should be detected for same partition"
        assert "Zone conflict" in conflict or "already has a copy" in conflict

    @pytest.mark.partition
    @pytest.mark.safety
    def test_partition_isolation(self):
        """Test that different partitions are treated as separate entities"""
        
        # Create mock shards for testing isolation
        mock_shards = [
            # events[2024-01] - has copies in zone1 and zone2
            ShardInfo("events", "doc", 0, "node1", "Node A", "zone1", True, 1024*1024*1024, 1.0, 1000, "STARTED", "STARTED", "04732d202401"),
            ShardInfo("events", "doc", 0, "node2", "Node B", "zone2", False, 1024*1024*1024, 1.0, 1000, "STARTED", "STARTED", "04732d202401"),
            
            # events[2024-02] - only has copy in zone1 (different distribution than 2024-01)
            ShardInfo("events", "doc", 0, "node1", "Node A", "zone1", True, 512*1024*1024, 0.5, 500, "STARTED", "STARTED", "04732d202402"),
        ]
        
        mock_client = Mock(spec=CrateDBClient)
        analyzer = ShardAnalyzer(mock_client)
        
        # Manually set the shards and nodes
        analyzer.shards = mock_shards
        analyzer.nodes = self._create_mock_nodes()
        
        # Test: Move events[2024-02] shard 0 from Node A (zone1) to Node C (zone2)
        # This SHOULD be allowed because events[2024-02] doesn't exist in zone2
        # even though events[2024-01] does exist in zone2
        
        recommendation = MoveRecommendation(
            table_name="events",
            schema_name="doc",
            shard_id=0,
            from_node="Node A",
            to_node="Node C",
            from_zone="zone1",
            to_zone="zone2",
            shard_type="PRIMARY",
            size_gb=0.5,
            reason="Test partition isolation",
            partition_ident="04732d202402"
        )
        
        # Mock query should return only events[2024-02] data (partition isolation)
        mock_partition_query_result = {
            'rows': [
                ["node1", "Node A", "zone1", True, "STARTED", "STARTED", "04732d202402"]
            ]
        }
        
        # Mock the zone allocation query (second query call)
        mock_zone_allocation_result = {
            'rows': [
                ["zone1", 1]  # events[2024-02] exists only in zone1
            ]
        }
        
        # Set up side_effect to return different results for different queries
        mock_client.execute_query.side_effect = [
            mock_partition_query_result,  # First call - partition conflict check
            mock_zone_allocation_result   # Second call - zone allocation check
        ]
        
        conflict = analyzer._check_zone_conflict(recommendation)
        
        # Should NOT have a conflict because events[2024-02] doesn't exist in zone2
        # (even though events[2024-01] does exist in zone2)
        assert conflict is None, f"Partition isolation failed - unexpected conflict: {conflict}"

    @pytest.mark.partition
    @pytest.mark.safety
    def test_broken_vs_fixed_zone_conflict_scenario(self):
        """Test demonstrating the before/after behavior of zone conflict detection"""
        
        # This test demonstrates what the broken logic would have done vs. fixed logic
        
        # Scenario: We have events table with two partitions:
        # - events[2024-01] has shards in zone1 and zone2  
        # - events[2024-02] has shard only in zone1
        # 
        # We want to move events[2024-02] shard 0 from zone1 to zone1 (Node C)
        # 
        # BROKEN logic would see:
        # - Query returns ALL shard 0s (both partitions)
        # - Sees zones: {zone1, zone2}
        # - Thinks: "Multiple zones, move is safe"
        # - APPROVES dangerous move
        #
        # FIXED logic should see:
        # - Query returns only events[2024-02] shard 0
        # - Sees zones: {zone1}
        # - Thinks: "Target zone already has this partition"
        # - REJECTS unsafe move
        
        mock_shards = [
            ShardInfo("events", "doc", 0, "node1", "Node A", "zone1", True, 512*1024*1024, 0.5, 500, "STARTED", "STARTED", "04732d202402"),
        ]
        
        mock_client = Mock(spec=CrateDBClient)
        analyzer = ShardAnalyzer(mock_client)
        analyzer.shards = mock_shards
        analyzer.nodes = self._create_mock_nodes()
        
        recommendation = MoveRecommendation(
            table_name="events",
            schema_name="doc",
            shard_id=0,
            from_node="Node A", 
            to_node="Node C",
            from_zone="zone1",
            to_zone="zone1",  # Same zone - should be rejected
            shard_type="PRIMARY",
            size_gb=0.5,
            reason="Demonstrate bug fix",
            partition_ident="04732d202402"
        )
        
        # Mock what the FIXED query should return (only specific partition)
        mock_fixed_result = {
            'rows': [
                ["node1", "Node A", "zone1", True, "STARTED", "STARTED", "04732d202402"]
            ]
        }
        
        mock_zone_result = {
            'rows': [["zone1", 1]]
        }
        
        mock_client.execute_query.side_effect = [mock_fixed_result, mock_zone_result]
        
        # Test the fixed behavior
        conflict = analyzer._check_zone_conflict(recommendation)
        
        # The fixed logic should correctly detect the conflict
        assert conflict is not None, "Fixed logic should detect zone conflict"
        assert "Zone conflict" in conflict or "already has a copy" in conflict
        
        # Verify the query includes partition filtering
        call_args = mock_client.execute_query.call_args_list[0]
        query = call_args[0][0] 
        params = call_args[0][1]
        
        assert "s.partition_ident" in query
        assert "04732d202402" in params

    @pytest.mark.partition
    def test_non_partitioned_table_compatibility(self):
        """Test that fixes work correctly with non-partitioned tables"""
        
        # Create mock shards for non-partitioned table
        mock_shards = [
            ShardInfo("users", "doc", 0, "node1", "Node A", "zone1", True, 512*1024*1024, 0.5, 500, "STARTED", "STARTED", None),
            ShardInfo("users", "doc", 0, "node2", "Node B", "zone2", False, 512*1024*1024, 0.5, 500, "STARTED", "STARTED", None),
        ]
        
        mock_client = Mock(spec=CrateDBClient)
        analyzer = ShardAnalyzer(mock_client)
        analyzer.shards = mock_shards
        analyzer.nodes = self._create_mock_nodes()
        
        recommendation = MoveRecommendation(
            table_name="users",
            schema_name="doc",
            shard_id=0,
            from_node="Node A",
            to_node="Node C",
            from_zone="zone1",
            to_zone="zone1",  # Same zone - should be rejected
            shard_type="PRIMARY", 
            size_gb=0.5,
            reason="Test non-partitioned compatibility"
            # partition_ident is None (default)
        )
        
        # Mock query result for non-partitioned table
        mock_result = {
            'rows': [
                ["node1", "Node A", "zone1", True, "STARTED", "STARTED", None],
                ["node2", "Node B", "zone2", False, "STARTED", "STARTED", None]
            ]
        }
        
        mock_zone_result = {
            'rows': [["zone1", 1], ["zone2", 1]]
        }
        
        mock_client.execute_query.side_effect = [mock_result, mock_zone_result]
        
        conflict = analyzer._check_zone_conflict(recommendation)
        
        # Should detect conflict for non-partitioned table too
        assert conflict is not None
        
        # Verify NULL handling in query parameters
        call_args = mock_client.execute_query.call_args_list[0]
        params = call_args[0][1]
        
        # Should have None values for partition parameters
        assert None in params, "Query should handle NULL partition_ident correctly"

    def _create_mock_nodes(self):
        """Helper method to create consistent mock nodes for tests"""
        node_a = Mock()
        node_a.id = "node1"
        node_a.name = "Node A"
        node_a.zone = "zone1"

        node_b = Mock()
        node_b.id = "node2"
        node_b.name = "Node B"
        node_b.zone = "zone2"

        node_c = Mock()
        node_c.id = "node3"
        node_c.name = "Node C"
        node_c.zone = "zone1"  # Same zone as Node A for testing conflicts

        return [node_a, node_b, node_c]


class TestPartitionBugScenarios:
    """Test specific bug scenarios that were dangerous before fixes"""

    @pytest.mark.partition
    @pytest.mark.safety
    def test_dangerous_move_approval_prevented(self):
        """Test that previously dangerous moves are now correctly rejected"""
        
        # Scenario that would have been approved by broken logic:
        # Table with multiple partitions, trying to move to a zone that already
        # has the specific partition we're moving
        
        mock_shards = [
            # Multiple partitions with same shard IDs but different distributions
            ShardInfo("logs", "doc", 0, "node1", "Node A", "zone1", True, 1024*1024*1024, 1.0, 1000, "STARTED", "STARTED", "2024-01"),
            ShardInfo("logs", "doc", 0, "node2", "Node B", "zone2", False, 1024*1024*1024, 1.0, 1000, "STARTED", "STARTED", "2024-01"), 
            ShardInfo("logs", "doc", 0, "node1", "Node A", "zone1", True, 512*1024*1024, 0.5, 500, "STARTED", "STARTED", "2024-02"),
            # Note: logs[2024-02] only exists in zone1, logs[2024-01] exists in zone1 and zone2
        ]
        
        mock_client = Mock(spec=CrateDBClient) 
        analyzer = ShardAnalyzer(mock_client)
        analyzer.shards = mock_shards
        
        nodes = []
        for node_id, node_name, zone in [("node1", "Node A", "zone1"), ("node2", "Node B", "zone2"), ("node3", "Node C", "zone1")]:
            node = Mock()
            node.id = node_id
            node.name = node_name  
            node.zone = zone
            nodes.append(node)
        analyzer.nodes = nodes
        
        # Try to move logs[2024-02] from Node A to Node C (both zone1)
        # This should be REJECTED because zone1 already has logs[2024-02]
        dangerous_recommendation = MoveRecommendation(
            table_name="logs",
            schema_name="doc",
            shard_id=0,
            from_node="Node A",
            to_node="Node C", 
            from_zone="zone1",
            to_zone="zone1",
            shard_type="PRIMARY",
            size_gb=0.5,
            reason="This should be rejected",
            partition_ident="2024-02"
        )
        
        # Mock response - fixed query returns only the specific partition
        mock_client.execute_query.side_effect = [
            {'rows': [["node1", "Node A", "zone1", True, "STARTED", "STARTED", "2024-02"]]},
            {'rows': [["zone1", 1]]}
        ]
        
        conflict = analyzer._check_zone_conflict(dangerous_recommendation)
        
        # This dangerous move should now be correctly rejected
        assert conflict is not None, "Dangerous move should be rejected by fixed logic"
        assert "zone1" in conflict.lower(), "Should mention zone1 in conflict message"

    @pytest.mark.partition
    @pytest.mark.safety
    def test_safe_move_cross_partition_allowed(self):
        """Test that safe moves across partitions are still allowed"""
        
        # Scenario that should be allowed:
        # Moving a partition to a zone that has OTHER partitions but not THIS partition
        
        mock_shards = [
            ShardInfo("logs", "doc", 0, "node1", "Node A", "zone1", True, 1024*1024*1024, 1.0, 1000, "STARTED", "STARTED", "2024-01"),
            ShardInfo("logs", "doc", 0, "node2", "Node B", "zone2", False, 1024*1024*1024, 1.0, 1000, "STARTED", "STARTED", "2024-01"),
            ShardInfo("logs", "doc", 0, "node3", "Node C", "zone1", True, 512*1024*1024, 0.5, 500, "STARTED", "STARTED", "2024-02"),
        ]
        
        mock_client = Mock(spec=CrateDBClient)
        analyzer = ShardAnalyzer(mock_client)
        analyzer.shards = mock_shards
        
        nodes = []
        for node_id, node_name, zone in [("node1", "Node A", "zone1"), ("node2", "Node B", "zone2"), ("node3", "Node C", "zone1"), ("node4", "Node D", "zone2")]:
            node = Mock()
            node.id = node_id
            node.name = node_name
            node.zone = zone
            nodes.append(node)
        analyzer.nodes = nodes
        
        # Try to move logs[2024-02] from Node C (zone1) to Node D (zone2)  
        # This should be ALLOWED because zone2 has logs[2024-01] but not logs[2024-02]
        safe_recommendation = MoveRecommendation(
            table_name="logs",
            schema_name="doc", 
            shard_id=0,
            from_node="Node C",
            to_node="Node D",
            from_zone="zone1", 
            to_zone="zone2",
            shard_type="PRIMARY",
            size_gb=0.5,
            reason="This should be allowed",
            partition_ident="2024-02"
        )
        
        # Mock response - only logs[2024-02] data (partition isolation)
        mock_client.execute_query.side_effect = [
            {'rows': [["node3", "Node C", "zone1", True, "STARTED", "STARTED", "2024-02"]]},
            {'rows': [["zone1", 1]]}  # Only zone1 has this partition currently
        ]
        
        conflict = analyzer._check_zone_conflict(safe_recommendation)
        
        # This safe move should be allowed
        assert conflict is None, f"Safe cross-partition move should be allowed, got: {conflict}"


class TestPartitionQueryVerification:
    """Test that all database queries are partition-aware"""

    @pytest.mark.partition
    def test_zone_conflict_query_includes_partition_filter(self):
        """Verify zone conflict queries include proper partition filtering"""
        
        mock_client = Mock(spec=CrateDBClient)
        analyzer = ShardAnalyzer(mock_client)
        
        # Set up minimal test data
        analyzer.shards = [
            ShardInfo("events", "doc", 0, "node1", "Node A", "zone1", True, 1024*1024*1024, 1.0, 1000, "STARTED", "STARTED", "2024-01")
        ]
        
        node = Mock()
        node.id = "node2" 
        node.name = "Node B"
        node.zone = "zone2"
        analyzer.nodes = [node]
        
        recommendation = MoveRecommendation(
            table_name="events",
            schema_name="doc",
            shard_id=0,
            from_node="Node A",
            to_node="Node B", 
            from_zone="zone1",
            to_zone="zone2",
            shard_type="PRIMARY",
            size_gb=1.0,
            reason="Test query",
            partition_ident="2024-01"
        )
        
        mock_client.execute_query.side_effect = [
            {'rows': [["node1", "Node A", "zone1", True, "STARTED", "STARTED", "2024-01"]]},
            {'rows': [["zone1", 1]]}
        ]
        
        analyzer._check_zone_conflict(recommendation)
        
        # Check both query calls
        assert mock_client.execute_query.call_count == 2
        
        # First query (main conflict check)
        first_call = mock_client.execute_query.call_args_list[0]
        first_query = first_call[0][0]
        first_params = first_call[0][1]
        
        assert "s.partition_ident" in first_query
        assert "AND (s.partition_ident = ? OR (s.partition_ident IS NULL AND ? IS NULL))" in first_query
        assert "2024-01" in first_params
        
        # Second query (zone allocation check) 
        second_call = mock_client.execute_query.call_args_list[1]
        second_query = second_call[0][0]
        second_params = second_call[0][1] 
        
        assert "s.partition_ident" in second_query or "partition_ident" in second_query
        assert "2024-01" in second_params

    @pytest.mark.partition
    def test_null_partition_handling(self):
        """Test that NULL partition values are handled correctly"""
        
        mock_client = Mock(spec=CrateDBClient)
        analyzer = ShardAnalyzer(mock_client)
        
        # Non-partitioned table shard
        analyzer.shards = [
            ShardInfo("users", "doc", 0, "node1", "Node A", "zone1", True, 512*1024*1024, 0.5, 500, "STARTED", "STARTED", None)
        ]
        
        node = Mock()
        node.id = "node2"
        node.name = "Node B" 
        node.zone = "zone2"
        analyzer.nodes = [node]
        
        recommendation = MoveRecommendation(
            table_name="users",
            schema_name="doc",
            shard_id=0,
            from_node="Node A",
            to_node="Node B",
            from_zone="zone1", 
            to_zone="zone2",
            shard_type="PRIMARY",
            size_gb=0.5,
            reason="Test NULL handling"
            # partition_ident is None (default)
        )
        
        mock_client.execute_query.side_effect = [
            {'rows': [["node1", "Node A", "zone1", True, "STARTED", "STARTED", None]]},
            {'rows': [["zone1", 1]]}
        ]
        
        analyzer._check_zone_conflict(recommendation)
        
        # Verify NULL values are passed correctly
        call_args = mock_client.execute_query.call_args_list[0]
        params = call_args[0][1]
        
        # Should have None values for partition parameters
        none_count = params.count(None)
        assert none_count >= 2, f"Should have at least 2 None values for NULL handling, got: {params}"