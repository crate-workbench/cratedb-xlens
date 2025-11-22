"""
Comprehensive tests for partition-aware distribution analysis

Tests the critical fixes identified in Phase 4 audit:
- DistributionAnalyzer partition-aware queries
- Zone balance checking per partition
- Partition-level imbalance detection
- Display formatting with partition context
"""

import pytest
from unittest.mock import Mock, patch
from cratedb_xlens.distribution_analyzer import DistributionAnalyzer, TableDistribution, DistributionAnomaly
from cratedb_xlens.analyzer import ShardAnalyzer
from cratedb_xlens.database import ShardInfo, NodeInfo


class TestPartitionAwareDistributionAnalyzer:
    """Test partition-aware functionality in DistributionAnalyzer"""

    @pytest.fixture
    def mock_client_with_partitioned_data(self):
        """Mock client that returns partitioned table data"""
        client = Mock()

        # Mock data representing a partitioned table with imbalanced partitions
        partitioned_query_result = {
            'rows': [
                # Partition 2024-01: All shards on node1 (CRITICAL VIOLATION)
                ['test_schema', 'events', '2024-01', 'node1', 3, 0, 3, 15.0, 15.0, 0.0, 3000000],
                ['test_schema', 'events', '2024-01', 'node2', 0, 0, 0, 0.0, 0.0, 0.0, 0],

                # Partition 2024-02: Perfectly balanced
                ['test_schema', 'events', '2024-02', 'node1', 1, 0, 1, 5.0, 5.0, 0.0, 1000000],
                ['test_schema', 'events', '2024-02', 'node2', 1, 0, 1, 5.0, 5.0, 0.0, 1000000],

                # Partition 2024-03: Slightly imbalanced
                ['test_schema', 'events', '2024-03', 'node1', 2, 0, 2, 8.0, 8.0, 0.0, 1600000],
                ['test_schema', 'events', '2024-03', 'node2', 1, 0, 1, 4.0, 4.0, 0.0, 800000],
            ]
        }

        # Mock largest partitions query result
        largest_partitions_result = {
            'rows': [
                # Should return largest PARTITIONS, not tables
                ['test_schema', 'events', '2024-01', 'node1', 3, 0, 3, 15.0, 15.0, 0.0, 3000000],
                ['test_schema', 'events', '2024-03', 'node1', 2, 0, 2, 8.0, 8.0, 0.0, 1600000],
                ['test_schema', 'events', '2024-03', 'node2', 1, 0, 1, 4.0, 4.0, 0.0, 800000],
                ['test_schema', 'events', '2024-02', 'node1', 1, 0, 1, 5.0, 5.0, 0.0, 1000000],
                ['test_schema', 'events', '2024-02', 'node2', 1, 0, 1, 5.0, 5.0, 0.0, 1000000],
            ]
        }

        client.execute_query.side_effect = [partitioned_query_result, largest_partitions_result]
        return client

    def test_get_all_partition_distributions(self, mock_client_with_partitioned_data):
        """Test that get_all_partition_distributions returns separate distributions per partition"""
        analyzer = DistributionAnalyzer(mock_client_with_partitioned_data)

        distributions = analyzer.get_all_partition_distributions('test_schema.events')

        # Should return 3 separate distributions (one per partition)
        assert len(distributions) == 3

        # Check that partitions are properly identified
        partition_idents = [d.partition_ident for d in distributions]
        assert '2024-01' in partition_idents
        assert '2024-02' in partition_idents
        assert '2024-03' in partition_idents

        # Find the 2024-01 partition (should be largest and most imbalanced)
        partition_2024_01 = next(d for d in distributions if d.partition_ident == '2024-01')

        # Verify partition shows in full table name
        assert partition_2024_01.full_table_name == 'test_schema.events[2024-01]'

        # Verify this partition shows severe imbalance
        node_distributions = partition_2024_01.node_distributions
        assert node_distributions['node1']['primary_shards'] == 3
        assert node_distributions['node2']['primary_shards'] == 0

        # Verify 2024-02 partition is balanced
        partition_2024_02 = next(d for d in distributions if d.partition_ident == '2024-02')
        node_distributions_02 = partition_2024_02.node_distributions
        assert node_distributions_02['node1']['primary_shards'] == 1
        assert node_distributions_02['node2']['primary_shards'] == 1

    def test_get_largest_tables_distribution_now_returns_partitions(self, mock_client_with_partitioned_data):
        """Test that get_largest_tables_distribution now returns largest PARTITIONS, not tables"""
        analyzer = DistributionAnalyzer(mock_client_with_partitioned_data)

        # Reset mock to return largest partitions result
        mock_client_with_partitioned_data.execute_query.return_value = {
            'rows': [
                ['test_schema', 'events', '2024-01', 'node1', 3, 0, 3, 15.0, 15.0, 0.0, 3000000],
                ['test_schema', 'events', '2024-03', 'node1', 2, 0, 2, 8.0, 8.0, 0.0, 1600000],
                ['test_schema', 'events', '2024-03', 'node2', 1, 0, 1, 4.0, 4.0, 0.0, 800000],
            ]
        }

        distributions = analyzer.get_largest_tables_distribution(top_n=10)

        # Should return separate distributions for each partition
        assert len(distributions) >= 2  # At least 2024-01 and 2024-03 partitions

        # Verify largest partition is 2024-01 (15GB)
        largest = distributions[0]
        assert largest.partition_ident == '2024-01'
        assert largest.total_primary_size_gb == 15.0
        assert largest.full_table_name == 'test_schema.events[2024-01]'

        # Verify query was called with partition-aware SQL
        called_query = mock_client_with_partitioned_data.execute_query.call_args[0][0]
        assert 'partition_ident' in called_query
        assert 'GROUP BY schema_name, table_name, partition_ident' in called_query


class TestPartitionAwareZoneBalancing:
    """Test partition-aware zone balance checking"""

    @pytest.fixture
    def partitioned_shards_with_violations(self):
        """Create test shards representing partition-level zone violations"""
        shards = [
            # Partition 2024-01: All primaries in zone1 (CRITICAL VIOLATION)
            ShardInfo(table_name='logs', schema_name='events', shard_id=0, node_id='node1-id', node_name='node1',
                     zone='zone1', is_primary=True, size_bytes=5368709120, size_gb=5.0, num_docs=1000000,
                     state='STARTED', routing_state='STARTED', partition_ident='2024-01'),
            ShardInfo(table_name='logs', schema_name='events', shard_id=1, node_id='node2-id', node_name='node2',
                     zone='zone1', is_primary=True, size_bytes=5368709120, size_gb=5.0, num_docs=1000000,
                     state='STARTED', routing_state='STARTED', partition_ident='2024-01'),
            ShardInfo(table_name='logs', schema_name='events', shard_id=2, node_id='node3-id', node_name='node3',
                     zone='zone1', is_primary=True, size_bytes=5368709120, size_gb=5.0, num_docs=1000000,
                     state='STARTED', routing_state='STARTED', partition_ident='2024-01'),

            # Partition 2024-02: Balanced across zones
            ShardInfo(table_name='logs', schema_name='events', shard_id=3, node_id='node1-id', node_name='node1',
                     zone='zone1', is_primary=True, size_bytes=3221225472, size_gb=3.0, num_docs=600000,
                     state='STARTED', routing_state='STARTED', partition_ident='2024-02'),
            ShardInfo(table_name='logs', schema_name='events', shard_id=4, node_id='node4-id', node_name='node4',
                     zone='zone2', is_primary=True, size_bytes=3221225472, size_gb=3.0, num_docs=600000,
                     state='STARTED', routing_state='STARTED', partition_ident='2024-02'),

            # Partition 2024-03: Moderate imbalance
            ShardInfo(table_name='logs', schema_name='events', shard_id=5, node_id='node1-id', node_name='node1',
                     zone='zone1', is_primary=True, size_bytes=2147483648, size_gb=2.0, num_docs=400000,
                     state='STARTED', routing_state='STARTED', partition_ident='2024-03'),
            ShardInfo(table_name='logs', schema_name='events', shard_id=6, node_id='node2-id', node_name='node2',
                     zone='zone1', is_primary=True, size_bytes=2147483648, size_gb=2.0, num_docs=400000,
                     state='STARTED', routing_state='STARTED', partition_ident='2024-03'),
            ShardInfo(table_name='logs', schema_name='events', shard_id=7, node_id='node4-id', node_name='node4',
                     zone='zone2', is_primary=True, size_bytes=2147483648, size_gb=2.0, num_docs=400000,
                     state='STARTED', routing_state='STARTED', partition_ident='2024-03'),
        ]
        return shards

    @pytest.fixture
    def analyzer_with_partitioned_shards(self, partitioned_shards_with_violations):
        """Create analyzer with partitioned shard data"""
        nodes = [
            NodeInfo(id='node1-id', name='node1', zone='zone1', heap_used=4000000000, heap_max=8000000000, fs_total=1000000000000, fs_used=500000000000, fs_available=500000000000),
            NodeInfo(id='node2-id', name='node2', zone='zone1', heap_used=3600000000, heap_max=8000000000, fs_total=1000000000000, fs_used=450000000000, fs_available=550000000000),
            NodeInfo(id='node3-id', name='node3', zone='zone1', heap_used=3200000000, heap_max=8000000000, fs_total=1000000000000, fs_used=400000000000, fs_available=600000000000),
            NodeInfo(id='node4-id', name='node4', zone='zone2', heap_used=2800000000, heap_max=8000000000, fs_total=1000000000000, fs_used=350000000000, fs_available=650000000000),
        ]

        mock_client = Mock()
        mock_client.get_nodes_info.return_value = nodes
        mock_client.get_shards_info.return_value = partitioned_shards_with_violations
        
        analyzer = ShardAnalyzer(mock_client)
        return analyzer

    def test_check_zone_balance_partition_aware(self, analyzer_with_partitioned_shards):
        """Test that zone balance checking is now partition-aware"""
        # Check balance for the events.logs table (should return per-partition stats)
        balance_stats = analyzer_with_partitioned_shards.check_zone_balance(
            table_name='logs', tolerance_percent=10.0
        )

        # Should return partition-specific stats
        assert 'partition_2024-01' in balance_stats
        assert 'partition_2024-02' in balance_stats
        assert 'partition_2024-03' in balance_stats

        # Verify 2024-01 partition shows zone imbalance (all in zone1)
        partition_01_stats = balance_stats['partition_2024-01']
        assert partition_01_stats['zone1']['PRIMARY'] == 3
        assert 'zone2' not in partition_01_stats or partition_01_stats['zone2']['PRIMARY'] == 0

        # Verify 2024-02 partition shows balance
        partition_02_stats = balance_stats['partition_2024-02']
        assert partition_02_stats['zone1']['PRIMARY'] == 1
        assert partition_02_stats['zone2']['PRIMARY'] == 1

    def test_detect_partition_zone_violations(self, analyzer_with_partitioned_shards):
        """Test detection of partition-level zone violations"""
        violations = analyzer_with_partitioned_shards.detect_partition_zone_violations(
            table_name='logs', tolerance_percent=10.0
        )

        # Should detect violation in 2024-01 partition
        critical_violations = [v for v in violations if v['severity'] == 'CRITICAL']
        assert len(critical_violations) >= 1

        # Find the 2024-01 partition violation
        partition_01_violation = next((v for v in critical_violations
                                     if v['partition'] == '2024-01'), None)
        assert partition_01_violation is not None
        assert partition_01_violation['type'] == 'SINGLE_ZONE_CONCENTRATION'
        assert partition_01_violation['table'] == 'events.logs'
        assert 'All 3 primary shards in zone zone1' in partition_01_violation['description']

        # Should NOT detect violation in balanced 2024-02 partition
        partition_02_violations = [v for v in violations if v['partition'] == '2024-02']
        assert len(partition_02_violations) == 0

    def test_masked_imbalance_scenario_critical_test_case(self):
        """
        CRITICAL TEST: Verify that partition-level imbalances are NOT masked by table-level aggregation
        This is the exact scenario described in the audit findings.
        """
        # Create scenario where table appears balanced overall but has severe partition imbalance
        shards = [
            # Partition A: Severely imbalanced (all shards on zone1)
            ShardInfo(table_name='events', schema_name='doc', shard_id=0, node_id='node1-id', node_name='node1',
                     zone='zone1', is_primary=True, size_bytes=10737418240, size_gb=10.0, num_docs=2000000,
                     state='STARTED', routing_state='STARTED', partition_ident='2024-01'),
            ShardInfo(table_name='events', schema_name='doc', shard_id=1, node_id='node2-id', node_name='node2',
                     zone='zone1', is_primary=True, size_bytes=10737418240, size_gb=10.0, num_docs=2000000,
                     state='STARTED', routing_state='STARTED', partition_ident='2024-01'),

            # Partition B: All shards on zone2 (balances table overall, but still violation per partition)
            ShardInfo(table_name='events', schema_name='doc', shard_id=2, node_id='node3-id', node_name='node3',
                     zone='zone2', is_primary=True, size_bytes=10737418240, size_gb=10.0, num_docs=2000000,
                     state='STARTED', routing_state='STARTED', partition_ident='2024-02'),
            ShardInfo(table_name='events', schema_name='doc', shard_id=3, node_id='node4-id', node_name='node4',
                     zone='zone2', is_primary=True, size_bytes=10737418240, size_gb=10.0, num_docs=2000000,
                     state='STARTED', routing_state='STARTED', partition_ident='2024-02'),
        ]

        nodes = [
            NodeInfo(id='node1-id', name='node1', zone='zone1', heap_used=4000000000, heap_max=8000000000, fs_total=1000000000000, fs_used=500000000000, fs_available=500000000000),
            NodeInfo(id='node2-id', name='node2', zone='zone1', heap_used=4000000000, heap_max=8000000000, fs_total=1000000000000, fs_used=500000000000, fs_available=500000000000),
            NodeInfo(id='node3-id', name='node3', zone='zone2', heap_used=4000000000, heap_max=8000000000, fs_total=1000000000000, fs_used=500000000000, fs_available=500000000000),
            NodeInfo(id='node4-id', name='node4', zone='zone2', heap_used=4000000000, heap_max=8000000000, fs_total=1000000000000, fs_used=500000000000, fs_available=500000000000),
        ]

        mock_client = Mock()
        mock_client.get_nodes_info.return_value = nodes
        mock_client.get_shards_info.return_value = shards
        
        analyzer = ShardAnalyzer(mock_client)

        # OLD BEHAVIOR (BROKEN): Would show table as "balanced" (2 shards per zone)
        # NEW BEHAVIOR (FIXED): Should detect TWO partition-level violations

        violations = analyzer.detect_partition_zone_violations(table_name='events')

        # Must detect BOTH partitions as having zone violations
        assert len(violations) == 2, f"Expected 2 violations (one per partition), got {len(violations)}"

        # Verify both partitions flagged as critical violations
        critical_violations = [v for v in violations if v['severity'] == 'CRITICAL']
        assert len(critical_violations) == 2

        # Verify specific partition violations detected
        partition_names = [v['partition'] for v in violations]
        assert '2024-01' in partition_names
        assert '2024-02' in partition_names

        # Verify descriptions are accurate
        for violation in violations:
            assert 'All 2 primary shards' in violation['description']
            assert violation['type'] == 'SINGLE_ZONE_CONCENTRATION'


class TestPartitionDisplayFormatting:
    """Test that partition information is properly displayed"""

    def test_table_distribution_full_name_includes_partition(self):
        """Test that TableDistribution.full_table_name includes partition identifier"""
        # Non-partitioned table
        non_partitioned = TableDistribution(
            schema_name='doc',
            table_name='logs',
            partition_ident=None,
            total_primary_size_gb=10.0,
            node_distributions={}
        )
        assert non_partitioned.full_table_name == 'logs'

        # Partitioned table
        partitioned = TableDistribution(
            schema_name='events',
            table_name='logs',
            partition_ident='2024-01',
            total_primary_size_gb=15.0,
            node_distributions={}
        )
        assert partitioned.full_table_name == 'events.logs[2024-01]'

    def test_distribution_anomaly_includes_partition_context(self):
        """Test that DistributionAnomaly includes partition information"""
        table_dist = TableDistribution(
            schema_name='events',
            table_name='logs',
            partition_ident='2024-Q1',
            total_primary_size_gb=20.0,
            node_distributions={}
        )

        anomaly = DistributionAnomaly(
            table=table_dist,
            anomaly_type='ZONE_IMBALANCE',
            severity_score=85.0,
            impact_score=70.0,
            combined_score=77.5,
            description='Partition has severe zone imbalance',
            details={'zone_distribution': {'zone1': 3, 'zone2': 0}},
            recommendations=['Redistribute shards across zones'],
            partition_ident='2024-Q1'
        )

        # Verify partition context is preserved
        assert anomaly.partition_ident == '2024-Q1'
        assert anomaly.full_identifier == 'events.logs[2024-Q1]'


class TestPartitionQueryValidation:
    """Test that SQL queries are properly updated to be partition-aware"""

    def test_get_table_distribution_detailed_query_includes_partition(self):
        """Verify the SQL query includes partition_ident in GROUP BY"""
        mock_client = Mock()
        mock_client.execute_query.return_value = {'rows': []}

        analyzer = DistributionAnalyzer(mock_client)
        analyzer.get_table_distribution_detailed('test.table')

        # Verify the query was called
        assert mock_client.execute_query.called
        called_query = mock_client.execute_query.call_args[0][0]

        # Critical fix verification: Query must include partition in GROUP BY
        assert 'COALESCE(s.partition_ident, \'\') as partition_ident' in called_query
        assert 'GROUP BY s.schema_name, s.table_name' in called_query
        assert 'ORDER BY COALESCE(s.partition_ident' in called_query

    def test_get_largest_tables_distribution_query_partition_aware(self):
        """Verify largest tables query now finds largest PARTITIONS"""
        mock_client = Mock()
        mock_client.execute_query.return_value = {'rows': []}

        analyzer = DistributionAnalyzer(mock_client)
        analyzer.get_largest_tables_distribution(top_n=5)

        called_query = mock_client.execute_query.call_args[0][0]

        # Critical fix verification: Query must group by partition to find largest partitions
        assert 'WITH largest_partitions AS' in called_query
        assert 'GROUP BY schema_name, table_name, partition_ident' in called_query
        assert 'COALESCE(s.partition_ident, \'\') as partition_ident' in called_query


class TestBackwardCompatibility:
    """Ensure partition fixes don't break non-partitioned tables"""

    @pytest.fixture
    def non_partitioned_data(self):
        """Mock data for non-partitioned table"""
        return {
            'rows': [
                ['doc', 'simple_table', '', 'node1', 2, 0, 2, 8.0, 8.0, 0.0, 1600000],
                ['doc', 'simple_table', '', 'node2', 2, 0, 2, 7.0, 7.0, 0.0, 1400000],
            ]
        }

    def test_non_partitioned_tables_still_work(self, non_partitioned_data):
        """Ensure non-partitioned tables continue to work correctly"""
        mock_client = Mock()
        mock_client.execute_query.return_value = non_partitioned_data

        analyzer = DistributionAnalyzer(mock_client)
        distribution = analyzer.get_table_distribution_detailed('simple_table')

        # Should work without partition_ident
        assert distribution is not None
        assert distribution.schema_name == 'doc'
        assert distribution.table_name == 'simple_table'
        assert distribution.partition_ident is None
        assert distribution.full_table_name == 'simple_table'  # No partition suffix

        # Should have node distributions
        assert len(distribution.node_distributions) == 2
        assert 'node1' in distribution.node_distributions
        assert 'node2' in distribution.node_distributions


@pytest.mark.integration
class TestPartitionIntegrationScenarios:
    """Integration tests for complex partition scenarios"""

    def test_mixed_partitioned_and_non_partitioned_tables(self):
        """Test handling cluster with both partitioned and non-partitioned tables"""
        # This would be an integration test with real database
        # For now, just verify our data structures can handle mixed scenarios

        mixed_shards = [
            # Non-partitioned table
            ShardInfo(table_name='users', schema_name='doc', shard_id=0, node_id='node1-id', node_name='node1',
                     zone='zone1', is_primary=True, size_bytes=5368709120, size_gb=5.0, num_docs=1000000,
                     state='STARTED', routing_state='STARTED', partition_ident=None),

            # Partitioned table
            ShardInfo(table_name='logs', schema_name='events', shard_id=1, node_id='node1-id', node_name='node1',
                     zone='zone1', is_primary=True, size_bytes=3221225472, size_gb=3.0, num_docs=600000,
                     state='STARTED', routing_state='STARTED', partition_ident='2024-01'),
        ]

        nodes = [NodeInfo(id='node1-id', name='node1', zone='zone1', heap_used=4000000000, heap_max=8000000000, fs_total=1000000000000, fs_used=500000000000, fs_available=500000000000)]
        mock_client = Mock()
        mock_client.get_nodes_info.return_value = nodes
        mock_client.get_shards_info.return_value = mixed_shards
        
        analyzer = ShardAnalyzer(mock_client)

        # Should handle mixed scenarios without errors
        balance_stats = analyzer.check_zone_balance()
        assert isinstance(balance_stats, dict)

    def test_large_number_of_partitions_performance(self):
        """Verify solution scales to tables with many partitions"""
        # Create shards for table with 100 partitions (2 shards per partition for zone violation detection)
        many_partition_shards = []
        for i in range(100):
            partition_id = f'2024-{i:02d}'
            # Add 2 shards per partition, both in same zone (creates violation)
            many_partition_shards.append(
                ShardInfo(table_name='metrics', schema_name='timeseries', shard_id=i*2, node_id='node1-id',
                         node_name='node1', zone='zone1', is_primary=True, size_bytes=1073741824,
                         size_gb=1.0, num_docs=200000, state='STARTED', routing_state='STARTED',
                         partition_ident=partition_id)
            )
            many_partition_shards.append(
                ShardInfo(table_name='metrics', schema_name='timeseries', shard_id=i*2+1, node_id='node2-id',
                         node_name='node2', zone='zone1', is_primary=True, size_bytes=1073741824,
                         size_gb=1.0, num_docs=200000, state='STARTED', routing_state='STARTED',
                         partition_ident=partition_id)
            )

        nodes = [
            NodeInfo(id='node1-id', name='node1', zone='zone1', heap_used=4000000000, heap_max=8000000000, fs_total=1000000000000, fs_used=500000000000, fs_available=500000000000),
            NodeInfo(id='node2-id', name='node2', zone='zone1', heap_used=4000000000, heap_max=8000000000, fs_total=1000000000000, fs_used=500000000000, fs_available=500000000000)
        ]
        mock_client = Mock()
        mock_client.get_nodes_info.return_value = nodes
        mock_client.get_shards_info.return_value = many_partition_shards
        
        analyzer = ShardAnalyzer(mock_client)

        # Should handle large partition counts efficiently
        violations = analyzer.detect_partition_zone_violations(table_name='metrics')

        # All partitions should be flagged (all shards in single zone)
        assert len(violations) == 100
