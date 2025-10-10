"""
Tests for distribution analyzer functionality
"""

import pytest
from unittest.mock import Mock, patch
from xmover.distribution_analyzer import DistributionAnalyzer, TableDistribution, DistributionAnomaly
from xmover.database import CrateDBClient, NodeInfo


class TestDistributionAnalyzer:
    
    def setup_method(self):
        """Set up test fixtures"""
        self.mock_client = Mock(spec=CrateDBClient)
        self.analyzer = DistributionAnalyzer(self.mock_client)
    
    def test_coefficient_of_variation_calculation(self):
        """Test CV calculation with different scenarios"""
        
        # Normal case
        values = [10, 12, 8, 14, 6]
        cv = self.analyzer.calculate_coefficient_of_variation(values)
        assert cv > 0
        
        # All equal values (should return 0)
        equal_values = [10, 10, 10, 10]
        cv_equal = self.analyzer.calculate_coefficient_of_variation(equal_values)
        assert cv_equal == 0.0
        
        # Empty list
        empty_values = []
        cv_empty = self.analyzer.calculate_coefficient_of_variation(empty_values)
        assert cv_empty == 0.0
        
        # Single value
        single_value = [10]
        cv_single = self.analyzer.calculate_coefficient_of_variation(single_value)
        assert cv_single == 0.0
    
    def test_get_largest_tables_distribution(self):
        """Test fetching table distribution data"""
        
        # Mock query results - updated to include partition_ident column
        mock_results = [
            # schema, table, partition_ident, node, primary_shards, replica_shards, total_shards, total_size, primary_size, replica_size, docs
            ['doc', 'large_table', '', 'node1', 5, 2, 7, 100.5, 80.2, 20.3, 1000000],
            ['doc', 'large_table', '', 'node2', 4, 3, 7, 95.1, 75.8, 19.3, 950000],
            ['doc', 'large_table', '', 'node3', 6, 1, 7, 110.2, 85.9, 24.3, 1100000],
            ['custom', 'another_table', '', 'node1', 3, 2, 5, 50.1, 40.2, 9.9, 500000],
            ['custom', 'another_table', '', 'node2', 2, 3, 5, 45.8, 35.1, 10.7, 480000],
        ]
        
        self.mock_client.execute_query.return_value = {'rows': mock_results}
        
        distributions = self.analyzer.get_largest_tables_distribution(top_n=10)
        
        # Verify query was called with correct parameters
        self.mock_client.execute_query.assert_called_once()
        call_args = self.mock_client.execute_query.call_args
        assert call_args[0][1] == [10]  # top_n parameter
        
        # Verify we got the expected number of tables
        assert len(distributions) == 2
        
        # Verify table data structure
        large_table = next(d for d in distributions if d.table_name == 'large_table')
        assert large_table.schema_name == 'doc'
        assert large_table.partition_ident is None or large_table.partition_ident == ''
        assert large_table.full_table_name == 'large_table'  # Should omit 'doc' schema
        assert len(large_table.node_distributions) == 3
        
        another_table = next(d for d in distributions if d.table_name == 'another_table')
        assert another_table.schema_name == 'custom'
        assert another_table.partition_ident is None or another_table.partition_ident == ''
        assert another_table.full_table_name == 'custom.another_table'
        assert len(another_table.node_distributions) == 2
        
        # Verify sorting by primary size (descending)
        assert distributions[0].total_primary_size_gb >= distributions[1].total_primary_size_gb
    
    def test_detect_shard_count_imbalance(self):
        """Test shard count imbalance detection"""
        
        # Create test table with imbalanced shard distribution
        imbalanced_table = TableDistribution(
            schema_name='doc',
            table_name='imbalanced_table',
            partition_ident=None,
            total_primary_size_gb=500.0,
            node_distributions={
                'node1': {'total_shards': 10, 'primary_shards': 5, 'replica_shards': 5},
                'node2': {'total_shards': 15, 'primary_shards': 8, 'replica_shards': 7},
                'node3': {'total_shards': 5, 'primary_shards': 2, 'replica_shards': 3},
            }
        )
        
        anomaly = self.analyzer.detect_shard_count_imbalance(imbalanced_table)
        
        assert anomaly is not None
        assert anomaly.anomaly_type == "Shard Count Imbalance"
        assert anomaly.combined_score > 0
        assert len(anomaly.recommendations) > 0
        
        # Create balanced table (should not detect anomaly)
        balanced_table = TableDistribution(
            schema_name='doc',
            table_name='balanced_table',
            partition_ident=None,
            total_primary_size_gb=300.0,
            node_distributions={
                'node1': {'total_shards': 10, 'primary_shards': 5, 'replica_shards': 5},
                'node2': {'total_shards': 10, 'primary_shards': 5, 'replica_shards': 5},
                'node3': {'total_shards': 10, 'primary_shards': 5, 'replica_shards': 5},
            }
        )
        
        no_anomaly = self.analyzer.detect_shard_count_imbalance(balanced_table)
        assert no_anomaly is None
    
    def test_detect_storage_imbalance(self):
        """Test storage imbalance detection"""
        
        # Create test table with storage imbalance
        storage_imbalanced_table = TableDistribution(
            schema_name='doc',
            table_name='storage_imbalanced',
            partition_ident=None,
            total_primary_size_gb=1000.0,
            node_distributions={
                'node1': {'total_size_gb': 500.0, 'primary_size_gb': 400.0, 'replica_size_gb': 100.0},
                'node2': {'total_size_gb': 300.0, 'primary_size_gb': 250.0, 'replica_size_gb': 50.0},
                'node3': {'total_size_gb': 200.0, 'primary_size_gb': 150.0, 'replica_size_gb': 50.0},
            }
        )
        
        anomaly = self.analyzer.detect_storage_imbalance(storage_imbalanced_table)
        
        assert anomaly is not None
        assert anomaly.anomaly_type == "Storage Imbalance"
        assert anomaly.combined_score > 0
        
        # Small table (should be ignored)
        small_table = TableDistribution(
            schema_name='doc',
            table_name='small_table',
            partition_ident=None,
            total_primary_size_gb=5.0,  # Small size
            node_distributions={
                'node1': {'total_size_gb': 25.0, 'primary_size_gb': 15.0, 'replica_size_gb': 10.0},
                'node2': {'total_size_gb': 25.0, 'primary_size_gb': 15.0, 'replica_size_gb': 10.0},
            }
        )
        
        no_anomaly = self.analyzer.detect_storage_imbalance(small_table)
        assert no_anomaly is None
    
    def test_detect_node_coverage_issues(self):
        """Test node coverage issue detection"""
        
        # Mock nodes_info to simulate cluster with 4 nodes
        class MockNode:
            def __init__(self, name):
                self.name = name
        
        mock_nodes = [
            MockNode('node1'), MockNode('node2'), 
            MockNode('node3'), MockNode('node4')
        ]
        self.mock_client.get_nodes_info.return_value = mock_nodes
        
        # Table with limited coverage (only on 2 out of 4 nodes)
        limited_coverage_table = TableDistribution(
            schema_name='doc',
            table_name='limited_coverage',
            partition_ident=None,
            total_primary_size_gb=100.0,  # Significant size
            node_distributions={
                'node1': {'total_shards': 10, 'primary_shards': 5, 'replica_shards': 5},
                'node2': {'total_shards': 10, 'primary_shards': 5, 'replica_shards': 5},
                # node3 and node4 missing
            }
        )
        
        anomaly = self.analyzer.detect_node_coverage_issues(limited_coverage_table)
        
        assert anomaly is not None
        assert anomaly.anomaly_type == "Node Coverage Issue"
        assert 'node3' in anomaly.details['nodes_without_shards']
        assert 'node4' in anomaly.details['nodes_without_shards']
        assert len(anomaly.recommendations) > 0
    
    def test_detect_document_imbalance(self):
        """Test document imbalance detection"""
        
        # Table with document imbalance
        doc_imbalanced_table = TableDistribution(
            schema_name='doc',
            table_name='doc_imbalanced',
            partition_ident=None,
            total_primary_size_gb=200.0,
            node_distributions={
                'node1': {'total_documents': 1000000},  # 1M docs
                'node2': {'total_documents': 500000},   # 500K docs
                'node3': {'total_documents': 100000},   # 100K docs (5x imbalance)
            }
        )
        
        anomaly = self.analyzer.detect_document_imbalance(doc_imbalanced_table)
        
        assert anomaly is not None
        assert anomaly.anomaly_type == "Document Imbalance"
        assert "data skew" in anomaly.recommendations[0].lower()
        
        # Table with very few documents (should be ignored)
        low_doc_table = TableDistribution(
            schema_name='doc',
            table_name='low_docs',
            partition_ident=None,
            total_primary_size_gb=100.0,
            node_distributions={
                'node1': {'total_documents': 1000},  # Very low count
                'node2': {'total_documents': 500},
            }
        )
        
        no_anomaly = self.analyzer.detect_document_imbalance(low_doc_table)
        assert no_anomaly is None
    
    def test_analyze_distribution_integration(self):
        """Test the full analysis workflow"""
        
        # Mock the get_largest_tables_distribution method
        mock_table = TableDistribution(
            schema_name='doc',
            table_name='test_table',
            partition_ident=None,
            total_primary_size_gb=500.0,
            node_distributions={
                'node1': {
                    'total_shards': 15, 'primary_shards': 8, 'replica_shards': 7,
                    'total_size_gb': 200.0, 'primary_size_gb': 120.0, 'replica_size_gb': 80.0,
                    'total_documents': 1000000
                },
                'node2': {
                    'total_shards': 10, 'primary_shards': 5, 'replica_shards': 5,
                    'total_size_gb': 150.0, 'primary_size_gb': 90.0, 'replica_size_gb': 60.0,
                    'total_documents': 500000
                },
                'node3': {
                    'total_shards': 5, 'primary_shards': 2, 'replica_shards': 3,
                    'total_size_gb': 100.0, 'primary_size_gb': 60.0, 'replica_size_gb': 40.0,
                    'total_documents': 100000
                }
            }
        )
        
        with patch.object(self.analyzer, 'get_largest_tables_distribution', return_value=[mock_table]):
            anomalies, tables_analyzed = self.analyzer.analyze_distribution(top_tables=10)
            
            # Should detect multiple types of anomalies
            assert len(anomalies) > 0
            assert tables_analyzed == 1  # We provided 1 mock table
            
            # Anomalies should be sorted by combined score (descending)
            if len(anomalies) > 1:
                for i in range(len(anomalies) - 1):
                    assert anomalies[i].combined_score >= anomalies[i + 1].combined_score
            
            # Each anomaly should have required fields
            for anomaly in anomalies:
                assert anomaly.table is not None
                assert anomaly.anomaly_type is not None
                assert anomaly.combined_score >= 0
                assert isinstance(anomaly.recommendations, list)
    
    def test_format_distribution_report_no_anomalies(self):
        """Test report formatting when no anomalies found"""
        
        # This should not raise an exception
        with patch('builtins.print'):  # Mock print to avoid console output during tests
            self.analyzer.format_distribution_report([], 5)
    
    def test_format_distribution_report_with_anomalies(self):
        """Test report formatting with anomalies"""
        
        mock_anomaly = DistributionAnomaly(
            table=TableDistribution('doc', 'test_table', None, 100.0, {}),
            anomaly_type='Test Anomaly',
            severity_score=7.5,
            impact_score=8.0,
            combined_score=60.0,
            description='Test description',
            details={},
            recommendations=['Test recommendation']
        )
        
        # This should not raise an exception
        with patch('builtins.print'):  # Mock print to avoid console output during tests
            self.analyzer.format_distribution_report([mock_anomaly], 3)


if __name__ == '__main__':
    pytest.main([__file__])