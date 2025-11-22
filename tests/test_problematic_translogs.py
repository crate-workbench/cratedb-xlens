"""
Tests for problematic translogs functionality with replica management
"""

import pytest
from unittest.mock import Mock, patch
from click.testing import CliRunner
from cratedb_xlens.cli import main
from cratedb_xlens.commands.maintenance import MaintenanceCommands
from cratedb_xlens.database import CrateDBClient


class TestProblematicTranslogs:

    def setup_method(self):
        """Set up test fixtures"""
        self.runner = CliRunner()
        self.mock_client = Mock(spec=CrateDBClient)

    def test_adaptive_threshold_filtering(self):
        """Test that tables with high flush_threshold_size are not incorrectly flagged"""
        # Shard with 518.9MB translog but table has 2048MB flush threshold
        individual_shards_data = [
            ['ACME', 'orders', None, 10, 'data-hot-7', 518.9]
        ]
        summary_data = [
            ['ACME', 'orders', None, None, 1, 518.9, 3, 6, 8.2, 16.3]
        ]
        # Flush threshold query returns 2048MB (2147483648 bytes) for this table
        flush_threshold_data = [
            ['ACME', 'orders', 2147483648]  # 2048 MB in bytes
        ]

        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': flush_threshold_data},    # Flush threshold query
        ]
        self.mock_client.test_connection.return_value = True

        with patch('cratedb_xlens.cli.CrateDBClient', return_value=self.mock_client):
            # Use default 512MB threshold
            result = self.runner.invoke(main, ['problematic-translogs'])

        assert result.exit_code == 0
        # Should NOT show any problematic tables because 518.9 < max(512, 2048*1.1) = 2252.8
        assert 'No problematic translog shards found' in result.output

    def test_no_problematic_tables(self):
        """Test when no tables meet the criteria"""
        self.mock_client.execute_query.return_value = {'rows': []}
        self.mock_client.test_connection.return_value = True

        with patch('cratedb_xlens.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs', '--sizeMB', '300'])

        assert result.exit_code == 0
        assert 'No problematic translog shards found' in result.output

    def test_non_partitioned_table_command_generation(self):
        """Test ALTER command generation for non-partitioned tables"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['ACME', 'shipments', None, 14, 'data-hot-6', 7011.8],
            ['ACME', 'orders', None, 5, 'data-hot-1', 600.5]  # Increased to exceed 512MB
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['ACME', 'shipments', None, None, 3, 7011.8, 5, 5, 12.4, 12.1],
            ['ACME', 'orders', None, None, 1, 600.5, 3, 6, 8.2, 16.3]
        ]
        # Flush threshold query - both tables use default 512MB
        flush_threshold_data = [
            ['ACME', 'orders', 536870912],  # 512 MB default
            ['ACME', 'shipments', 536870912],  # 512 MB default
        ]

        # Set up mock call sequence - includes flush threshold and replica count queries
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': flush_threshold_data},    # Flush threshold query
            {'rows': [['1']]},                 # Replica count for shipments (for display)
            {'rows': [['2']]},                 # Replica count for orders (for display)
            {'rows': [['1']]},                 # Replica count for shipments (for command gen)
            {'rows': [['2']]},                 # Replica count for orders (for command gen)
        ]
        self.mock_client.test_connection.return_value = True

        with patch('cratedb_xlens.cli.CrateDBClient') as mock_client_class:
            mock_client_class.return_value = self.mock_client
            result = self.runner.invoke(main, ['problematic-translogs', '--sizeMB', '300', '--execute'])

        assert result.exit_code == 0
        assert 'Problematic Replica Shards' in result.output
        assert 'Tables with Problematic Replicas' in result.output
        assert '1. Stop Automatic Shard Rebalancing:' in result.output

        # Check that REROUTE CANCEL commands are present
        assert 'REROUTE CANCEL' in result.output
        assert 'ALTER TABLE "ACME"."shipments"' in result.output
        assert 'ALTER TABLE "ACME"."orders"' in result.output

    def test_partitioned_table_command_generation(self):
        """Test ALTER command generation for partitioned tables"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['ACME', 'shipments_events', '("sync_day"=1757376000000)', 3, 'data-hot-2', 600.0],
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['ACME', 'shipments_events', '("sync_day"=1757376000000)', 'partition123', 2, 600.0, 2, 2, 1.1, 1.0],
        ]
        # Flush threshold queries - table level then partition level (4 columns)
        table_flush_threshold_data = [
            ['ACME', 'shipments_events', 536870912],  # 512 MB default
        ]
        partition_flush_threshold_data = [
            ['ACME', 'shipments_events', '("sync_day"=1757376000000)', 536870912],  # 4 columns for partitions
        ]

        # Set up mock call sequence
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': table_flush_threshold_data},    # Table flush threshold query
            {'rows': partition_flush_threshold_data},  # Partition flush threshold query
            {'rows': [['1']]},                 # Replica count for partitioned table (for display)
            {'rows': [['1']]},                 # Replica count for partitioned table (for command gen)
        ]
        self.mock_client.test_connection.return_value = True

        with patch('cratedb_xlens.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs', '--sizeMB', '300', '--execute'])

        assert result.exit_code == 0
        assert 'Problematic Replica Shards' in result.output
        assert '1. Stop Automatic Shard Rebalancing:' in result.output

        # Check that partitioned table commands are present
        assert 'ALTER TABLE "ACME"."shipments_events"' in result.output
        assert 'REROUTE CANCEL' in result.output
        assert '("sync_day"=1757376000000)' in result.output

    def test_mixed_partitioned_non_partitioned(self):
        """Test handling of both partitioned and non-partitioned tables"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['ACME', 'shipments', None, 14, 'data-hot-6', 7011.8],
            ['ACME', 'shipments_events', '("sync_day"=1757376000000)', 3, 'data-hot-2', 600.0],
            ['ACME', 'orders', None, 5, 'data-hot-1', 650.5]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['ACME', 'shipments', None, None, 2, 7011.8, 5, 5, 12.4, 12.1],
            ['ACME', 'shipments_events', '("sync_day"=1757376000000)', 'partition123', 1, 600.0, 2, 2, 1.1, 1.0],
            ['ACME', 'orders', None, None, 1, 650.5, 3, 6, 8.2, 16.3]
        ]
        # Flush threshold queries - table level then partition level
        table_flush_threshold_data = [
            ['ACME', 'shipments', 536870912],  # 512 MB default
            ['ACME', 'orders', 536870912],  # 512 MB default
            ['ACME', 'shipments_events', 536870912],  # 512 MB default
        ]
        partition_flush_threshold_data = [
            ['ACME', 'shipments_events', '("sync_day"=1757376000000)', 536870912],  # 4 columns for partitions
        ]

        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': table_flush_threshold_data},    # Table flush threshold query
            {'rows': partition_flush_threshold_data},  # Partition flush threshold query
            {'rows': [['2']]},                 # Replica count for shipments
            {'rows': [['1']]},                 # Replica count for partitioned table
            {'rows': [['3']]},                 # Replica count for orders
        ]
        self.mock_client.test_connection.return_value = True

        with patch('cratedb_xlens.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs', '--sizeMB', '200'])

        assert result.exit_code == 0
        assert 'Found 3 table/partition(s) with problematic translogs' in result.output

        # Check that the table summary is displayed correctly
        assert 'Tables with Problematic Replicas' in result.output
        assert 'shipments' in result.output or 'shipments' in result.output
        assert 'orders' in result.output or 'orders' in result.output
        assert '7011.8' in result.output  # Max translog MB for shipments
        assert '600.0' in result.output   # Max translog MB for partitioned table
        assert '650.5' in result.output   # Max translog MB for orders

        # Check hint about --execute flag
        assert '--execute flag to generate comprehensive shard management commands' in result.output

    def test_query_parameters(self):
        """Test that the query is called with correct parameters"""
        self.mock_client.execute_query.return_value = {'rows': []}
        self.mock_client.test_connection.return_value = True

        with patch('cratedb_xlens.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs', '--sizeMB', '500'])

        # Verify the query was called twice (individual shards + summary)
        assert self.mock_client.execute_query.call_count == 2
        call_args = self.mock_client.execute_query.call_args
        query = call_args[0][0]
        parameters = call_args[0][1]

        assert 'COALESCE(sh.translog_stats[\'uncommitted_size\'], 0) > ? * 1024^2' in query
        assert 'primary=FALSE' in query
        assert 'GROUP BY' in query
        assert 'max_translog_uncommitted_mb DESC' in query
        assert parameters == [500, 500, 500]

    def test_execute_flag_user_confirmation_no(self):
        """Test --execute flag generates commands for display"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['ACME', 'shipments', None, 14, 'data-hot-6', 7011.8]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['ACME', 'shipments', None, None, 1, 7011.8, 5, 5, 12.4, 12.1]
        ]
        flush_threshold_data = [
            ['ACME', 'shipments', 536870912],  # 512 MB default
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': flush_threshold_data},    # Flush threshold query
            {'rows': [['1']]},                 # Replica count for display
            {'rows': [['1']]},                 # Replica count for command generation
        ]
        self.mock_client.test_connection.return_value = True

        with patch('cratedb_xlens.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs', '--execute'])

        assert result.exit_code == 0
        assert 'Generated Comprehensive Shard Management Commands' in result.output
        assert 'REROUTE CANCEL' in result.output
        assert 'SET ("number_of_replicas" = 0)' in result.output
        # Should be called 5 times: individual query, summary query, flush threshold, 2x replica count queries
        assert self.mock_client.execute_query.call_count == 5

    def test_execute_flag_command_generation(self):
        """Test --execute flag generates comprehensive commands"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['ACME', 'shipments', None, 14, 'data-hot-6', 7011.8]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['ACME', 'shipments', None, None, 1, 7011.8, 5, 5, 12.4, 12.1]
        ]
        flush_threshold_data = [
            ['ACME', 'shipments', 536870912],  # 512 MB default
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': flush_threshold_data},    # Flush threshold query
            {'rows': [['1']]},                 # Replica count for display
            {'rows': [['1']]},                 # Replica count for command generation
        ]
        self.mock_client.test_connection.return_value = True

        with patch('cratedb_xlens.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs', '--execute'])

        assert result.exit_code == 0
        assert 'Generated Comprehensive Shard Management Commands' in result.output
        assert 'Stop Automatic Shard Rebalancing' in result.output
        assert 'REROUTE CANCEL' in result.output
        assert 'SET ("number_of_replicas" = 0)' in result.output
        assert 'Restore replicas to original value' in result.output
        assert 'Re-enable Automatic Shard Rebalancing' in result.output

        # Should be called 5 times: individual query, summary query, flush threshold, 2x replica count queries
        assert self.mock_client.execute_query.call_count == 5

    def test_execute_flag_comprehensive_commands(self):
        """Test --execute flag displays all comprehensive commands"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['ACME', 'shipments', None, 14, 'data-hot-6', 7011.8]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['ACME', 'shipments', None, None, 1, 7011.8, 5, 5, 12.4, 12.1]
        ]
        flush_threshold_data = [
            ['ACME', 'shipments', 536870912],  # 512 MB default
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': flush_threshold_data},    # Flush threshold query
            {'rows': [['1']]},                 # Replica count for display
            {'rows': [['1']]},                 # Replica count for command generation
        ]
        self.mock_client.test_connection.return_value = True

        with patch('cratedb_xlens.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs', '--execute'])

        assert result.exit_code == 0
        assert 'Generated Comprehensive Shard Management Commands' in result.output
        assert '1. Stop Automatic Shard Rebalancing:' in result.output
        assert '2. REROUTE CANCEL Commands:' in result.output
        assert '3. Set replicas to 0:' in result.output
        assert '4. Monitor retention leases:' in result.output
        assert '5. Restore replicas to original value:' in result.output
        assert '6. Re-enable Automatic Shard Rebalancing:' in result.output
        assert 'Total Commands:' in result.output

        # Should be called 5 times: individual query, summary query, flush threshold, 2x replica count queries
        assert self.mock_client.execute_query.call_count == 5

    def test_execute_flag_with_valid_replica_counts(self):
        """Test that execute flag works correctly when replica counts are available"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['ACME', 'shipments', None, 14, 'data-hot-6', 7011.8]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['ACME', 'shipments', None, None, 1, 7011.8, 5, 5, 12.4, 12.1]
        ]
        flush_threshold_data = [
            ['ACME', 'shipments', 536870912],  # 512 MB default
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': flush_threshold_data},    # Flush threshold query
            {'rows': [['1']]},                 # Replica count for display
            {'rows': [['1']]},                 # Replica count for command generation
        ]
        self.mock_client.test_connection.return_value = True

        with patch('cratedb_xlens.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs', '--execute'])

        assert result.exit_code == 0
        assert 'Generated Comprehensive Shard Management Commands' in result.output
        assert '1 set replicas to 0 commands' in result.output
        assert '1 restore replicas commands' in result.output

    def test_skip_tables_with_unknown_replicas(self):
        """Test handling tables with unknown replica counts"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['ACME', 'shipments', None, 14, 'data-hot-6', 7011.8]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['ACME', 'shipments', None, None, 1, 7011.8, 5, 5, 12.4, 12.1]
        ]
        flush_threshold_data = [
            ['ACME', 'shipments', 536870912],  # 512 MB default
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': flush_threshold_data},    # Flush threshold query
            Exception("Cannot get replica count"),  # Replica count query fails
        ]
        self.mock_client.test_connection.return_value = True

        with patch('cratedb_xlens.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs'])

        assert result.exit_code == 0
        assert 'Warning: Could not determine replica count' in result.output
        assert 'Tables with Problematic Replicas' in result.output
        assert '?' in result.output  # Unknown replica count shown as ?
        assert 'shipments' in result.output or 'shipme…' in result.output

    def test_skip_tables_with_zero_replicas(self):
        """Test handling tables that already have 0 replicas"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['ACME', 'shipments', None, 14, 'data-hot-6', 7011.8]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['ACME', 'shipments', None, None, 1, 7011.8, 5, 5, 12.4, 12.1]
        ]
        flush_threshold_data = [
            ['ACME', 'shipments', 536870912],  # 512 MB default
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': flush_threshold_data},    # Flush threshold query
            {'rows': [['0']]},                 # Replica count query returns 0
        ]
        self.mock_client.test_connection.return_value = True

        with patch('cratedb_xlens.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs'])

        assert result.exit_code == 0
        assert 'Tables with Problematic Replicas' in result.output
        assert '0' in result.output  # Zero replica count shown in table
        assert 'shipments' in result.output or 'shipme…' in result.output

    def test_database_error_handling(self):
        """Test handling of database connection errors"""
        self.mock_client.execute_query.side_effect = Exception("Connection failed")
        self.mock_client.test_connection.return_value = True

        with patch('cratedb_xlens.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs'])

        assert result.exit_code == 0
        assert 'Error analyzing problematic translogs' in result.output
        assert 'Connection failed' in result.output

    def test_default_size_mb(self):
        """Test that default sizeMB is 512"""
        self.mock_client.execute_query.return_value = {'rows': []}
        self.mock_client.test_connection.return_value = True

        with patch('cratedb_xlens.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs'])

        assert result.exit_code == 0
        assert '512 MB' in result.output

        # Verify query was called with default value
        call_args = self.mock_client.execute_query.call_args
        parameters = call_args[0][1]
        assert parameters == [512, 512, 512]

    def test_partitioned_and_non_partitioned_replica_queries(self):
        """Test that correct replica queries are used for partitioned vs non-partitioned tables"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['ACME', 'partitioned_table', '("id"=123)', 14, 'data-hot-6', 650.0],
            ['ACME', 'regular_table', None, 5, 'data-hot-1', 600.0]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['ACME', 'partitioned_table', '("id"=123)', 'part123', 1, 650.0, 3, 3, 5.5, 5.2],
            ['ACME', 'regular_table', None, None, 1, 600.0, 2, 4, 3.1, 6.2]
        ]
        table_flush_threshold_data = [
            ['ACME', 'regular_table', 536870912],  # 512 MB default
            ['ACME', 'partitioned_table', 536870912],  # 512 MB default
        ]
        partition_flush_threshold_data = [
            ['ACME', 'partitioned_table', '("id"=123)', 536870912],  # 4 columns for partitions
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': table_flush_threshold_data},    # Table flush threshold query
            {'rows': partition_flush_threshold_data},  # Partition flush threshold query
            {'rows': [[1]]},                   # Partitioned table replica count
            {'rows': [[2]]},                   # Regular table replica count
        ]
        self.mock_client.test_connection.return_value = True

        with patch('cratedb_xlens.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs'])

        assert result.exit_code == 0

        # Verify the replica queries were called correctly
        calls = self.mock_client.execute_query.call_args_list

        # First four calls are individual shards, summary, table flush threshold, and partition flush threshold queries
        assert len(calls) == 6

        # Fifth call should be partitioned table replica query
        partitioned_query = calls[4][0][0]
        assert 'information_schema.table_partitions' in partitioned_query
        assert 'partition_ident' in partitioned_query
        assert calls[4][0][1] == ['ACME', 'partitioned_table', 'part123']

        # Sixth call should be regular table replica query
        regular_query = calls[5][0][0]
        assert 'information_schema.tables' in regular_query
        assert 'partition_ident' not in regular_query
        assert calls[5][0][1] == ['ACME', 'regular_table']
