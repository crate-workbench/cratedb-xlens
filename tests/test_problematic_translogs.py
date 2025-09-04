"""
Tests for problematic translogs functionality with replica management
"""

import pytest
from unittest.mock import Mock, patch
from click.testing import CliRunner
from xmover.cli import main, problematic_translogs
from xmover.database import CrateDBClient


class TestProblematicTranslogs:

    def setup_method(self):
        """Set up test fixtures"""
        self.runner = CliRunner()
        self.mock_client = Mock(spec=CrateDBClient)

    def test_no_problematic_tables(self):
        """Test when no tables meet the criteria"""
        self.mock_client.execute_query.return_value = {'rows': []}
        self.mock_client.test_connection.return_value = True

        with patch('xmover.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs', '--sizeMB', '300'])

        assert result.exit_code == 0
        assert 'No tables found with replica shards having translog uncommitted size > 300MB' in result.output

    def test_non_partitioned_table_command_generation(self):
        """Test ALTER command generation for non-partitioned tables"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['TURVO', 'shipmentFormFieldData', None, 14, 'data-hot-6', 7011.8],
            ['TURVO', 'orderFormFieldData', 'NULL', 5, 'data-hot-1', 469.5]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['TURVO', 'shipmentFormFieldData', None, None, 3, 7011.8, 5, 5, 12.4, 12.1],
            ['TURVO', 'orderFormFieldData', 'NULL', None, 1, 469.5, 3, 6, 8.2, 16.3]
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': [[1]]},                   # Replica count for shipmentFormFieldData
            {'rows': [[2]]},                   # Replica count for orderFormFieldData
        ]
        self.mock_client.test_connection.return_value = True

        with patch('xmover.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs', '--sizeMB', '300'])

        assert result.exit_code == 0
        assert 'Found 2 table/partition(s) with problematic translogs' in result.output
        assert 'Tables with Problematic Replicas' in result.output
        assert 'Generated ALTER Commands:' in result.output

        # Check that replica management commands are present
        assert 'SET ("number_of_replicas" = 0)' in result.output
        assert 'SET ("number_of_replicas" = 1)' in result.output
        assert 'SET ("number_of_replicas" = 2)' in result.output
        assert 'ALTER TABLE "TURVO"."shipmentFormFieldData"' in result.output
        assert 'ALTER TABLE "TURVO"."orderFormFieldData"' in result.output

    def test_partitioned_table_command_generation(self):
        """Test ALTER command generation for partitioned tables"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['TURVO', 'shipmentFormFieldData_events', '("sync_day"=1757376000000)', 3, 'data-hot-2', 481.2],
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['TURVO', 'shipmentFormFieldData_events', '("sync_day"=1757376000000)', 'partition123', 2, 481.2, 2, 2, 1.1, 1.0],
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': [[1]]},                   # Replica count for partitioned table
        ]
        self.mock_client.test_connection.return_value = True

        with patch('xmover.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs', '--sizeMB', '400'])

        assert result.exit_code == 0
        assert 'Found 1 table/partition(s) with problematic translogs' in result.output
        assert 'Generated ALTER Commands:' in result.output

        # Check that partitioned table commands are present (handle Rich line wrapping)
        assert 'ALTER TABLE "TURVO"."shipmentFormFieldData_events"' in result.output
        assert 'PARTITION' in result.output
        assert '("sync_day"=1757376000000)' in result.output
        assert 'SET ("number_of_replicas" = 0)' in result.output
        assert 'SET ("number_of_replicas" = 1)' in result.output

    def test_mixed_partitioned_non_partitioned(self):
        """Test handling of both partitioned and non-partitioned tables"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['TURVO', 'shipmentFormFieldData', None, 14, 'data-hot-6', 7011.8],
            ['TURVO', 'shipmentFormFieldData_events', '("sync_day"=1757376000000)', 3, 'data-hot-2', 481.2],
            ['TURVO', 'orderFormFieldData', 'NULL', 5, 'data-hot-1', 469.5]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['TURVO', 'shipmentFormFieldData', None, None, 2, 7011.8, 5, 5, 12.4, 12.1],
            ['TURVO', 'shipmentFormFieldData_events', '("sync_day"=1757376000000)', 'partition123', 1, 481.2, 2, 2, 1.1, 1.0],
            ['TURVO', 'orderFormFieldData', 'NULL', None, 1, 469.5, 3, 6, 8.2, 16.3]
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': [[2]]},                   # Replica count for shipmentFormFieldData
            {'rows': [[1]]},                   # Replica count for partitioned table
            {'rows': [[3]]},                   # Replica count for orderFormFieldData
        ]
        self.mock_client.test_connection.return_value = True

        with patch('xmover.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs', '--sizeMB', '200'])

        assert result.exit_code == 0
        assert 'Found 3 table/partition(s) with problematic translogs' in result.output

        # Check non-partitioned commands
        assert 'ALTER TABLE "TURVO"."shipmentFormFieldData" SET ("number_of_replicas" = 0)' in result.output
        assert 'ALTER TABLE "TURVO"."shipmentFormFieldData" SET ("number_of_replicas" = 2)' in result.output

        # Check partitioned commands (handle Rich line wrapping)
        assert 'ALTER TABLE "TURVO"."shipmentFormFieldData_events"' in result.output
        assert 'PARTITION' in result.output
        assert '("sync_day"=1757376000000)' in result.output
        # Check that both 0 and 1 replica settings are present for partitioned table
        assert 'SET ("number_of_replicas" = 0)' in result.output
        assert 'SET ("number_of_replicas" = 1)' in result.output

        # Check NULL partition handled as non-partitioned
        assert 'ALTER TABLE "TURVO"."orderFormFieldData" SET ("number_of_replicas" = 0)' in result.output
        assert 'ALTER TABLE "TURVO"."orderFormFieldData" SET ("number_of_replicas" = 3)' in result.output

    def test_query_parameters(self):
        """Test that the query is called with correct parameters"""
        self.mock_client.execute_query.return_value = {'rows': []}
        self.mock_client.test_connection.return_value = True

        with patch('xmover.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs', '--sizeMB', '500'])

        # Verify the query was called twice (individual shards + summary)
        assert self.mock_client.execute_query.call_count == 2
        call_args = self.mock_client.execute_query.call_args
        query = call_args[0][0]
        parameters = call_args[0][1]

        assert 'sh.translog_stats[\'uncommitted_size\'] > ? * 1024^2' in query
        assert 'primary=FALSE' in query
        assert 'GROUP BY' in query
        assert 'max_translog_uncommitted_mb DESC' in query
        assert parameters == [500, 500, 500]

    def test_execute_flag_user_confirmation_no(self):
        """Test --execute flag with user declining confirmation"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['TURVO', 'shipmentFormFieldData', None, 14, 'data-hot-6', 7011.8]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['TURVO', 'shipmentFormFieldData', None, None, 1, 7011.8, 5, 5, 12.4, 12.1]
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': [[1]]},                   # Replica count query
        ]
        self.mock_client.test_connection.return_value = True

        with patch('xmover.cli.CrateDBClient', return_value=self.mock_client), \
             patch('click.confirm', return_value=False):
            result = self.runner.invoke(main, ['problematic-translogs', '--execute'])

        assert result.exit_code == 0
        assert 'Operation cancelled by user' in result.output
        # Should be called 3 times: individual shards query, summary query, replica count query
        assert self.mock_client.execute_query.call_count == 3

    def test_execute_flag_user_confirmation_set_zero_only(self):
        """Test --execute flag with user confirming reroute but skipping replica commands"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['TURVO', 'shipmentFormFieldData', None, 14, 'data-hot-6', 7011.8]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['TURVO', 'shipmentFormFieldData', None, None, 1, 7011.8, 5, 5, 12.4, 12.1]
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': [[1]]},                   # Replica count query
            None,                              # REROUTE CANCEL execution
        ]
        self.mock_client.test_connection.return_value = True

        # Confirm overall execution, confirm REROUTE CANCEL, skip SET to 0
        with patch('xmover.cli.CrateDBClient', return_value=self.mock_client), \
             patch('click.confirm', side_effect=[True, True, False]):
            result = self.runner.invoke(main, ['problematic-translogs', '--execute'])

        assert result.exit_code == 0
        assert 'Executing commands individually' in result.output
        assert 'executed successfully' in result.output
        assert 'skipped' in result.output

        # Should be called 4 times: individual query, summary query, replica count, reroute execution
        assert self.mock_client.execute_query.call_count == 4

    def test_execute_flag_user_confirmation_both_steps(self):
        """Test --execute flag with user confirming all commands"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['TURVO', 'shipmentFormFieldData', None, 14, 'data-hot-6', 7011.8]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['TURVO', 'shipmentFormFieldData', None, None, 1, 7011.8, 5, 5, 12.4, 12.1]
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': [[1]]},                   # Replica count query
            None,                              # REROUTE CANCEL execution
            None,                              # SET to 0 execution
            None,                              # RESTORE execution
        ]
        self.mock_client.test_connection.return_value = True

        # Confirm overall execution, confirm REROUTE CANCEL, confirm SET to 0, confirm RESTORE
        with patch('xmover.cli.CrateDBClient', return_value=self.mock_client), \
             patch('click.confirm', side_effect=[True, True, True, True]):
            result = self.runner.invoke(main, ['problematic-translogs', '--execute'])

        assert result.exit_code == 0
        assert 'Executing commands individually' in result.output
        assert 'executed successfully' in result.output
        assert 'Execution Summary:' in result.output
        assert 'Successful: 3' in result.output

        # Should be called 6 times: individual query, summary query, replica count, reroute, set to 0, restore
        assert self.mock_client.execute_query.call_count == 6

    def test_execution_failure_handling(self):
        """Test handling of command execution failures"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['TURVO', 'shipmentFormFieldData', None, 14, 'data-hot-6', 7011.8]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['TURVO', 'shipmentFormFieldData', None, None, 1, 7011.8, 5, 5, 12.4, 12.1]
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': [[1]]},                   # Replica count query
            Exception("REROUTE failed"),       # REROUTE CANCEL execution fails
        ]
        self.mock_client.test_connection.return_value = True

        # Confirm overall execution, confirm REROUTE CANCEL (which fails), then decline next command
        with patch('xmover.cli.CrateDBClient', return_value=self.mock_client), \
             patch('click.confirm', side_effect=[True, True, False]):
            result = self.runner.invoke(main, ['problematic-translogs', '--execute'])

        assert result.exit_code == 0
        assert 'failed' in result.output
        assert 'REROUTE failed' in result.output
        assert 'Failed: 1' in result.output

    def test_skip_tables_with_unknown_replicas(self):
        """Test skipping tables with unknown replica counts"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['TURVO', 'shipmentFormFieldData', None, 14, 'data-hot-6', 7011.8]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['TURVO', 'shipmentFormFieldData', None, None, 1, 7011.8, 5, 5, 12.4, 12.1]
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            Exception("Cannot get replica count"),  # Replica count query fails
        ]
        self.mock_client.test_connection.return_value = True

        with patch('xmover.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs'])

        assert result.exit_code == 0
        assert 'Warning: Could not retrieve replica count' in result.output
        assert 'Skipping' in result.output
        assert 'unknown replica count' in result.output
        assert 'REROUTE CANCEL commands' in result.output
        assert '1 REROUTE CANCEL commands + 0 replica management commands' in result.output

    def test_skip_tables_with_zero_replicas(self):
        """Test skipping tables that already have 0 replicas"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['TURVO', 'shipmentFormFieldData', None, 14, 'data-hot-6', 7011.8]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)
        summary_data = [
            ['TURVO', 'shipmentFormFieldData', None, None, 1, 7011.8, 5, 5, 12.4, 12.1]
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': [[0]]},                   # Replica count query returns 0
        ]
        self.mock_client.test_connection.return_value = True

        with patch('xmover.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs'])

        assert result.exit_code == 0
        assert 'Skipping' in result.output
        assert 'already has 0 replicas' in result.output
        assert 'REROUTE CANCEL commands' in result.output
        assert '1 REROUTE CANCEL commands + 0 replica management commands' in result.output

    def test_database_error_handling(self):
        """Test handling of database connection errors"""
        self.mock_client.execute_query.side_effect = Exception("Connection failed")
        self.mock_client.test_connection.return_value = True

        with patch('xmover.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs'])

        assert result.exit_code == 0
        assert 'Error analyzing problematic translogs' in result.output
        assert 'Connection failed' in result.output

    def test_default_size_mb(self):
        """Test that default sizeMB is 300"""
        self.mock_client.execute_query.return_value = {'rows': []}
        self.mock_client.test_connection.return_value = True

        with patch('xmover.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs'])

        assert result.exit_code == 0
        assert '300MB' in result.output

        # Verify query was called with default value
        call_args = self.mock_client.execute_query.call_args
        parameters = call_args[0][1]
        assert parameters == [300, 300, 300]

    def test_partitioned_and_non_partitioned_replica_queries(self):
        """Test that correct replica queries are used for partitioned vs non-partitioned tables"""
        # Individual shards data (6 columns)
        individual_shards_data = [
            ['TURVO', 'partitioned_table', '("id"=123)', 14, 'data-hot-6', 500.0],
            ['TURVO', 'regular_table', None, 5, 'data-hot-1', 400.0]
        ]
        # Summary data (10 columns from query, displayed as 8 by combining P/R columns)  
        summary_data = [
            ['TURVO', 'partitioned_table', '("id"=123)', 'part123', 1, 500.0, 3, 3, 5.5, 5.2],
            ['TURVO', 'regular_table', None, None, 1, 400.0, 2, 4, 3.1, 6.2]
        ]
        self.mock_client.execute_query.side_effect = [
            {'rows': individual_shards_data},  # Individual shards query
            {'rows': summary_data},            # Summary query
            {'rows': [[1]]},                   # Partitioned table replica count
            {'rows': [[2]]},                   # Regular table replica count
        ]
        self.mock_client.test_connection.return_value = True

        with patch('xmover.cli.CrateDBClient', return_value=self.mock_client):
            result = self.runner.invoke(main, ['problematic-translogs'])

        assert result.exit_code == 0

        # Verify the replica queries were called correctly
        calls = self.mock_client.execute_query.call_args_list

        # First two calls are the individual shards and summary queries
        assert len(calls) == 4

        # Third call should be partitioned table replica query
        partitioned_query = calls[2][0][0]
        assert 'information_schema.table_partitions' in partitioned_query
        assert 'partition_ident' in partitioned_query
        assert calls[2][0][1] == ['partitioned_table', 'TURVO', 'part123']

        # Fourth call should be regular table replica query
        regular_query = calls[3][0][0]
        assert 'information_schema.tables' in regular_query
        assert 'partition_ident' not in regular_query
        assert calls[3][0][1] == ['regular_table', 'TURVO']
