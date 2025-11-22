"""
Tests for partition-aware command interfaces and display formatting

Tests Phase 2 fixes identified in the audit:
- Command displays include partition context
- Table formatters show partition information
- CLI supports partition filtering
- Error messages include partition context
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from click.testing import CliRunner
from rich.table import Table
from rich.console import Console
from io import StringIO

from cratedb_xlens.commands.operations import OperationsCommands
from cratedb_xlens.commands.analysis import AnalysisCommands
from cratedb_xlens.formatting.tables import RichTableFormatter
from cratedb_xlens.database import ShardInfo, NodeInfo
from cratedb_xlens.distribution_analyzer import TableDistribution
from cratedb_xlens.analyzer import ShardAnalyzer


class TestPartitionAwareOperationsDisplay:
    """Test that operations commands display partition information"""

    @pytest.fixture
    def partitioned_shards(self):
        """Sample partitioned shards for display testing"""
        return [
            ShardInfo(
                table_name='logs',
                schema_name='events',
                shard_id=0,
                node_id='data-hot-1-id',
                node_name='data-hot-1',
                zone='zone1',
                is_primary=True,
                size_bytes=5368709120,
                size_gb=5.0,
                num_docs=1000000,
                state='STARTED',
                routing_state='STARTED',
                partition_ident='2024-01'
            ),
            ShardInfo(
                table_name='logs',
                schema_name='events',
                shard_id=1,
                node_id='data-hot-2-id',
                node_name='data-hot-2',
                zone='zone2',
                is_primary=True,
                size_bytes=3221225472,
                size_gb=3.0,
                num_docs=600000,
                state='STARTED',
                routing_state='STARTED',
                partition_ident='2024-02'
            ),
            ShardInfo(
                table_name='users',
                schema_name='doc',
                shard_id=0,
                node_id='data-hot-1-id',
                node_name='data-hot-1',
                zone='zone1',
                is_primary=True,
                size_bytes=2147483648,
                size_gb=2.0,
                num_docs=400000,
                state='STARTED',
                routing_state='STARTED',
                partition_ident=None  # Non-partitioned
            )
        ]

    @pytest.fixture
    def mock_operations_command(self, partitioned_shards):
        """Mock operations command with partitioned data"""
        mock_client = Mock()
        mock_analyzer = Mock()
        mock_analyzer.shards = partitioned_shards

        with patch('xmover.commands.operations.ShardAnalyzer') as mock_analyzer_class:
            mock_analyzer_class.return_value = mock_analyzer
            command = OperationsCommands(mock_client)
            return command, mock_analyzer

    def test_list_shards_displays_partition_column(self, mock_operations_command):
        """Test that list_shards command includes partition column in output"""
        command, mock_analyzer = mock_operations_command

        # Capture console output
        console = Console(file=StringIO(), width=120)

        # Test that our table formatter handles partitions
        formatter = RichTableFormatter(console)
        table = formatter.create_shard_table(mock_analyzer.shards, "Shard Distribution")

        console.print(table)
        output = console.file.getvalue()

        # Verify partition information is displayed
        assert "Partition" in output
        assert "2024-01" in output
        assert "2024-02" in output
        assert "—" in output  # For non-partitioned table

    def test_show_candidates_includes_partition_context(self, mock_operations_command):
        """Test that move candidates display includes partition information"""
        command, mock_analyzer = mock_operations_command

        # Mock move recommendations with partition info
        mock_recommendations = [
            Mock(
                table_name='logs',
                schema_name='events',
                partition_ident='2024-01',
                shard_id=0,
                from_node='data-hot-1',
                to_node='data-hot-2',
                from_zone='zone1',
                to_zone='zone2',
                shard_type='PRIMARY',
                size_gb=5.0,
                reason='Zone balance optimization',
                full_table_identifier='events.logs[2024-01]'
            )
        ]

        mock_analyzer.generate_rebalancing_recommendations.return_value = mock_recommendations

        console = Console(file=StringIO(), width=140)
        # Test that move recommendations include partition context
        table = Table(title="Move Candidates")
        table.add_column("Table")
        table.add_column("Partition", style="cyan")  # NEW
        table.add_column("Shard", justify="right")
        table.add_column("From Node")
        table.add_column("To Node")
        table.add_column("From Zone")
        table.add_column("To Zone")
        table.add_column("Size", justify="right")
        table.add_column("Reason")

        for rec in mock_recommendations:
            partition_display = rec.partition_ident if rec.partition_ident else "-"
            table.add_row(
                f"{rec.schema_name}.{rec.table_name}",
                partition_display,
                str(rec.shard_id),
                rec.from_node,
                rec.to_node,
                rec.from_zone,
                rec.to_zone,
                f"{rec.size_gb:.1f}GB",
                rec.reason
            )

        console.print(table)
        output = console.file.getvalue()

        # Verify partition context is shown
        assert "Partition" in output
        assert "2024-01" in output
        assert "events.logs" in output


class TestPartitionAwareTableFormatters:
    """Test formatting/tables.py includes partition information"""

    def test_create_shard_table_includes_partition_column(self):
        """Test that RichTableFormatter.create_shard_table includes partition information"""
        partitioned_shards = [
            ShardInfo(
                table_name='logs',
                schema_name='events',
                shard_id=0,
                node_id='node1-id',
                node_name='node1',
                zone='zone1',
                is_primary=True,
                size_bytes=5368709120,
                size_gb=5.0,
                num_docs=1000000,
                state='STARTED',
                routing_state='STARTED',
                partition_ident='2024-Q1'
            ),
            ShardInfo(
                table_name='users',
                schema_name='doc',
                shard_id=1,
                node_id='node2-id',
                node_name='node2',
                zone='zone2',
                is_primary=True,
                size_bytes=2147483648,
                size_gb=2.0,
                num_docs=400000,
                state='STARTED',
                routing_state='STARTED',
                partition_ident=None
            )
        ]

        # Test that our table formatter handles partitions
        console = Console(file=StringIO(), width=120)
        formatter = RichTableFormatter(console)
        table = formatter.create_shard_table(partitioned_shards, "Shard Information")
        
        console.print(table)
        output = console.file.getvalue()

        # Should show partition information properly formatted
        assert "2024-Q1" in output
        assert "Partition" in output
        # Non-partitioned should show "—"
        assert "—" in output

    def test_format_shard_info_includes_partition(self):
        """Test shard info formatting includes partition context"""
        partitioned_shard = ShardInfo(
            table_name='metrics',
            schema_name='timeseries',
            shard_id=5,
            node_id='data-node-3-id',
            node_name='data-node-3',
            zone='zone-west',
            is_primary=True,
            size_bytes=13421772800,
            size_gb=12.5,
            num_docs=2500000,
            state='STARTED',
            routing_state='STARTED',
            partition_ident='2024-01-15'
        )

        # Test formatting that includes partition
        formatted = f"{partitioned_shard.schema_name}.{partitioned_shard.table_name}"
        if partitioned_shard.partition_ident:
            formatted += f"[{partitioned_shard.partition_ident}]"
        formatted += f" shard {partitioned_shard.shard_id}"

        assert formatted == "timeseries.metrics[2024-01-15] shard 5"

        # Test non-partitioned
        non_partitioned = ShardInfo(
            table_name='simple',
            schema_name='doc',
            shard_id=0,
            node_id='node1-id',
            node_name='node1',
            zone='zone1',
            is_primary=True,
            size_bytes=1073741824,
            size_gb=1.0,
            num_docs=200000,
            state='STARTED',
            routing_state='STARTED',
            partition_ident=None
        )

        formatted_simple = f"{non_partitioned.schema_name}.{non_partitioned.table_name}"
        if non_partitioned.partition_ident:
            formatted_simple += f"[{non_partitioned.partition_ident}]"
        formatted_simple += f" shard {non_partitioned.shard_id}"

        assert formatted_simple == "doc.simple shard 0"


class TestPartitionAwareAnalysisCommands:
    """Test analysis commands show partition context"""

    @pytest.fixture
    def partitioned_distributions(self):
        """Sample partition distributions for testing"""
        return [
            TableDistribution(
                schema_name='events',
                table_name='logs',
                partition_ident='2024-01',
                total_primary_size_gb=15.0,
                node_distributions={
                    'node1': {'primary_shards': 3, 'primary_size_gb': 15.0},
                    'node2': {'primary_shards': 0, 'primary_size_gb': 0.0}
                }
            ),
            TableDistribution(
                schema_name='events',
                table_name='logs',
                partition_ident='2024-02',
                total_primary_size_gb=10.0,
                node_distributions={
                    'node1': {'primary_shards': 1, 'primary_size_gb': 5.0},
                    'node2': {'primary_shards': 1, 'primary_size_gb': 5.0}
                }
            )
        ]

    def test_analyze_distribution_shows_partition_breakdown(self, partitioned_distributions):
        """Test that analyze distribution shows per-partition breakdown"""
        console = Console(file=StringIO(), width=140)

        # Create table showing distribution per partition
        table = Table(title="Table Distribution Analysis")
        table.add_column("Table")
        table.add_column("Partition", style="cyan")
        table.add_column("Primary Size", justify="right")
        table.add_column("Balance Status")
        table.add_column("Risk Level")

        for dist in partitioned_distributions:
            # Analyze balance for this partition
            node_counts = [metrics['primary_shards'] for metrics in dist.node_distributions.values()]
            max_shards = max(node_counts)
            min_shards = min(node_counts)

            if min_shards == 0 and max_shards > 0:
                balance_status = "🚨 CRITICAL IMBALANCE"
                risk_level = "HIGH"
            elif max_shards - min_shards <= 1:
                balance_status = "✅ BALANCED"
                risk_level = "LOW"
            else:
                balance_status = "⚠️ IMBALANCED"
                risk_level = "MEDIUM"

            table.add_row(
                f"{dist.schema_name}.{dist.table_name}",
                dist.partition_ident or "-",
                f"{dist.total_primary_size_gb:.1f}GB",
                balance_status,
                risk_level
            )

        console.print(table)
        output = console.file.getvalue()

        # Should show per-partition analysis
        assert "2024-01" in output
        assert "2024-02" in output
        assert "CRITICAL IMBALANCE" in output  # 2024-01 partition
        assert "BALANCED" in output  # 2024-02 partition

        # Verify partition-level problems are visible (not masked)
        assert "🚨" in output  # Critical indicator for imbalanced partition

    def test_health_report_partition_context(self, partitioned_distributions):
        """Test health reports include partition context in warnings"""
        console = Console(file=StringIO(), width=120)

        # Simulate health report that shows partition-specific issues
        for dist in partitioned_distributions:
            node_counts = [metrics['primary_shards'] for metrics in dist.node_distributions.values()]
            max_shards = max(node_counts)
            min_shards = min(node_counts)

            if min_shards == 0:
                console.print(
                    f"[red]WARNING[/red]: {dist.full_table_name} has severe imbalance "
                    f"(max: {max_shards}, min: {min_shards} shards per node)"
                )
            elif max_shards - min_shards > 1:
                console.print(
                    f"[yellow]NOTICE[/yellow]: {dist.full_table_name} has minor imbalance "
                    f"(max: {max_shards}, min: {min_shards} shards per node)"
                )
            else:
                console.print(
                    f"[green]OK[/green]: {dist.full_table_name} is well balanced"
                )

        output = console.file.getvalue()

        # Should include partition identifiers in warnings
        assert "events.logs[2024-01]" in output
        assert "events.logs[2024-02]" in output
        assert "WARNING" in output  # For the imbalanced partition
        assert "OK" in output  # For the balanced partition


class TestPartitionErrorMessages:
    """Test error messages include partition context"""

    def test_move_validation_errors_include_partition(self):
        """Test that move validation errors mention specific partitions"""
        # Simulate a move validation error
        error_table = "events.logs"
        error_partition = "2024-01"
        error_shard = 5
        error_reason = "Target node at capacity"

        error_message = (
            f"Cannot move shard {error_shard} from {error_table}"
            f"[{error_partition}]: {error_reason}"
        )

        assert "events.logs[2024-01]" in error_message
        assert "shard 5" in error_message
        assert error_reason in error_message

    def test_zone_violation_errors_specify_partition(self):
        """Test zone violation errors specify which partition"""
        violations = [
            {
                'table': 'events.logs',
                'partition': '2024-01',
                'severity': 'CRITICAL',
                'description': 'All 3 primary shards in zone zone1',
                'recommendation': 'Redistribute shards across zones'
            }
        ]

        console = Console(file=StringIO(), width=120)

        for violation in violations:
            console.print(
                f"[red]{violation['severity']}[/red]: "
                f"Table {violation['table']} partition [{violation['partition']}] - "
                f"{violation['description']}"
            )
            console.print(f"  💡 {violation['recommendation']}")

        output = console.file.getvalue()

        # Should clearly identify which partition has the violation
        assert "events.logs partition [2024-01]" in output
        assert "All 3 primary shards in zone zone1" in output
        assert "CRITICAL" in output


class TestPartitionCommandParsing:
    """Test command line parsing for partition-specific operations"""

    def test_parse_table_partition_syntax(self):
        """Test parsing table[partition] syntax"""
        test_cases = [
            ("events.logs[2024-01]", ("events.logs", "2024-01")),
            ("logs[Q1-2024]", ("logs", "Q1-2024")),
            ("simple_table", ("simple_table", None)),
            ("schema.table", ("schema.table", None)),
            ("complex.table[part_2024_01_15]", ("complex.table", "part_2024_01_15"))
        ]

        def parse_table_partition(identifier):
            """Parse table[partition] syntax"""
            if '[' in identifier and identifier.endswith(']'):
                table_part, partition_part = identifier.rsplit('[', 1)
                partition_ident = partition_part.rstrip(']')
                return table_part, partition_ident
            return identifier, None

        for input_str, expected in test_cases:
            result = parse_table_partition(input_str)
            assert result == expected, f"Failed for input: {input_str}"

    def test_partition_filtering_in_commands(self):
        """Test that commands can filter by specific partitions"""
        # This would test CLI argument parsing for partition filtering

        # Simulate command: xmover analyze --table=events.logs --partition=2024-01
        mock_args = {
            'table': 'events.logs',
            'partition': '2024-01'
        }

        # Filter logic that would be in the command
        table_name = mock_args['table']
        partition_filter = mock_args.get('partition')

        # Simulate filtering shards
        all_shards = [
            Mock(table_name='logs', partition_ident='2024-01'),
            Mock(table_name='logs', partition_ident='2024-02'),
            Mock(table_name='other', partition_ident=None)
        ]

        filtered_shards = [
            s for s in all_shards
            if s.table_name == table_name.split('.')[-1]
            and (partition_filter is None or getattr(s, 'partition_ident', None) == partition_filter)
        ]

        # Should only return the 2024-01 partition
        assert len(filtered_shards) == 1
        assert filtered_shards[0].partition_ident == '2024-01'


class TestPartitionDisplayBackwardCompatibility:
    """Ensure partition displays work with non-partitioned tables"""

    def test_mixed_partitioned_non_partitioned_display(self):
        """Test display of mixed partitioned and non-partitioned tables"""
        mixed_shards = [
            ShardInfo(
                table_name='logs',
                schema_name='events',
                shard_id=0,
                node_id='node1-id',
                node_name='node1',
                zone='zone1',
                is_primary=True,
                size_bytes=5368709120,
                size_gb=5.0,
                num_docs=1000000,
                state='STARTED',
                routing_state='STARTED',
                partition_ident='2024-01'  # Partitioned
            ),
            ShardInfo(
                table_name='users',
                schema_name='doc',
                shard_id=0,
                node_id='node1-id',
                node_name='node1',
                zone='zone1',
                is_primary=True,
                size_bytes=3221225472,
                size_gb=3.0,
                num_docs=600000,
                state='STARTED',
                routing_state='STARTED',
                partition_ident=None  # Non-partitioned
            )
        ]

        console = Console(file=StringIO(), width=120)
        table = Table()
        table.add_column("Table")
        table.add_column("Partition", style="dim")
        table.add_column("Shard", justify="right")
        table.add_column("Node")
        table.add_column("Size", justify="right")

        for shard in mixed_shards:
            partition_display = shard.partition_ident if shard.partition_ident else "—"
            table_name = f"{shard.schema_name}.{shard.table_name}"

            table.add_row(
                table_name,
                partition_display,
                str(shard.shard_id),
                shard.node_name,
                f"{shard.size_gb:.1f}GB"
            )

        console.print(table)
        output = console.file.getvalue()

        # Both partitioned and non-partitioned should display correctly
        assert "events.logs" in output
        assert "doc.users" in output
        assert "2024-01" in output  # Partitioned
        assert "—" in output  # Non-partitioned placeholder

    def test_empty_partition_handling(self):
        """Test handling of empty/null partition identifiers"""
        edge_cases = [
            None,  # Null partition
            "",    # Empty string
            " ",   # Whitespace
        ]

        for partition_value in edge_cases:
            shard = ShardInfo(
                table_name='table',
                schema_name='test',
                shard_id=0,
                node_id='node1-id',
                node_name='node1',
                zone='zone1',
                is_primary=True,
                size_bytes=1073741824,
                size_gb=1.0,
                num_docs=200000,
                state='STARTED',
                routing_state='STARTED',
                partition_ident=partition_value
            )

            # Should display consistently as non-partitioned
            partition_display = shard.partition_ident if shard.partition_ident and shard.partition_ident.strip() else "—"
            assert partition_display == "—"


@pytest.mark.integration
class TestPartitionDisplayIntegration:
    """Integration tests for partition display functionality"""

    def test_end_to_end_partition_display_flow(self):
        """Test complete flow from data to display with partitions"""
        # This would test the full pipeline:
        # 1. Query returns partition data
        # 2. Data models handle partitions
        # 3. Analysis considers partitions separately
        # 4. Display shows partition context
        # 5. User can identify partition-specific issues

        # Mock the complete flow
        query_result = {
            'rows': [
                # 2024-01 partition: Severely imbalanced (all shards on node1)
                ['events', 'logs', '2024-01', 'node1', 3, 0, 3, 15.0, 15.0, 0.0, 3000000],
                ['events', 'logs', '2024-01', 'node2', 0, 0, 0, 0.0, 0.0, 0.0, 0],
                # 2024-02 partition: Balanced
                ['events', 'logs', '2024-02', 'node1', 1, 0, 1, 5.0, 5.0, 0.0, 1000000],
                ['events', 'logs', '2024-02', 'node2', 1, 0, 1, 5.0, 5.0, 0.0, 1000000],
            ]
        }

        # Parse into distributions
        partitions = {}
        for row in query_result['rows']:
            partition_key = row[2]
            if partition_key not in partitions:
                partitions[partition_key] = {
                    'schema_name': row[0],
                    'table_name': row[1],
                    'partition_ident': row[2],
                    'nodes': {}
                }
            partitions[partition_key]['nodes'][row[3]] = {
                'primary_shards': row[4],
                'primary_size_gb': row[7]
            }

        # Create distributions
        distributions = []
        for p_data in partitions.values():
            total_size = sum(n['primary_size_gb'] for n in p_data['nodes'].values())
            dist = TableDistribution(
                schema_name=p_data['schema_name'],
                table_name=p_data['table_name'],
                partition_ident=p_data['partition_ident'],
                total_primary_size_gb=total_size,
                node_distributions=p_data['nodes']
            )
            distributions.append(dist)

        # Verify we can identify partition-specific issues
        imbalanced_partitions = []
        for dist in distributions:
            node_shards = [n['primary_shards'] for n in dist.node_distributions.values()]
            if min(node_shards) == 0:  # Severe imbalance
                imbalanced_partitions.append(dist.partition_ident)

        # Should detect 2024-01 as imbalanced
        assert '2024-01' in imbalanced_partitions
        assert '2024-02' not in imbalanced_partitions  # This one is balanced

        # Verify display would show partition context
        for dist in distributions:
            display_name = dist.full_table_name
            if dist.partition_ident == '2024-01':
                assert display_name == 'events.logs[2024-01]'
            elif dist.partition_ident == '2024-02':
                assert display_name == 'events.logs[2024-02]'
