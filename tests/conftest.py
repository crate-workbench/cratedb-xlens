"""
Pytest configuration for XMover tests

Note: This project uses uv for dependency management.
Run tests with: uv run pytest tests/
Install dev dependencies with: uv sync
"""

import pytest
import os
from unittest.mock import Mock, patch
from xmover.database import CrateDBClient


@pytest.fixture(scope="session", autouse=True)
def setup_test_environment():
    """Setup test environment variables"""
    # Set test environment variables
    os.environ.setdefault('CRATE_CONNECTION_STRING', 'test://localhost:4200')
    yield
    # Cleanup after tests


@pytest.fixture
def mock_crate_client():
    """Create a mock CrateDB client for testing"""
    client = Mock(spec=CrateDBClient)
    client.test_connection.return_value = True
    client.execute.return_value = []
    client.fetchall.return_value = []
    client.connection_string = 'test://localhost:4200'
    return client


@pytest.fixture
def sample_shard_data():
    """Sample shard data for testing"""
    return [
        {
            'schema_name': 'test_schema',
            'table_name': 'test_table',
            'partition_ident': '',
            'shard_id': 0,
            'node_name': 'data-hot-1',
            'state': 'STARTED',
            'primary': True,
            'size': 1073741824,  # 1GB
            'docs': 1000000,
            'zone': 'zone1'
        },
        {
            'schema_name': 'test_schema',
            'table_name': 'test_table',
            'partition_ident': '',
            'shard_id': 0,
            'node_name': 'data-hot-2',
            'state': 'STARTED',
            'primary': False,
            'size': 1073741824,  # 1GB
            'docs': 1000000,
            'zone': 'zone2'
        }
    ]


@pytest.fixture
def sample_translog_data():
    """Sample translog data for testing"""
    return [
        {
            'schema_name': 'test_schema',
            'table_name': 'problematic_table',
            'shard_id': 1,
            'node_name': 'data-hot-1',
            'translog_uncommitted_size_bytes': 524288000,  # 500MB
            'partition_ident': ''
        }
    ]


@pytest.fixture
def sample_recovery_data():
    """Sample recovery data for testing"""
    return [
        {
            'schema_name': 'test_schema',
            'table_name': 'recovering_table',
            'shard_id': 2,
            'source_node': 'data-hot-1',
            'target_node': 'data-hot-2',
            'stage': 'INDEX',
            'bytes_recovered': 500000000,
            'total_bytes': 1000000000,
            'percent': 50.0
        }
    ]


@pytest.fixture
def sample_node_stats():
    """Sample node statistics for testing"""
    return [
        {
            'node_name': 'data-hot-1',
            'zone': 'zone1',
            'disk_used_bytes': 500000000000,  # 500GB
            'disk_total_bytes': 1000000000000,  # 1TB
            'disk_available_bytes': 500000000000,  # 500GB
            'heap_used_bytes': 4000000000,  # 4GB
            'heap_max_bytes': 8000000000   # 8GB
        },
        {
            'node_name': 'data-hot-2',
            'zone': 'zone2',
            'disk_used_bytes': 300000000000,  # 300GB
            'disk_total_bytes': 1000000000000,  # 1TB
            'disk_available_bytes': 700000000000,  # 700GB
            'heap_used_bytes': 2000000000,  # 2GB
            'heap_max_bytes': 8000000000   # 8GB
        }
    ]


@pytest.fixture(autouse=True)
def mock_env_file():
    """Mock the .env file loading"""
    with patch('xmover.database.load_dotenv'):
        yield


@pytest.fixture
def cli_runner():
    """Click CLI test runner"""
    from click.testing import CliRunner
    return CliRunner()