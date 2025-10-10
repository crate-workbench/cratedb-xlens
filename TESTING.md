# Testing Guide for XMover

This document explains how to run tests for the XMover CrateDB shard management tool.

## Prerequisites

XMover uses [uv](https://docs.astral.sh/uv/) for dependency management. Make sure you have:

1. **uv installed**: Follow the [uv installation guide](https://docs.astral.sh/uv/getting-started/installation/)
2. **Dependencies synced**: Run `uv sync` to install all dependencies including dev dependencies

## Quick Start

### Run All Tests
```bash
uv run python run_tests.py all
```

### Run Specific Test Categories
```bash
# CLI command tests
uv run python run_tests.py cli

# Enhanced feature tests  
uv run python run_tests.py enhanced

# Existing component tests
uv run python run_tests.py existing

# Quick test run (stop on first failure)
uv run python run_tests.py quick
```

### Run Individual Test Files
```bash
# Command validation tests
uv run pytest tests/test_command_validation.py -v

# CLI command tests
uv run pytest tests/test_cli_commands.py -v

# Enhanced command tests
uv run pytest tests/test_enhanced_commands.py -v
```

### Run Specific Tests
```bash
# Run tests matching a pattern
uv run pytest tests/ -k "test_analyze" -v

# Run a specific test method
uv run pytest tests/test_cli_commands.py::TestAnalyzeCommand::test_analyze_basic -v
```

## Test Coverage

### Generate Coverage Report
```bash
uv run python run_tests.py coverage
```

### Manual Coverage Analysis
```bash
uv add --dev pytest-cov
uv run pytest tests/ --cov=src/xmover --cov-report=term-missing --cov-report=html
```

## Test Structure

### Main Test Files

- **`test_cli_commands.py`** - Comprehensive CLI tests for all subcommands
- **`test_enhanced_commands.py`** - Tests for enhanced features and edge cases  
- **`test_command_validation.py`** - Simple validation that commands parse correctly
- **`conftest.py`** - Shared test fixtures and configuration

### Tested Subcommands

The test suite covers all key XMover subcommands:

- `xmover analyze` - Shard distribution analysis
- `xmover test-connection` - Database connectivity testing  
- `xmover monitor-recovery` - Recovery monitoring with `--include-transitioning --watch`
- `xmover problematic-translogs` - Problematic translog detection with `--sizeMB 520`
- `xmover deep-analyze` - Deep analysis with custom rules
- `xmover large-translogs` - Large translog monitoring
- `xmover shard-distribution` - Distribution anomaly analysis
- `xmover zone-analysis` - Zone distribution analysis

## Test Philosophy

These tests follow the "simple, robust, on-point" approach requested:

1. **Mock external dependencies** - All database connections are mocked
2. **Test argument parsing** - Verify commands accept correct options
3. **Test basic functionality flow** - Ensure commands execute without crashing
4. **Test error conditions** - Verify graceful handling of failures
5. **Keep it minimal** - Focus on core functionality verification

## Running Tests in Development

### Watch Mode (requires pytest-watch)
```bash
uv add --dev pytest-watch
uv run ptw tests/ -- -v
```

### Debug Failed Tests
```bash
# Run with more verbose output
uv run pytest tests/test_cli_commands.py -vvv --tb=long

# Run with pdb on failure
uv run pytest tests/test_cli_commands.py --pdb

# Run only failed tests from last run
uv run pytest --lf -v
```

## Test Environment

### Environment Variables
Tests automatically set up a mock environment:
- `CRATE_CONNECTION_STRING=test://localhost:4200`

### Mock Strategy
All tests use comprehensive mocking:
- Database connections are mocked to avoid requiring a real CrateDB instance
- External dependencies are patched to return predictable test data
- CLI interactions are simulated using Click's test runner

## Continuous Integration

For CI/CD pipelines, use:
```bash
# Install dependencies
uv sync

# Run tests with junit output
uv run pytest tests/ --junitxml=test-results.xml

# Generate coverage for CI
uv run pytest tests/ --cov=src/xmover --cov-report=xml
```

## Troubleshooting

### Common Issues

1. **Import errors**: Run `uv sync` to ensure all dependencies are installed
2. **Test failures**: Check that you're in the project root directory
3. **Missing uv**: Install uv from https://docs.astral.sh/uv/

### Getting Help

- Run `uv run pytest --help` for pytest options
- Run `uv run python run_tests.py` without arguments for usage info
- Check individual test files for specific test documentation

## Contributing New Tests

When adding new functionality:

1. Add tests to the appropriate test file (`test_cli_commands.py` for basic functionality)
2. Mock all external dependencies
3. Test both success and error conditions  
4. Keep tests simple and focused
5. Run the full test suite to ensure no regressions

Example test structure:
```python
def test_new_command(self, runner, mock_successful_connection):
    """Test new command functionality"""
    with patch('xmover.cli.SomeAnalyzer') as mock_analyzer:
        mock_instance = Mock()
        mock_instance.some_method.return_value = expected_data
        mock_analyzer.return_value = mock_instance
        
        result = runner.invoke(main, ['new-command', '--option', 'value'])
        assert result.exit_code == 0
        mock_instance.some_method.assert_called_once()
```
