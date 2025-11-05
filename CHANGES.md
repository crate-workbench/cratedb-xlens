# XMover Changelog

All notable changes to XMover will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **AutoExec functionality for `problematic-translogs`**: Automated replica reset operations
  - New `--autoexec` flag: Automatically executes replica reset operations without manual intervention
  - New `--dry-run` flag: Safe simulation mode that shows what would be executed without making changes
  - New `--percentage` parameter: Filter tables by threshold percentage (default: 200%)
  - New `--max-wait` parameter: Configure timeout for retention lease monitoring (default: 720s)
  - New `--log-format` parameter: JSON logging support for container/Kubernetes environments
  - Robust state machine implementation: Handles set replicas → monitor leases → restore replicas workflow
  - Intelligent retry logic: Exponential backoff with configurable timeouts
  - Comprehensive error handling: Automatic rollback on failures with manual intervention guidance
  - Production-ready: Designed for Kubernetes CronJob automation

- **New `read-check` command**: Professional cluster data readability monitor
  - Continuously monitors the 5 largest tables/partitions using max(\_seq_no) to detect data changes
  - Health status indicators: 🟢 Active, 🟡 Slow, 🔴 Stale tables
  - Query performance tracking with ⚡ alerts for >1000ms queries
  - Fresh connections with exponential backoff retry logic
  - Enhanced statistics with per-table metrics on exit (CTRL+C)
  - Automatic table discovery every 10 minutes with partition support
  - Professional logging format: `timestamp: schema.table // _seq_no ±X // total_docs ±Y // XXXms`
  - Anomaly detection with ⚠️ indicators for unusual activity patterns
  - Optimized `max(_seq_no)` queries instead of expensive ORDER BY + LIMIT operations
  - Removed obsolete `--limit` parameter (no longer needed with max() aggregation)
  - Uses CrateDB's actual query execution time instead of network RTT for performance metrics

### Fixed

- **Critical CrateDB response handling bug**: Fixed AutoExec operations incorrectly reported as failures
  - Issue: Successful ALTER TABLE operations were marked as "Failed: Unknown error"
  - Root cause: Code checked for 'success' field that CrateDB doesn't return in HTTP responses
  - Fix: Changed to check for absence of 'error' field instead of presence of 'success' field
  - Impact: AutoExec operations now correctly report success/failure status
  - Evidence: Operations were actually succeeding (retention leases changed) but reported as failed

- **Hardcoded threshold bug in AutoExec filtering**: Fixed percentage calculations using wrong thresholds
  - Issue: AutoExec percentage filtering used hardcoded 563MB for all tables instead of adaptive thresholds
  - Fix: Modified `_filter_tables_by_percentage()` to use actual table-specific adaptive thresholds
  - Impact: AutoExec now respects individual table configurations instead of using one-size-fits-all approach

### Changed

- **Enhanced `problematic-translogs` command**: Adaptive threshold detection based on table settings
  - Default `--sizeMB` changed from 300MB to 512MB (CrateDB default flush threshold)
  - Adaptive thresholds: Uses table-specific `flush_threshold_size * 1.1` for intelligent detection
  - Performance optimized: Only queries table settings for tables with initially problematic shards
  - Enhanced display: Shows both configured value and calculated threshold (e.g., "2048MB/2253MB config/threshold")
  - Partition support: Handles partition-specific flush_threshold_size settings
  - Clean CLI: Simplified help text for better usability
  - Fixed REROUTE CANCEL commands to include partition information for partitioned tables

- **Enhanced SQL logging**: Complete transparency for AutoExec operations
  - Dry-run mode: Shows "DRY RUN: Would execute: SQL" for all operations
  - Regular mode: Shows "Executing: SQL" before actual database execution
  - JSON logging: Uses loguru with structured data (consistent with read-check command)
  - Rollback operations: Clear logging for failure recovery attempts
  - Benefit: Full audit trail and debugging visibility for all database operations

- **Consistent loguru usage**: Both `read-check` and `problematic-translogs --autoexec` use loguru for structured logging
- **Enhanced per-table statistics**: Shows document change tracking and performance metrics
  - Document changes: Total change with min/avg/max deltas
  - Performance: Query response time min/avg/max
  - Anomaly counter per table
- **Query optimization**: `read-check` uses efficient `max(_seq_no)` aggregation instead of sorting
- **Parameter cleanup**: Removed obsolete `--limit` parameter from `read-check` command
- **Performance measurement**: `read-check` now uses database execution time from CrateDB response instead of measuring network round-trip time

### Dependencies

- Enhanced `loguru>=0.7.0` usage: AutoExec now uses loguru for JSON logging (consistent with read-check command)

### Testing

- **Comprehensive AutoExec test coverage**: 44 new tests across 3 test modules
  - `test_autoexec_functionality.py`: 37 unit tests for state machine, dry-run safety, and core logic
  - `test_autoexec_integration.py`: Integration tests for real-world scenarios
  - `test_autoexec_cli.py`: CLI parameter validation and usage tests
  - `test_adaptive_thresholds.py`: 7 tests specifically verifying information_schema threshold lookup
  - Coverage includes: State transitions, error handling, retry logic, SQL generation, percentage filtering

### Documentation

- Updated main README.md with `read-check` command reference and AutoExec functionality
- Added comprehensive `docs/read-check.md` with usage guide and examples
- Updated `docs/README.md` with data readability monitoring section
- Added `SQL_LOGGING_AND_BUGFIX_SUMMARY.md`: Technical documentation of bug fixes and enhancements
- Created verification scripts: `verify_dry_run_safety.py` and `verify_adaptive_thresholds.py`

---

## Format Notes

### Version Format

- **[Unreleased]**: Features ready but not yet in a tagged release
- **[X.Y.Z]**: Released versions with date

### Change Categories

- **Added**: New features
- **Changed**: Changes to existing functionality
- **Deprecated**: Soon-to-be removed features
- **Removed**: Removed features
- **Fixed**: Bug fixes
- **Security**: Vulnerability fixes

### Commit Convention

This project will follow conventional commits for automatic changelog generation:

- `feat:` for new features (minor version bump)
- `fix:` for bug fixes (patch version bump)
- `docs:` for documentation changes
- `refactor:` for code refactoring
- `test:` for adding tests
- `chore:` for maintenance tasks

### Example Future Entry

```
## [1.2.0] - 2024-02-15

### Added
- New feature X with capability Y

### Fixed
- Bug where Z caused unexpected behavior

### Changed
- Improved performance of command A
```
