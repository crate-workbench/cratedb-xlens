# XMover Changelog

All notable changes to XMover will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

### Changed

- **First loguru integration**: `read-check` is the first command to use structured logging
- **Enhanced per-table statistics**: Shows document change tracking and performance metrics
  - Document changes: Total change with min/avg/max deltas
  - Performance: Query response time min/avg/max
  - Anomaly counter per table
- **Query optimization**: `read-check` uses efficient `max(_seq_no)` aggregation instead of sorting
- **Parameter cleanup**: Removed obsolete `--limit` parameter from `read-check` command
- **Performance measurement**: `read-check` now uses database execution time from CrateDB response instead of measuring network round-trip time

### Dependencies

- Added `loguru>=0.7.0` for professional structured logging

### Documentation

- Updated main README.md with `read-check` command reference
- Added comprehensive `docs/read-check.md` with usage guide and examples
- Updated `docs/README.md` with data readability monitoring section

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
