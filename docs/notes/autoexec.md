# Autoexec feature

:::{div} sd-text-muted
Technical implementation notes.
:::

## Summary

The `--autoexec` flag extends `problematic-translogs` to automatically execute replica reset operations for tables with problematic translog sizes. This addresses a known CrateDB issue where replica shards accumulate large uncommitted translogs requiring manual intervention.

**Implementation approach:** Set replicas to 0, monitor retention lease clearance, restore original replica count.

## Technical Implementation

### Core Workflow

```
1. Identify tables exceeding translog thresholds (using adaptive flush_threshold_size)
2. For each problematic table/partition:
   a. ALTER TABLE SET ("number_of_replicas" = 0)
   b. Monitor sys.shards until retention_leases['leases'] count equals primary shard count
   c. ALTER TABLE SET ("number_of_replicas" = <original_value>)
```

### State Machine

```
DETECTED → SETTING_REPLICAS_ZERO → MONITORING_LEASES → RESTORING_REPLICAS → COMPLETED
                                                 ↓
                                              FAILED (with rollback attempt)
```

**State tracking:** Each table processed independently via `TableResetProcessor` class.

### CrateDB-Specific Queries

**Retention lease monitoring:**
```sql
SELECT array_length(retention_leases['leases'], 1) as cnt_leases
FROM sys.shards
WHERE table_name = ? AND schema_name = ? AND partition_ident = ?
```

**Replica count lookup:**
```sql
-- Non-partitioned tables
SELECT number_of_replicas
FROM information_schema.tables
WHERE table_name = ? AND table_schema = ?

-- Partitioned tables
SELECT number_of_replicas
FROM information_schema.table_partitions
WHERE table_name = ? AND table_schema = ? AND partition_ident = ?
```

**Adaptive threshold detection:**
```sql
SELECT table_schema, table_name,
       settings['translog']['flush_threshold_size'] as flush_threshold_size
FROM information_schema.tables
WHERE table_name = ? AND table_schema = ?
```

### Partition Handling

Partitioned tables use partition-specific ALTER TABLE syntax:
```sql
ALTER TABLE "schema"."table" PARTITION (date='2024-01-01') SET ("number_of_replicas" = 0);
```

Partition identification via `sys.shards.partition_ident` and `information_schema.table_partitions`.

## Security Considerations

### SQL Injection Prevention

All queries use parameterized statements:
```python
# Before (vulnerable)
sql = f"WHERE table_name = '{table_name}'"

# After (secure)
sql = "WHERE table_name = ?"
params = [table_name]
result = client.execute_query(sql, params)
```

**Validation:** Schema/table identifiers validated to reject characters that could break out of quoted identifiers (specifically `"`).

### Required Permissions

- `ALTER TABLE` on target schemas
- `SELECT` on `sys.shards`, `information_schema.tables`, `information_schema.table_partitions`

### Rollback Safety

On failure during `MONITORING_LEASES` or `RESTORING_REPLICAS` states:
1. Attempt to restore original replica count
2. Log CRITICAL error with table name and original replica count if rollback fails
3. Manual intervention required

**Critical bug fixed:** Rollback previously never executed due to state check occurring after state transition to FAILED. Fixed by capturing previous state before transition.

## Retry Strategy

### Retention Lease Monitoring

Incremental backoff delays (seconds): 10, 15, 30, 45, 60, 90, 135, 200, 300, 450, 720

**Rationale:** Retention lease clearance depends on cluster load and replication speed. Aggressive retries waste resources; incremental backoff balances responsiveness with cluster impact.

**Timeout handling:** Configurable via `--max-wait` (default: 720s). On timeout, log current lease count and fail with exit code 2.

## Error Handling

### Exit Codes

- `0`: Success (all tables processed)
- `1`: General error (connection failure, invalid parameters)
- `2`: Complete failure (all tables failed)
- `3`: Partial failure (some tables succeeded, some failed)

### Error Recovery

**Transient failures:** Connection errors, temporary database unavailability
- Action: Fail fast, log error, continue to next table

**Permanent failures:** Permission denied, table not found
- Action: Mark table as failed, log error, continue to next table

**Critical failures:** Unable to restore replica count
- Action: Log CRITICAL with manual intervention message, continue to next table

### Logging

**Console mode (default):** Human-readable with timestamps and state transitions

**JSON mode (`--log-format json`):** Structured logging for log aggregation systems (Kubernetes, ELK stack)
```json
{
  "timestamp": "2024-01-15T10:30:15.123Z",
  "level": "INFO",
  "table": "schema.table",
  "state": "MONITORING_LEASES",
  "original_replicas": 2,
  "elapsed_seconds": 45
}
```

## Code Quality Improvements

### Domain Models

**TableInfo dataclass:**
```python
@dataclass
class TableInfo:
    schema_name: str
    table_name: str
    partition_values: Optional[str]
    current_replicas: int
    max_translog_uncommitted_mb: float
    adaptive_threshold_mb: float
```

Replaces untyped `Dict[str, Any]` for type safety and IDE support.

**QueryResultHelper:**
```python
class QueryResultHelper:
    @staticmethod
    def is_success(result: Dict[str, Any]) -> bool:
        return 'error' not in result

    @staticmethod
    def get_error_message(result: Dict[str, Any]) -> str:
        return result.get('error', 'Unknown error')
```

Standardizes CrateDB response handling across codebase.

### SQL Generation

**ReplicaSQLBuilder class:** Centralizes ALTER TABLE and monitoring query generation, eliminates code duplication, provides consistent identifier validation.

### Context Management

**JSON logging:** Uses context manager to isolate loguru configuration changes, preventing global state mutation.

## Test Coverage

**Test suite:** 16 focused tests covering business scenarios (reduced from 44 implementation-detail tests)

**Coverage:**
- Regular and partitioned table workflows
- Timeout and failure scenarios
- Dry-run simulation
- Percentage-based filtering with adaptive thresholds
- Partial/complete failure handling
- CLI parameter validation

**Test philosophy:** Focus on business outcomes rather than implementation details. For example, test "replica reset completes successfully" rather than "backoff delay calculation produces specific values."

## Known Limitations

1. **Sequential processing:** Tables processed one at a time. No concurrent execution (safety over speed).

2. **No resume capability:** Interrupted operations must restart from beginning. State not persisted between runs.

3. **Retention lease assumption:** Assumes lease count equals primary shard count when cleared. May not hold in all CrateDB versions or configurations.

4. **Replica range parsing:** CrateDB returns replica counts as strings like "0-1". Current implementation takes maximum value. May not be correct interpretation for all use cases.

## Integration with Existing Code

### Backward Compatibility

- `--execute` flag (manual command generation) unchanged
- Default behavior (analysis mode) unchanged
- New flags (`--autoexec`, `--dry-run`) additive only

### Code Organization

- `MaintenanceCommands.problematic_translogs()`: Entry point, orchestrates workflow
- `MaintenanceCommands._execute_autoexec()`: Autoexec orchestration
- `TableResetProcessor`: Individual table state machine
- `ReplicaSQLBuilder`: SQL generation utilities

## Performance Characteristics

**Memory:** ~1KB per table for state tracking

**Network:** 1 ALTER TABLE + N retention lease queries + 1 ALTER TABLE per table
- N depends on cluster load and replication speed
- Typical: 3-10 queries per table

**Duration:** Primarily determined by retention lease clearance time
- Fast clusters: 30-60 seconds per table
- Loaded clusters: 5-10 minutes per table
- Timeout: Configurable (default 12 minutes)

## Questions for CrateDB Core Team

1. **Retention lease semantics:** Is `array_length(retention_leases['leases'], 1) == primary_shard_count` the correct success condition for all CrateDB versions?

2. **Replica count parsing:** When `number_of_replicas` returns "0-1", should we interpret this as max value (1) or target value?

3. **Flush threshold defaults:** Is 512MB + 10% buffer the correct default for tables without explicit `flush_threshold_size` configuration?

4. **Alternative approach:** Is there a more direct way to trigger translog flush/replica recreation than setting replicas to 0?

5. **Safety concerns:** Are there scenarios where setting replicas to 0 could cause data loss or corruption?
