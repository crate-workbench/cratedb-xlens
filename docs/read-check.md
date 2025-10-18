# read-check - Cluster Data Readability Monitor

The `read-check` command is a professional monitoring tool that continuously checks cluster health by sampling data from the largest tables and partitions in your CrateDB cluster.

## Overview

This command provides real-time insights into your cluster's data availability and write activity patterns by:

- Automatically discovering the 5 largest tables/partitions
- Efficiently querying max(\_seq_no) every configurable interval
- Tracking `_seq_no` and `total_docs` changes
- Monitoring query performance
- Providing health status indicators

## Basic Usage

```bash
# Default monitoring (30s interval)
xmover read-check

# High-frequency monitoring
xmover read-check --seconds 10

# Custom configuration
xmover read-check --seconds 60
```

## Health Indicators

The command uses separate indicators for write activity and query performance:

**Write Activity Indicators:**

- 🟢 **Active**: Tables with recent `_seq_no` changes (data being written)
- 🟡 **Slow**: Tables with minimal write activity (infrequent writes)
- 🔴 **Stale**: Tables with no write activity for extended periods

**Query Performance Indicators:**

- ⚡ **Slow Query**: Individual queries taking >1000ms to complete
- ⚠️ **Anomaly**: Unusual activity patterns detected

## Sample Output

```
╭─────────────────────────────────────────────────╮
│ CrateDB Read Check [my-cluster]                 │
│ Monitoring max(_seq_no) every 30s from largest tables │
╰─────────────────────────────────────────────────╯

Write Activity: 🟢 Active • 🟡 Slow • 🔴 Stale
Query Performance: ⚡ >1000ms • ⚠️ Anomaly

19:27:33.681 | INFO | 🟢 TURVO.orderFormFieldData // _seq_no +1234 // total_docs +567 // 45ms
19:27:34.234 | INFO | 🟡 TURVO.inventoryData // _seq_no +12 // total_docs ±0 // 123ms
19:27:35.105 | INFO | 🔴 archive.old_events // _seq_no ±0 // total_docs ±0 // 2100ms ⚡

Note: 🟡🔴 indicate write activity level, ⚡ indicates slow query performance
```

## Key Features

### Automatic Table Discovery

- Discovers largest 5 tables/partitions by size every 10 minutes
- Handles both regular tables and partitioned tables seamlessly
- Logs when table discovery changes

### Optimized Query Performance

- Uses efficient `max(_seq_no)` queries instead of sorting/LIMIT
- Tracks actual database execution time (not network RTT)
- Alerts on queries >1000ms with ⚡ indicator
- Maintains rolling average of recent performance

### Reliability Features

- **Fresh connections** for each sample cycle
- **Exponential backoff retry** on connection/query failures
- **Graceful degradation** if individual tables become unavailable
- **Comprehensive error tracking** and reporting

### Statistics on Exit

Press CTRL+C to see detailed monitoring statistics:

```
╭───────────────────────────╮
│ 📊 Read Check Statistics  │
╰───────────────────────────╯
• Runtime: 0:05:42
• Samples taken: 87
• Changes detected: 23
• Connection failures: 0
• Query failures: 2

• Tables monitored (5):
  - TURVO.orderFormFieldData
  - TURVO.inventoryLevelFormFieldData
  - replication_first_materialized.orders_items
  - TURVO.inventoryFormFieldData
  - archive.events[date=2024-01-15]
```

## Partition Support

The command automatically handles partitioned tables by:

- Detecting partition information from system tables
- Building proper WHERE clauses for partition filtering
- Treating each partition as a separate monitoring target
- Displaying partition values in log output

Example with partitioned table:

```
19:28:15.442 | INFO | 🟢 TURVO.events[date=2024-01-15] // _seq_no +89 // total_docs +45 // 67ms
```

## Use Cases

### Cluster Health Monitoring

Run continuously to monitor overall cluster health and detect issues:

```bash
# Production monitoring with moderate frequency
xmover read-check --seconds 60

# High-frequency monitoring for troubleshooting
xmover read-check --seconds 10
```

### Write Activity Analysis

Identify which tables are most active and track write patterns:

- Monitor `_seq_no` changes to see write activity
- Track `total_docs` changes to see net document changes
- Use performance metrics to identify database execution bottlenecks

### Availability Testing

Verify cluster data availability with fresh connections:

- Each sample uses a new connection to test cluster responsiveness
- Retry logic validates connection stability
- Error tracking helps identify intermittent issues

## Command Options

| Option      | Default | Description                  |
| ----------- | ------- | ---------------------------- |
| `--seconds` | 30      | Sampling interval in seconds |

## Technical Details

### Discovery Query

The command uses an enhanced discovery query that joins system tables to get complete partition information:

```sql
SELECT s.schema_name, s.table_name, s.partition_ident,
       tp.values AS partition_values,
       ROUND(SUM(s.size) / 1024 / 1024 / 1024, 2) AS size_gb,
       SUM(s.num_docs) AS total_docs
FROM sys.shards s
LEFT JOIN information_schema.table_partitions tp
  ON s.schema_name = tp.table_schema
 AND s.table_name = tp.table_name
 AND s.partition_ident = tp.partition_ident
WHERE s.primary = true
GROUP BY s.schema_name, s.table_name, s.partition_ident, tp.values
ORDER BY size_gb DESC
LIMIT 5
```

### Sampling Query

For each discovered table, the command uses an optimized query:

```sql
SELECT max(_seq_no)
FROM "schema"."table"
[WHERE partition_conditions]
```

**Performance Measurement:**
The command uses CrateDB's built-in query duration from the response metadata, avoiding network round-trip time contamination. This provides accurate database execution times rather than end-to-end response times.

## Integration with Other Commands

The `read-check` command complements other XMover monitoring tools:

- Use with `monitor-recovery --watch` to track recovery progress
- Combine with `active-shards --watch` to correlate write activity
- Run alongside `large-translogs --watch` for comprehensive monitoring

## Best Practices

1. **Production Monitoring**: Use 30-60 second intervals for continuous monitoring
2. **Troubleshooting**: Use 10-15 second intervals for detailed investigation
3. **Write Activity**: Monitor 🟢🟡🔴 indicators to understand data activity patterns
4. **Query Performance**: Monitor ⚡ indicators to identify slow database execution (>1000ms)
5. **Capacity**: Track `total_docs` changes to understand growth patterns
6. **Reliability**: Check statistics on exit to validate connection stability
7. **Efficiency**: Optimized queries and database-side timing make frequent monitoring practical

The `read-check` command is the first XMover command to use structured logging with loguru, providing professional-grade monitoring capabilities with efficient query patterns for CrateDB clusters.
