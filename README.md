# XMover - CrateDB Shard Analyzer and Movement Tool

A comprehensive Python tool for analyzing CrateDB shard distribution across nodes and availability zones, and generating safe SQL commands for shard rebalancing and node decommissioning.

## Features

- **Cluster Analysis**: Complete overview of shard distribution across nodes and zones
- **Shard Distribution Analysis**: Detect and rank distribution anomalies across largest tables
- **Shard Movement Recommendations**: Intelligent suggestions for rebalancing with safety validation
- **Recovery Monitoring**: Track ongoing shard recovery operations with progress details
- **Cluster Health Monitoring**: Monitor data readability by sampling from largest tables
- **Zone Conflict Detection**: Prevents moves that would violate CrateDB's zone awareness
- **Node Decommissioning**: Plan safe node removal with automated shard relocation
- **Dry Run Mode**: Test recommendations without generating actual SQL commands
- **Safety Validation**: Comprehensive checks to ensure data availability during moves

## Installation

**Note: This project uses [uv](https://docs.astral.sh/uv/) for dependency management. Make sure you have uv installed.**

1. Clone the repository:

```bash
git clone <repository-url>
cd xmover
```

2. Install using uv (recommended) or pip:

```bash
# Using uv
uv sync

# Or using pip
pip install -e .
```

3. Create a `.env` file with your CrateDB connection details:

**For localhost CrateDB:**

```bash
CRATE_CONNECTION_STRING=https://localhost:4200
CRATE_USERNAME=crate
# CRATE_PASSWORD=  # Leave empty or unset for default crate user
CRATE_SSL_VERIFY=false
```

**For remote CrateDB:**

```bash
CRATE_CONNECTION_STRING=https://your-cluster.cratedb.net:4200
CRATE_USERNAME=your-username
CRATE_PASSWORD=your-password
CRATE_SSL_VERIFY=true
```

## Quick Start

### Test Connection

You can test your connection configuration with the included test script:

```bash
python test_connection.py
```

Or use the built-in test:

```bash
xmover test-connection
```

### Analyze Cluster

```bash
# Complete cluster analysis
xmover analyze

# Analyze specific table
xmover analyze --table my_table
```

### Find Movement Candidates

```bash
# Find shards that can be moved (40-60GB by default)
xmover find-candidates

# Custom size range
xmover find-candidates --min-size 20 --max-size 100
```

### Generate Recommendations

```bash
# Dry run (default) - shows what would be recommended
xmover recommend

# Generate actual SQL commands
xmover recommend --execute

# Prioritize space over zone balancing
xmover recommend --prioritize-space
```

### Shard Distribution Analysis

```bash
# Analyze distribution anomalies for top 10 largest tables
xmover shard-distribution

# Analyze more tables
xmover shard-distribution --top-tables 20

# Detailed health report for specific table
xmover shard-distribution --table my_table
```

### Zone Analysis

```bash
# Check zone balance
xmover check-balance

# Detailed zone analysis with shard-level details
xmover zone-analysis --show-shards
```

### Advanced Troubleshooting

```bash
# Validate specific moves before execution
xmover validate-move SCHEMA.TABLE SHARD_ID FROM_NODE TO_NODE

# Explain CrateDB error messages
xmover explain-error "your error message here"
```

## Commands Reference

### `analyze`

Analyzes current shard distribution across nodes and zones.

**Options:**

- `--table, -t`: Analyze specific table only
- `--largest INTEGER`: Show N largest tables/partitions by size
- `--smallest INTEGER`: Show N smallest tables/partitions by size
- `--no-zero-size`: Exclude zero-sized tables from smallest results (default: include zeros)

**Examples:**

```bash
# Basic cluster analysis
xmover analyze

# Analyze specific table only
xmover analyze --table events

# Show top 10 largest tables/partitions
xmover analyze --largest 10

# Show top 5 smallest tables/partitions (includes zero-sized)
xmover analyze --smallest 5

# Show top 5 smallest non-zero tables/partitions (exclude zero-sized)
xmover analyze --smallest 5 --no-zero-size

# Combine options
xmover analyze --table events --largest 3
```

**Sample Output (--largest 3):**

```
                        Largest Tables/Partitions by Size (Top 3)
╭─────────────────────────────────┬─────────────────────────────┬────────┬───────┬──────────┬──────────┬──────────┬────────────╮
│ Table                           │ Partition                   │ Shards │  P/R  │ Min Size │ Avg Size │ Max Size │ Total Size │
├─────────────────────────────────┼─────────────────────────────┼────────┼───────┼──────────┼──────────┼──────────┼────────────┤
│ TURVO.shipmentFormFieldData     │ ("id_ts_month"=162777600000 │      4 │ 2P/2R │   89.1GB │   95.3GB │  104.2GB │    381.2GB │
│ TURVO.orderFormFieldData        │ N/A                         │      6 │ 3P/3R │   23.4GB │   28.7GB │   35.1GB │    172.2GB │
│ TURVO.documentUploadProgress    │ ("sync_day"=1635724800000)  │      8 │ 4P/4R │   15.2GB │   18.4GB │   22.1GB │    147.2GB │
╰─────────────────────────────────┴─────────────────────────────┴────────┴───────┴──────────┴──────────┴──────────┴────────────╯

📊 Summary: 18 total shards using 700.6GB across 3 largest table/partition(s)
```

**Sample Output (--smallest 5 --no-zero-size):**

```
ℹ️  Found 12 table/partition(s) with 0.0GB size (excluded from results)

                        Smallest Tables/Partitions by Size (Top 5)
╭─────────────────────────────────┬─────────────────────────────┬────────┬───────┬──────────┬──────────┬──────────┬────────────╮
│ Table                           │ Partition                   │ Shards │  P/R  │ Min Size │ Avg Size │ Max Size │ Total Size │
├─────────────────────────────────┼─────────────────────────────┼────────┼───────┼──────────┼──────────┼──────────┼────────────┤
│ TURVO.emailActivity_transformf… │ N/A                         │      2 │ 1P/1R │    0.001GB │   0.001GB │    0.002GB │      0.002GB │
│ TURVO.calendarFormFieldData_tr… │ ("sync_day"=1627776000000)  │      2 │ 1P/1R │    0.005GB │   0.005GB │    0.005GB │      0.010GB │
│ TURVO.shipmentSummary_failures  │ N/A                         │      2 │ 1P/1R │    0.100GB │   0.100GB │    0.100GB │      0.200GB │
│ TURVO.documentActivity_failures │ N/A                         │      4 │ 2P/2R │    0.250GB │   0.325GB │    0.400GB │      1.300GB │
│ TURVO.userActivity_logs         │ ("date"=2024-01-01)         │      6 │ 3P/3R │    0.800GB │   0.950GB │    1.100GB │      5.700GB │
╰─────────────────────────────────┴─────────────────────────────┴────────┴───────┴──────────┴──────────┴──────────┴────────────╯

📊 Summary: 16 total shards using 7.212GB across 5 smallest non-zero table/partition(s)
```

### `find-candidates`

Finds shards suitable for movement based on size and health criteria.

**Options:**

- `--table, -t`: Find candidates in specific table only
- `--min-size`: Minimum shard size in GB (default: 40)
- `--max-size`: Maximum shard size in GB (default: 60)
- `--node`: Only show candidates from this specific source node (e.g., data-hot-4)

**Examples:**

```bash
# Find candidates in size range for specific table
xmover find-candidates --min-size 20 --max-size 50 --table logs

# Find candidates on a specific node
xmover find-candidates --min-size 30 --max-size 60 --node data-hot-4
```

### `recommend`

Generates intelligent shard movement recommendations for cluster rebalancing.

**Options:**

- `--table, -t`: Generate recommendations for specific table only
- `--min-size`: Minimum shard size in GB (default: 40)
- `--max-size`: Maximum shard size in GB (default: 60)
- `--zone-tolerance`: Zone balance tolerance percentage (default: 10)
- `--min-free-space`: Minimum free space required on target nodes in GB (default: 100)
- `--max-moves`: Maximum number of move recommendations (default: 10)
- `--max-disk-usage`: Maximum disk usage percentage for target nodes (default: 95, auto-adjusted based on watermarks)
- `--validate/--no-validate`: Validate move safety (default: True)
- `--prioritize-space/--prioritize-zones`: Prioritize available space over zone balancing (default: False)
- `--dry-run/--execute`: Show what would be done without generating SQL commands (default: True)
- `--node`: Only recommend moves from this specific source node (e.g., data-hot-4)

**Examples:**

```bash
# Dry run with zone balancing priority
xmover recommend --prioritize-zones

# Generate SQL for space optimization
xmover recommend --prioritize-space --execute

# Focus on specific table with custom parameters
xmover recommend --table events --min-size 10 --max-size 30 --execute

# Target space relief for a specific node
xmover recommend --prioritize-space --min-size 30 --max-size 60 --node data-hot-4

# Allow higher disk usage for urgent moves
xmover recommend --prioritize-space --max-disk-usage 90
```

**⚠️ IMPORTANT: Cluster Rebalancing Management**

Before executing manual shard moves with `--execute`, disable CrateDB's automatic rebalancing to prevent conflicts:

```sql
SET GLOBAL PERSISTENT "cluster.routing.rebalance.enable"='none';
```

After completing your moves, re-enable rebalancing:

```sql
SET GLOBAL PERSISTENT "cluster.routing.rebalance.enable"='all';
```

This prevents the automatic rebalancer from interfering with your manual moves. See `MANUAL_SHARD_MANAGEMENT_GUIDE.md` for comprehensive operational procedures.

**💡 Smart Disk Usage Thresholds**

XMover automatically uses your cluster's disk watermark settings to determine safe disk usage thresholds:

```bash
# XMover queries your cluster's watermark configuration
SELECT settings['cluster']['routing']['allocation']['disk']['watermark'] FROM sys.cluster;
SELECT settings['cluster']['routing']['allocation']['disk']['threshold_enabled'] FROM sys.cluster;

# Example: Cluster with 85% low watermark → XMover uses 83% threshold (with 2% safety buffer)
xmover recommend --max-disk-usage 90  # Auto-adjusted to 83% with warning
xmover recommend --max-disk-usage 80  # Used as-is (within safe limits)
```

**Benefits:**

- **Cluster-Aware**: Respects your specific watermark configuration
- **Safety Buffer**: Maintains 2% buffer below low watermark
- **User Override**: Honors lower user-specified values
- **Automatic Adjustment**: Shows clear warnings when adjusting thresholds

### `zone-analysis`

Provides detailed analysis of zone distribution and potential conflicts.

**Options:**

- `--table, -t`: Analyze zones for specific table only
- `--show-shards/--no-show-shards`: Show individual shard details (default: False)

**Example:**

```bash
xmover zone-analysis --show-shards --table critical_data
```

### `check-balance`

Checks zone balance for shards with configurable tolerance.

**Options:**

- `--table, -t`: Check balance for specific table only
- `--tolerance`: Zone balance tolerance percentage (default: 10)

**Example:**

```bash
xmover check-balance --tolerance 15
```

### `validate-move`

Validates a specific shard move before execution to prevent errors.

**Arguments:**

- `SCHEMA_TABLE`: Schema and table name (format: schema.table)
- `SHARD_ID`: Shard ID to move
- `FROM_NODE`: Source node name
- `TO_NODE`: Target node name

**Examples:**

```bash
# Standard validation
xmover validate-move CUROV.maddoxxxS 4 data-hot-1 data-hot-3

# Allow higher disk usage for urgent moves
xmover validate-move CUROV.tendedero 4 data-hot-1 data-hot-3 --max-disk-usage 90
```

### `explain-error`

Explains CrateDB allocation error messages and provides troubleshooting guidance.

**Arguments:**

- `ERROR_MESSAGE`: The CrateDB error message to analyze (optional - can be provided interactively)

**Examples:**

```bash
# Interactive mode
xmover explain-error

# Direct analysis
xmover explain-error "NO(a copy of this shard is already allocated to this node)"
```

### `monitor-recovery`

Monitors active shard recovery operations on the cluster.

**Options:**

- `--table, -t`: Monitor recovery for specific table only
- `--node, -n`: Monitor recovery on specific node only
- `--watch, -w`: Continuously monitor (refresh every 10s)
- `--refresh-interval`: Refresh interval for watch mode in seconds (default: 10)
- `--recovery-type`: Filter by recovery type - PEER, DISK, or all (default: all)
- `--include-transitioning`: Include recently completed recoveries (DONE stage)

**Examples:**

```bash
# Check current recovery status
xmover monitor-recovery

# Monitor specific table recoveries
xmover monitor-recovery --table PartioffD

# Continuous monitoring with custom refresh rate
xmover monitor-recovery --watch --refresh-interval 5

# Monitor only PEER recoveries on specific node
xmover monitor-recovery --node data-hot-1 --recovery-type PEER

# Include completed recoveries still transitioning
xmover monitor-recovery --watch --include-transitioning
```

**Recovery Types:**

- **PEER**: Copying shard data from another node (replication/relocation)
- **DISK**: Rebuilding shard from local data (after restart/disk issues)

**Enhanced Translog Monitoring:**
The recovery monitor now displays detailed translog information in the format:

```
📋 TURVO.shipmentFormFieldData_events S4 PEER TRANSLOG 0.0% 6.2GB (TL:109.8GB / 22.1GB / 20%) data-hot-0 → data-hot-7
```

**Translog Display Format**: `TL:X.XGB / Y.YGB / ZZ%`

- `X.XGB`: Total translog file size (`translog_stats['size']`)
- `Y.YGB`: Uncommitted translog size (`translog_stats['uncommitted_size']`)
- `ZZ%`: Uncommitted as percentage of total translog size

**Color Coding:**

- 🔴 **Red**: Uncommitted ≥ 5GB OR uncommitted ≥ 80% (critical)
- 🟡 **Yellow**: Uncommitted ≥ 1GB OR uncommitted ≥ 50% (warning)
- 🟢 **Green**: Below warning thresholds (normal)

Translog information is only shown when significant (uncommitted ≥ 10MB or total ≥ 50MB).

**Enhanced Replica Progress Tracking:**
For replica shard recoveries, the monitor now shows sequence number-based progress when available:

```
📋 TURVO.LINEAGE_DIRECTLY_OPEN_TO_APPOINTMENT S2R PEER TRANSLOG 99.9% (seq) 15.2GB data-hot-0 → data-hot-1
```

**Progress Display Formats:**

- `99.9% (seq)`: Replica progress based on sequence number comparison with primary
- `37.5% (seq) / 95.0% (rec)`: Shows both when sequence and traditional progress differ significantly (>5%)
- `98.5%`: Primary shards or when sequence data unavailable (traditional progress)

**Sequence Progress Benefits:**

- More accurate progress indication for replica synchronization
- Based on comparing `max_seq_no` between replica and primary shards
- Reveals actual replication lag in terms of operations behind primary
- Particularly useful for detecting stuck replica recoveries where traditional recovery shows 100% but replica is still far behind

**Enhanced Transitioning Recovery Display:**
The monitor now shows detailed information for transitioning recoveries instead of just "(transitioning)":

```
16:08:20 | 5 done (transitioning)
         | 🔄 TURVO.accountFormFieldData S7R PEER DONE 99.8% (seq) 3.8GB data-hot-5 → data-hot-7
         | 🔄 TURVO_MySQL.composite_mapping S11P PEER DONE 100.0% 3.0GB data-hot-5 → data-hot-6
         | 🔄 TURVO.shipmentFormFieldData ("id_ts_month"=1633046400000) S6R PEER DONE 99.8% (seq) 8.2GB (TL:233MB / 49MB / 21%) data-hot-4 → data-hot-7
```

**Transitioning Display Features:**

- Shows up to 5 transitioning recoveries with full details
- Includes sequence progress, translog info, and node routing
- Throttled to every 30 seconds to reduce noise
- Uses 🔄 icon to indicate transitioning state
- Distinguishes primary (P) vs replica (R) shards

### `problematic-translogs`

Find tables with problematic translog sizes and generate replica management commands.

**Options:**

- `--sizeMB INTEGER`: Minimum translog uncommitted size in MB (default: 300)
- `--execute`: Execute the replica management commands after confirmation

**Description:**
This command identifies tables with replica shards that have large uncommitted translog sizes indicating replication issues. It shows both individual problematic shards and a summary by table/partition. It generates two types of ALTER commands: individual REROUTE CANCEL SHARD commands for each problematic shard, and replica management commands that temporarily set replicas to 0 and restore them to force recreation of problematic replicas.

**Examples:**

```bash
# Show problematic tables with translog > 300MB (default)
xmover problematic-translogs

# Show tables with translog > 500MB
xmover problematic-translogs --sizeMB 500

# Execute replica management commands for tables > 1GB after confirmation
xmover problematic-translogs --sizeMB 1000 --execute
```

**Sample Output:**

```
                   Problematic Replica Shards (translog > 300MB)
╭────────┬───────────────────────────────┬────────────────────────────┬──────────┬────────────┬─────────────╮
│ Schema │ Table                         │ Partition                  │ Shard ID │ Node       │ Translog MB │
├────────┼───────────────────────────────┼────────────────────────────┼──────────┼────────────┼─────────────┤
│ TURVO  │ shipmentFormFieldData         │ none                       │       14 │ data-hot-6 │      7040.9 │
│ TURVO  │ shipmentFormFieldData_events  │ ("sync_day"=1757376000000) │        3 │ data-hot-2 │       481.2 │
│ TURVO  │ orderFormFieldData            │ none                       │        5 │ data-hot-1 │       469.5 │
╰────────┴───────────────────────────────┴────────────────────────────┴──────────┴────────────┴─────────────╯

Found 2 table/partition(s) with problematic translogs:

              Tables with Problematic Replicas (translog > 300MB)
╭────────┬───────────┬───────────┬───────────┬──────────┬─────────────┬──────────────┬──────────╮
│ Schema │ Table     │ Partition │ Problema… │ Max      │ Shards      │ Size GB      │ Current  │
│        │           │           │ Replicas  │ Trans.MB │ (P/R)       │ (P/R)        │ Replicas │
├────────┼───────────┼───────────┼───────────┼──────────┼─────────────┼──────────────┼──────────┤
│ TURVO  │ shipment… │ ("sync..  │         2 │   7011.8 │ 5P/5R       │ 12.4/12.1    │        1 │
│ TURVO  │ orderFor… │ none      │         1 │    469.5 │ 3P/6R       │ 8.2/16.3     │        2 │
╰────────┴───────────┴───────────┴───────────┴──────────┴─────────────┴──────────────┴──────────╯

Generated ALTER Commands:

ALTER TABLE "TURVO"."shipmentFormFieldData" REROUTE CANCEL SHARD 14 on 'data-hot-6' WITH (allow_primary=False);
ALTER TABLE "TURVO"."shipmentFormFieldData_events" partition ("sync_day"=1757376000000) REROUTE CANCEL SHARD 3 on 'data-hot-2' WITH (allow_primary=False);
ALTER TABLE "TURVO"."orderFormFieldData" REROUTE CANCEL SHARD 5 on 'data-hot-1' WITH (allow_primary=False);

-- Set replicas to 0:
ALTER TABLE "TURVO"."shipmentFormFieldData" PARTITION ("id_ts_month"=1756684800000) SET ("number_of_replicas" = 0);
-- Restore replicas to 1:
ALTER TABLE "TURVO"."shipmentFormFieldData" PARTITION ("id_ts_month"=1756684800000) SET ("number_of_replicas" = 1);

-- Set replicas to 0:
ALTER TABLE "TURVO"."orderFormFieldData" SET ("number_of_replicas" = 0);
-- Restore replicas to 2:
ALTER TABLE "TURVO"."orderFormFieldData" SET ("number_of_replicas" = 2);

Total: 3 REROUTE CANCEL commands + 4 replica management commands
```

When using `--execute`, each command is presented individually for confirmation, allowing you to selectively execute specific commands as needed.

### `active-shards`

Monitors the most active shards by tracking checkpoint progression over time.

**Options:**

- `--count`: Number of most active shards to show (default: 10)
- `--interval`: Observation interval in seconds (default: 30)
- `--min-checkpoint-delta`: Minimum checkpoint progression between snapshots to show shard (default: 1000)
- `--table, -t`: Monitor specific table only
- `--node, -n`: Monitor specific node only
- `--watch, -w`: Continuously monitor (refresh every interval)
- `--exclude-system`: Exclude system tables (gc._, information_schema._, _\_events, _\_log)
- `--min-rate`: Minimum activity rate (changes/sec) to show
- `--show-replicas/--hide-replicas`: Show replica shards (default: True)

**Examples:**

```bash
# Show top 10 most active shards over 30 seconds
xmover active-shards

# Top 20 shards with 60-second observation period
xmover active-shards --count 20 --interval 60

# Continuous monitoring with 30-second intervals
xmover active-shards --watch --interval 30

# Monitor specific table activity
xmover active-shards --table my_table --watch

# Monitor specific node with custom threshold
xmover active-shards --node data-hot-1 --min-checkpoint-delta 500

# Exclude system tables and event logs for business data focus
xmover active-shards --exclude-system --count 20

# Only show high-activity shards (≥50 changes/sec)
xmover active-shards --min-rate 50 --count 15

# Focus on primary shards only
xmover active-shards --hide-replicas --count 20
```

This command helps identify which shards are receiving the most write activity by measuring local checkpoint progression between two snapshots.

**How it works:**

1. **Takes snapshot of ALL started shards** (not just currently active ones)
2. **Waits for observation interval** (configurable, default: 30 seconds)
3. **Takes second snapshot** of all started shards
4. **Compares snapshots** to find shards with checkpoint progression ≥ threshold
5. **Shows ranked results** with activity trends and insights

**Enhanced output features:**

- **Checkpoint visibility**: Shows actual `local_checkpoint` values (CP Start → CP End → Delta)
- **Partition awareness**: Separate tracking for partitioned tables (different partition_ident values)
- **Activity trends**: 🔥 HOT (≥100/s), 📈 HIGH (≥50/s), 📊 MED (≥10/s), 📉 LOW (<10/s)
- **Smart insights**: Identifies concentration patterns and load distribution (non-watch mode)
- **Flexible filtering**: Exclude system tables, set minimum rates, hide replicas
- **Context information**: Total activity, average rates, observation period
- **Clean watch mode**: Streamlined output without legend/insights for continuous monitoring

This approach captures shards that become active during the observation period, providing a complete view of cluster write patterns and identifying hot spots. The enhanced filtering helps focus on business-critical activity patterns.

**Sample output (single run):**

```
🔥 Most Active Shards (3 shown, 30s observation period)

Total checkpoint activity: 190,314 changes, Average rate: 2,109.0/sec

   Rank | Schema.Table           | Shard | Partition      | Node       | Type | Checkpoint Δ | Rate/sec | Trend
   -----------------------------------------------------------------------------------------------------------
   1    | gc.scheduled_jobs_log  | 0     | -              | data-hot-8 | P    | 113,744      | 3,791.5  | 🔥 HOT
   2    | TURVO.events           | 0     | 04732dpl6osj8d | data-hot-0 | P    | 45,837       | 1,527.9  | 🔥 HOT
   3    | doc.user_actions       | 1     | 04732dpk70rj6d | data-hot-2 | P    | 30,733       | 1,024.4  | 🔥 HOT

Legend:
  • Checkpoint Δ: Write operations during observation period
  • Partition: partition_ident (truncated if >14 chars, '-' if none)

Insights:
  • 3 HOT shards (≥100 changes/sec) - consider load balancing
  • All active shards are PRIMARY - normal write pattern
```

**Sample output (watch mode - cleaner):**

```
30s interval | threshold: 1,000 | top 5

🔥 Most Active Shards (3 shown, 30s observation period)

Total checkpoint activity: 190,314 changes, Average rate: 2,109.0/sec

   Rank | Schema.Table           | Shard | Partition      | Node       | Type | Checkpoint Δ | Rate/sec | Trend
   -----------------------------------------------------------------------------------------------------------
   1    | gc.scheduled_jobs_log  | 0     | -              | data-hot-8 | P    | 113,744      | 3,791.5  | 🔥 HOT
   2    | TURVO.events           | 0     | 04732dpl6osj8d | data-hot-0 | P    | 45,837       | 1,527.9  | 🔥 HOT
   3    | doc.user_actions       | 1     | 04732dpk70rj6d | data-hot-2 | P    | 30,733       | 1,024.4  | 🔥 HOT

━━━ Next update in 30s ━━━
```

### `large-translogs`

Monitors shards with large translog uncommitted sizes that do not flush properly, displaying both primary and replica shards.

**Options:**

- `--translogsize`: Minimum translog uncommitted size threshold in MB (default: 500)
- `--interval`: Monitoring interval in seconds for watch mode (default: 60)
- `--watch, -w`: Continuously monitor (refresh every interval)
- `--table, -t`: Monitor specific table only
- `--node, -n`: Monitor specific node only
- `--count`: Maximum number of shards with large translogs to show (default: 50)

**Examples:**

```bash
# Show shards with translog over default 500MB threshold
xmover large-translogs

# Show shards with translog over 1GB threshold
xmover large-translogs --translogsize 1000

# Continuous monitoring every 30 seconds
xmover large-translogs --watch --interval 30

# Monitor specific table
xmover large-translogs --table my_table --watch

# Monitor specific node, show top 20
xmover large-translogs --node data-hot-1 --count 20
```

This command helps identify shards that are not flushing properly by monitoring their translog uncommitted sizes, which can indicate replication or flush issues.

### `read-check`

Monitors cluster data readability by continuously sampling records from the largest tables/partitions.

**Options:**

- `--seconds`: Sampling interval in seconds (default: 30)

**Examples:**

```bash
# Default monitoring (30s interval)
xmover read-check

# High-frequency monitoring
xmover read-check --seconds 10

# Custom interval
xmover read-check --seconds 60
```

**Features:**

- 🟢 Active tables (seq_no changing), 🟡 Slow tables, 🔴 Stale tables
- Performance tracking with query response times
- Automatic discovery of 5 largest tables every 10 minutes
- Fresh connections and retry logic for reliability
- Professional statistics on exit (CTRL+C)

**Output includes:**

- **Schema.Table**: Combined schema and table name
- **Partition**: Partition values or "-" for non-partitioned tables
- **Shard**: Numeric shard identifier
- **Node**: Node where shard is located
- **TL MB**: Translog uncommitted size (color-coded: bright_red >1GB, red >500MB, yellow >100MB, green ≤100MB)
- **Type**: "P" for primary shards, "R" for replica shards
- **Timestamp**: Current time for each update
- **Summary**: Total shards, primary/replica breakdown, average translog size

**Sample output:**

```
Large Translogs (>400MB) - 09:45:51
╭────────────────────────────┬──────────────────────┬───────┬────────────┬────────┬──────╮
│ Schema.Table               │ Partition            │ Shard │ Node       │  TL MB │ Type │
├────────────────────────────┼──────────────────────┼───────┼────────────┼────────┼──────┤
│ TURVO.orderFormFieldData_… │ ("sync_day"=175936.… │     7 │ data-hot-7 │    510 │  P   │
│ TURVO.orderFormFieldData   │ -                    │     8 │ data-hot-6 │    509 │  R   │
│ TURVO.orderFormFieldData   │ -                    │    20 │ data-hot-3 │    507 │  R   │
╰────────────────────────────┴──────────────────────┴───────┴────────────┴────────┴──────╯
3 shards (1P/2R) - Avg translog: 509MB
```

### `test-connection`

Tests the connection to CrateDB and displays basic cluster information.

## Operation Modes

### Analysis vs Operational Views

XMover provides two distinct views of your cluster:

1. **Analysis View** (`analyze`, `zone-analysis`): Includes ALL shards regardless of state for complete cluster visibility
2. **Operational View** (`find-candidates`, `recommend`): Only includes healthy shards (STARTED + 100% recovered) for safe operations

### Prioritization Modes

When generating recommendations, you can choose between two prioritization strategies:

1. **Zone Balancing Priority** (default): Focuses on achieving optimal zone distribution first, then considers available space
2. **Space Priority**: Prioritizes moving shards to nodes with more available space, regardless of zone balance

### Safety Features

- **Zone Conflict Detection**: Prevents moves that would place multiple copies of the same shard in the same zone
- **Capacity Validation**: Ensures target nodes have sufficient free space
- **Health Checks**: Only operates on healthy shards (STARTED routing state + 100% recovery)
- **SQL Quoting**: Properly quotes schema and table names in generated SQL commands

## Example Workflows

### Regular Cluster Maintenance

1. Analyze current state:

```bash
xmover analyze
```

2. Check for zone imbalances:

```bash
xmover check-balance
```

3. Generate and review recommendations:

```bash
xmover recommend --dry-run
```

4. Execute safe moves:

```bash
xmover recommend --execute
```

### Targeted Node Relief

When a specific node is running low on space:

1. Check which node needs relief:

```bash
xmover analyze
```

2. Generate recommendations for that specific node:

```bash
xmover recommend --prioritize-space --node data-hot-4 --dry-run
```

3. Execute the moves:

```bash
xmover recommend --prioritize-space --node data-hot-4 --execute
```

### Monitoring Shard Recovery Operations

After executing shard moves, monitor the recovery progress:

1. Execute moves and monitor recovery:

```bash
# Execute moves
xmover recommend --node data-hot-1 --execute

# Monitor the resulting recoveries
xmover monitor-recovery --watch
```

2. Monitor specific table or node recovery:

```bash
# Monitor specific table
xmover monitor-recovery --table shipmentFormFieldData --watch

# Monitor specific node
xmover monitor-recovery --node data-hot-4 --watch

# Monitor including completed recoveries
xmover monitor-recovery --watch --include-transitioning
```

3. Check recovery after node maintenance:

```bash
# After bringing a node back online
xmover monitor-recovery --node data-hot-3 --recovery-type DISK
```

### Monitoring Active Shards and Write Patterns

Identify which shards are receiving the most write activity:

1. Quick snapshot of most active shards:

```bash
# Show top 10 most active shards over 30 seconds
xmover active-shards

# Longer observation period for more accurate results
xmover active-shards --count 15 --interval 60
```

2. Continuous monitoring for real-time insights:

```bash
# Continuous monitoring with 30-second intervals
xmover active-shards --watch --interval 30

# Monitor specific table for focused analysis
xmover active-shards --table critical_table --watch
```

3. Integration with rebalancing workflow:

```bash
# Identify hot shards first
xmover active-shards --count 20 --interval 60

# Move hot shards away from overloaded nodes
xmover recommend --table hot_table --prioritize-space --execute

# Monitor the impact
xmover active-shards --table hot_table --watch
```

### Manual Shard Movement

1. Validate the move first:

```bash
xmover validate-move SCHEMA.TABLE SHARD_ID FROM_NODE TO_NODE
```

2. Generate safe recommendations:

```bash
xmover recommend --prioritize-space --execute
```

3. Monitor shard health after moves

### Troubleshooting Zone Conflicts

1. Identify conflicts:

```bash
xmover zone-analysis --show-shards
```

2. Generate targeted fixes:

```bash
xmover recommend --prioritize-zones --execute
```

## Configuration

### Environment Variables

- `CRATE_CONNECTION_STRING`: CrateDB HTTP endpoint (required)
- `CRATE_USERNAME`: Username for authentication (optional)
- `CRATE_PASSWORD`: Password for authentication (optional, only used if username is also provided)
- `CRATE_SSL_VERIFY`: SSL certificate verification (default: auto-detects based on connection string)
  - `true`: Always verify SSL certificates
  - `false`: Disable SSL certificate verification
  - `auto`: Automatically disable for localhost/127.0.0.1, enable for remote connections

#### Retry and Timeout Configuration

For clusters under pressure, you can configure retry behavior:

- `CRATE_MAX_RETRIES`: Maximum number of retries for failed queries (default: 3)
- `CRATE_TIMEOUT`: Base timeout in seconds for queries (default: 30)
- `CRATE_MAX_TIMEOUT`: Maximum timeout in seconds for retries (default: 120)
- `CRATE_RETRY_BACKOFF`: Exponential backoff factor between retries (default: 2.0)

### Connection String Format

```
https://hostname:port
```

The tool automatically appends `/_sql` to the endpoint.

## Safety Considerations

⚠️ **Important Safety Notes:**

1. **Always test in non-production environments first**
2. **Monitor shard health after each move before proceeding with additional moves**
3. **Ensure adequate cluster capacity before decommissioning nodes**
4. **Verify zone distribution after rebalancing operations**
5. **Keep backups current before performing large-scale moves**

## Troubleshooting

XMover provides comprehensive troubleshooting tools to help diagnose and resolve shard movement issues.

### Quick Diagnosis Commands

```bash
# Validate a specific move before execution
xmover validate-move SCHEMA.TABLE SHARD_ID FROM_NODE TO_NODE

# Explain CrateDB error messages
xmover explain-error "your error message here"

# Check zone distribution for conflicts
xmover zone-analysis --show-shards

# Verify overall cluster health
xmover analyze
```

### Common Issues and Solutions

1. **Zone Conflicts**

   ```
   Error: "NO(a copy of this shard is already allocated to this node)"
   ```

   - **Cause**: Target node already has a copy of the shard
   - **Solution**: Use `xmover zone-analysis --show-shards` to find alternative targets
   - **Prevention**: Always use `xmover validate-move` before executing moves

2. **Zone Allocation Limits**

   ```
   Error: "too many copies of the shard allocated to nodes with attribute [zone]"
   ```

   - **Cause**: CrateDB's zone awareness prevents too many copies in same zone
   - **Solution**: Move shard to a different availability zone
   - **Prevention**: Use `xmover recommend` which respects zone constraints

3. **Insufficient Space**

   ```
   Error: "not enough disk space"
   ```

   - **Cause**: Target node lacks sufficient free space
   - **Solution**: Choose node with more capacity or free up space
   - **Check**: `xmover analyze` to see available space per node

4. **High Disk Usage Blocking Moves**

   ```
   Error: "Target node disk usage too high (85.3%)"
   ```

   - **Cause**: Target node exceeds default 85% disk usage threshold
   - **Solution**: Use `--max-disk-usage` to allow higher usage for urgent moves
   - **Example**: `xmover recommend --max-disk-usage 90 --prioritize-space`

5. **No Recommendations Generated**
   - **Cause**: Cluster may already be well balanced
   - **Solution**: Adjust size filters or check `xmover check-balance`
   - **Try**: `--prioritize-space` mode for capacity-based moves

### Error Message Decoder

Use the built-in error decoder for complex CrateDB messages:

```bash
# Interactive mode - paste your error message
xmover explain-error

# Direct analysis
xmover explain-error "NO(a copy of this shard is already allocated to this node)"
```

### Configurable Safety Thresholds

XMover uses configurable safety thresholds to prevent risky moves:

**Disk Usage Threshold (default: 85%)**

```bash
# Allow moves to nodes with higher disk usage
xmover recommend --max-disk-usage 90 --prioritize-space

# For urgent space relief
xmover validate-move SCHEMA.TABLE SHARD_ID FROM TO --max-disk-usage 95
```

**When to Adjust Thresholds:**

- **Emergency situations**: Increase to 90-95% for critical space relief
- **Conservative operations**: Decrease to 75-80% for safer moves
- **Staging environments**: Can be more aggressive (90%+)
- **Production**: Keep conservative (80-85%)

### Advanced Troubleshooting

For detailed troubleshooting procedures, see [TROUBLESHOOTING.md](TROUBLESHOOTING.md) which covers:

- Step-by-step diagnostic procedures
- Emergency recovery procedures
- Best practices for safe operations
- Complete error reference guide

### Debug Information

All commands provide detailed safety validation messages and explanations for any issues detected.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

[Add your license information here]

## Support

For issues and questions, please create an issue in the repository or contact your CrateDB administrator.
