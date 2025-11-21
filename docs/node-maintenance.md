(node-maintenance)=

# CrateDB Node Maintenance Analysis

The `check-maintenance` command provides comprehensive analysis of CrateDB node decommissioning scenarios, helping you understand the impact and requirements before taking a node offline for maintenance.

## Overview

When you need to perform maintenance on a CrateDB node (hardware upgrades, OS updates, repairs, etc.), you need to understand:

- What data will be affected
- Where that data can be relocated
- How long the process will take
- Whether the cluster has sufficient capacity
- What commands to run for safe decommissioning

The `check-maintenance` command analyzes all these factors and provides actionable recommendations.

## Usage

```bash
xmover check-maintenance --node <node-name> --min-availability <level>
```

### Required Parameters

- `--node`: The name of the node you want to analyze for maintenance
- `--min-availability`: The minimum data availability level during maintenance
  - `full`: Move all shards (primaries and replicas) off the node
  - `primaries`: Only ensure primary shards have replicas elsewhere (faster)

### Examples

```bash
# Full analysis - move all shards off the node
xmover check-maintenance --node data-hot-4 --min-availability full

# Primaries-only analysis - faster but may leave some replicas on the node
xmover check-maintenance --node data-hot-4 --min-availability primaries
```

## Min-Availability Levels Explained

### Full Mode (`--min-availability full`)

- **What it does**: Analyzes moving ALL shards (primaries and replicas) off the target node
- **When to use**: Complete node shutdown, hardware replacement, or when you want zero data on the node
- **Impact**: Slower process but ensures the node can be completely powered down
- **Data movement**: All shard data must be copied to other nodes

### Primaries Mode (`--min-availability primaries`)

- **What it does**: Ensures all primary shards have replicas on other nodes
- **When to use**: Software updates, restarts, temporary maintenance where the node will come back
- **Impact**: Faster process, minimal data movement
- **Data movement**: Only primary shards without replicas need to be moved

#### Primaries Mode Operations:
- **Fast Operations**: Primary shards with existing replicas are demoted to replicas (metadata change only)
- **Slow Operations**: Primary shards without replicas must have their data copied elsewhere

## Output Sections

### 1. Maintenance Analysis Summary

Provides high-level overview of the maintenance requirements:

```
┌─────────────────────────────────────────┐
│       📊 Maintenance Analysis Summary    │
├─────────────────────────────────────────┤
│ Target Node: data-hot-4 (Zone: us-west-2c) │
│ Min-availability: Full                   │
│ Total Shards on Node: 45 (30 primaries, 15 replicas) │
│ Data to Move: 1.2TB                    │
│ Available Capacity: 2.8TB              │
│ Capacity Check: ✅ Sufficient           │
└─────────────────────────────────────────┘
```

**Key Metrics:**
- **Target Node**: Node being analyzed and its availability zone
- **Total Shards**: Breakdown of primary vs replica shards
- **Data to Move**: Total amount of data that needs to be relocated
- **Available Capacity**: Free space on candidate nodes in the same zone
- **Capacity Check**: Whether the cluster has sufficient space and shard slots

### 2. Shard Analysis by Type

Breaks down what actions are required for different types of shards:

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Shard Analysis by Type                          │
├─────────────────────────────┬─────────┬─────────────┬───────────────┤
│ Shard Type                  │ Count   │ Total Size  │ Action Required│
├─────────────────────────────┼─────────┼─────────────┼───────────────┤
│ Primary Shards (with replicas) │  20   │    800GB    │ Move data     │
│ Primary Shards (without replicas)│ 10  │    400GB    │ Move data     │
│ Replica Shards              │   15    │    600GB    │ Move data     │
└─────────────────────────────┴─────────┴─────────────┴───────────────┘
```

**For Full Mode:**
- All shard types show "Move data" as the action
- Shows the impact scope of the maintenance

**For Primaries Mode:**
- "Convert to replica (fast)" for primaries with existing replicas
- "Move data (slow)" for primaries without replicas  
- "No action needed" for replica shards

### 3. Target Nodes Capacity

Shows which nodes in the same availability zone can accept the relocated shards:

```
┌─────────────────────────────────────────────────────────────────────┐
│                Target Nodes Capacity (Zone: us-west-2c)             │
├─────────────┬──────────────────┬───────────────┬─────────────┬──────────────┤
│ Node        │ Space Below Low WM│ Shard Capacity│ Disk Usage  │ Status       │
├─────────────┼──────────────────┼───────────────┼─────────────┼──────────────┤
│ data-hot-5  │ 800GB           │ 234 / 1000    │ 65.2%       │ ✅ Available  │
│ data-hot-6  │ 1.2TB           │ 156 / 1000    │ 58.7%       │ ✅ Available  │
│ data-hot-7  │ 450GB           │ 0 / 1000      │ 78.3%       │ ❌ Max shards │
└─────────────┴──────────────────┴───────────────┴─────────────┴──────────────┘
```

**Column Explanations:**
- **Node**: Candidate nodes in the same availability zone (master nodes are excluded)
- **Space Below Low WM**: Available disk space before hitting the low watermark threshold
- **Shard Capacity**: Current shard count vs. max_shards_per_node limit (e.g., "234 / 1000")
- **Disk Usage**: Current disk utilization percentage
- **Status**: Overall availability status for receiving shards

**Status Indicators:**
- `✅ Available`: Node can accept shards (has both disk space and shard capacity)
- `❌ No space`: Node has hit disk space watermarks
- `❌ Max shards`: Node has reached the max_shards_per_node limit
- `⚠️ High usage`: Node is approaching capacity limits (>90% disk usage)
- `❌ At capacity`: Node has multiple capacity constraints

### 4. Recovery Time Estimation

Provides estimates for how long the maintenance process will take:

```
┌─────────────────────────────────────────┐
│      ⏱️ Recovery Time Estimation         │
├─────────────────────────────────────────┤
│ Data Transfer Rate: 20 MB/s per stream  │
│ Concurrent Recovery Streams: 2          │
│ Effective Transfer Rate: 40 MB/s        │
│                                         │
│ Estimated Time: 8.5 hours              │
│                                         │
│ Factors affecting recovery time:        │
│ • Network bandwidth between nodes       │
│ • Disk I/O performance                 │
│ • Current cluster load                  │
│ • Number of concurrent operations       │
└─────────────────────────────────────────┘
```

**Time Calculation:**
- Based on cluster recovery settings (`indices.recovery.max_bytes_per_sec`)
- Considers concurrent recovery streams (`cluster.routing.allocation.node_concurrent_recoveries`)
- Provides realistic estimates for data movement duration

### 5. Next Steps and Recommendations

Provides actionable recommendations based on the analysis:

#### When Maintenance is Safe:

```
📋 Next Steps:

✅ MAINTENANCE APPEARS SAFE - Proceed with caution

Pre-maintenance checklist:
1. Verify cluster health: GREEN status required
2. Ensure no ongoing recoveries or reallocations
3. Check that no critical applications are running heavy queries
4. Consider maintenance window during low-traffic periods

Recommended maintenance procedure:
1. Disable shard allocation:
   ALTER CLUSTER SET "cluster.routing.allocation.enable" = 'primaries';

2. Safely stop the node:
   # Graceful shutdown allows ongoing operations to complete

3. Perform maintenance work

4. Restart the node and verify it rejoins the cluster

5. Re-enable shard allocation:
   ALTER CLUSTER SET "cluster.routing.allocation.enable" = 'all';
```

#### When Issues are Detected:

```
📋 Next Steps:

❌ MAINTENANCE NOT RECOMMENDED - Issues detected

Issues Found:
• Insufficient disk space: Need 500GB more capacity
• Shard capacity exceeded: 3 nodes at max_shards_per_node limit
• High cluster load: >90% disk usage on multiple nodes

Recommendations:
1. Free up disk space or add storage capacity
2. Increase max_shards_per_node setting:
   ALTER CLUSTER SET "cluster.max_shards_per_node" = 1500;
3. Add more data nodes to the cluster
4. Wait for current cluster load to decrease
5. Consider shard optimization:
   - Review partition strategies
   - Consolidate smaller tables
   - Optimize shard allocation settings
```

#### Shard Capacity Issues:

When nodes are approaching or have hit the `max_shards_per_node` limit:

```
Shard Capacity Solutions:
1. Increase the cluster-wide shard limit:
   ALTER CLUSTER SET "cluster.max_shards_per_node" = 2000;

2. Add more nodes to distribute shards:
   - Ensures better load distribution
   - Provides more shard capacity headroom

3. Optimize shard allocation:
   - Review tables with excessive shard counts
   - Consider consolidating small tables
   - Adjust partition strategies for time-series data

4. Monitor shard distribution:
   xmover shard-distribution --detailed
```

## Understanding Capacity Constraints

### Disk Space Constraints

The analysis considers CrateDB's disk watermark settings:

- **Low Watermark (default 85%)**: No new shards allocated to nodes above this threshold
- **High Watermark (default 90%)**: Shards relocated away from nodes above this threshold  
- **Flood Stage (default 95%)**: All allocations to the node are blocked

### Shard Count Constraints

CrateDB limits the number of shards per node via `cluster.max_shards_per_node` (default: 1000):

- Prevents nodes from becoming overloaded with too many small shards
- Each shard has memory and file handle overhead
- The limit applies to both primary and replica shards

### Availability Zone Constraints

Shard relocation only considers nodes in the same availability zone:

- Maintains data locality and network performance
- Respects rack awareness and failure domain isolation
- Master nodes are automatically excluded as shard targets

## Best Practices

### Before Running Maintenance

1. **Check Cluster Health**: Ensure cluster status is GREEN
   ```bash
   # Check overall cluster health
   SELECT health FROM sys.cluster;
   ```

2. **Verify No Ongoing Operations**: No active recoveries or reallocations
   ```bash
   # Check for ongoing shard operations
   SELECT * FROM sys.shards WHERE state != 'STARTED';
   ```

3. **Monitor Cluster Load**: Ensure low I/O and query load during maintenance

### Choosing Min-Availability Level

**Use `full` when:**
- Complete hardware replacement
- Long-duration maintenance (>24 hours)
- Node will be permanently removed
- Maximum safety is required

**Use `primaries` when:**
- Quick software updates or restarts
- Temporary maintenance (<4 hours)  
- Need to minimize data movement
- Node will return to service quickly

### Timing Considerations

- **Plan for Recovery Time**: Data movement takes time, especially for large datasets
- **Maintenance Windows**: Schedule during low-traffic periods
- **Network Impact**: Large data transfers can impact cluster performance
- **Cascading Effects**: Moving shards may trigger additional rebalancing

## Troubleshooting Common Issues

### "No candidate nodes found in same availability zone"

**Cause**: No other data nodes exist in the same zone, or all nodes are at capacity.

**Solutions:**
- Add more nodes to the availability zone
- Free up space on existing nodes
- Consider temporary cross-zone allocation (advanced)

### "Insufficient shard capacity"

**Cause**: Candidate nodes have reached `max_shards_per_node` limit.

**Solutions:**
- Increase `cluster.max_shards_per_node` setting
- Add more nodes to the cluster
- Optimize shard allocation (reduce shard count)

### "Capacity check failed"

**Cause**: Not enough disk space available on candidate nodes.

**Solutions:**
- Free up disk space (drop old data, optimize storage)
- Add storage capacity to existing nodes
- Add more nodes to the cluster

### High Recovery Time Estimates

**Cause**: Large amounts of data to transfer with limited bandwidth.

**Solutions:**
- Increase `indices.recovery.max_bytes_per_sec` (if network allows)
- Increase `cluster.routing.allocation.node_concurrent_recoveries`
- Schedule maintenance during low-traffic periods
- Consider using `primaries` mode instead of `full`

## Integration with Other Commands

The maintenance analysis works well with other `xmover` commands:

```bash
# Get detailed shard distribution before maintenance
xmover shard-distribution --detailed --node data-hot-4

# Check for problematic shards that might complicate maintenance
xmover problematic-translogs --threshold 100

# After maintenance, verify cluster health
xmover cluster-overview
```

## Advanced Configuration

### Recovery Performance Tuning

Before maintenance, you might want to adjust recovery settings:

```sql
-- Increase recovery bandwidth (if network supports it)
ALTER CLUSTER SET "indices.recovery.max_bytes_per_sec" = '100mb';

-- Increase concurrent recovery streams (if nodes can handle it)  
ALTER CLUSTER SET "cluster.routing.allocation.node_concurrent_recoveries" = 4;

-- After maintenance, consider reverting to defaults
ALTER CLUSTER SET "indices.recovery.max_bytes_per_sec" = '20mb';
ALTER CLUSTER SET "cluster.routing.allocation.node_concurrent_recoveries" = 2;
```

### Watermark Adjustments

For clusters with predictable space usage:

```sql
-- More aggressive space management
ALTER CLUSTER SET "cluster.routing.allocation.disk.watermark.low" = '80%';
ALTER CLUSTER SET "cluster.routing.allocation.disk.watermark.high" = '85%';
ALTER CLUSTER SET "cluster.routing.allocation.disk.watermark.flood_stage" = '90%';
```

## Limitations and Considerations

### Current Limitations

- Analysis is point-in-time (cluster state may change)
- Estimates assume steady-state performance (actual times may vary)
- Does not account for ongoing application queries during recovery
- Cross-zone analysis not currently supported

### Important Considerations

- **Network Bandwidth**: Large data transfers can impact application performance
- **Cluster Load**: Recovery competes with normal query processing
- **Cascading Effects**: Moving shards may trigger additional rebalancing across the cluster
- **Application Impact**: Some applications may experience higher latency during recovery

### Future Enhancements

Planned improvements include:
- Real-time progress monitoring during maintenance
- Cross-availability-zone analysis options
- Integration with cluster monitoring systems
- Automated maintenance orchestration
- Historical performance analysis for better time estimates

## Related Documentation

- [CrateDB Cluster Administration](https://crate.io/docs/crate/reference/en/latest/admin/)
- [Shard Allocation Settings](https://crate.io/docs/crate/reference/en/latest/admin/system-information.html#cluster-settings)
- [Recovery and Replication](https://crate.io/docs/crate/reference/en/latest/concepts/clustering.html)