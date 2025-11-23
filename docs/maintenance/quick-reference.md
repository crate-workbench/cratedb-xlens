(quick-reference)=

# Node maintenance » Quick reference

## Command syntax

```bash
xmover check-maintenance --node <node-name> --min-availability <level>
```

## Parameters

| Parameter | Required | Options | Description |
|-----------|----------|---------|-------------|
| `--node` | Yes | Node name | Target node to analyze for maintenance |
| `--min-availability` | Yes | `full` \| `primaries` | Minimum data availability level |

## Min-availability levels

| Level | Data Movement | Speed | Use Case |
|-------|--------------|-------|----------|
| `full` | All shards (primaries + replicas) | Slower | Hardware replacement, permanent removal |
| `primaries` | Only primaries without replicas | Faster | Software updates, temporary maintenance |

## Quick commands

```bash
# Full maintenance analysis
xmover check-maintenance --node data-hot-4 --min-availability full

# Fast maintenance analysis
xmover check-maintenance --node data-hot-4 --min-availability primaries
```

## Status indicators

### Capacity check (summary)
- `✅ Sufficient` - Maintenance can proceed safely
- `❌ Insufficient` - Not enough capacity, see recommendations

### Node status (target nodes table)
- `✅ Available` - Node can accept shards
- `❌ No space` - Node at disk watermark limit
- `❌ Max shards` - Node at max_shards_per_node limit
- `⚠️ High usage` - Node approaching limits (>90% disk)
- `❌ At capacity` - Multiple constraints active

## Pre-maintenance checklist

- [ ] Cluster health is GREEN
- [ ] No ongoing shard recoveries
- [ ] Low cluster load/traffic
- [ ] Maintenance window scheduled
- [ ] Recovery time estimate acceptable

## Common issues & solutions

### Issue: "No candidate nodes found"
**Solution**: Add nodes to same availability zone or free up space

### Issue: "❌ Max shards" status
**Solution**:
```sql
ALTER CLUSTER SET "cluster.max_shards_per_node" = 1500;
```

### Issue: "❌ Insufficient" capacity
**Solutions**:
- Free up disk space
- Increase disk space
- Add more nodes
- Use `primaries` instead of `full` mode

### Issue: High recovery time estimates
**Solutions**:
```sql
-- Increase recovery bandwidth
ALTER CLUSTER SET "indices.recovery.max_bytes_per_sec" = '100mb';

-- Increase concurrent streams
ALTER CLUSTER SET "cluster.routing.allocation.node_concurrent_recoveries" = 4;
```

## Monitoring commands

```bash
# Check cluster health
SELECT health FROM sys.cluster;

# Check active shard operations
SELECT * FROM sys.shards WHERE state != 'STARTED';

# Monitor recovery progress
SELECT table_name, id, state, routing_state
FROM sys.shards
WHERE routing_state IN ('RELOCATING', 'INITIALIZING');
```

## Performance tuning

### Before maintenance
```sql
-- Increase recovery performance (if network supports)
ALTER CLUSTER SET "indices.recovery.max_bytes_per_sec" = '100mb';
ALTER CLUSTER SET "cluster.routing.allocation.node_concurrent_recoveries" = 4;
```

### After maintenance
```sql
-- Restore defaults
ALTER CLUSTER SET "indices.recovery.max_bytes_per_sec" = '20mb';
ALTER CLUSTER SET "cluster.routing.allocation.node_concurrent_recoveries" = 2;
```

## Integration with other commands

```bash
# Detailed shard analysis before maintenance
xmover shard-distribution --detailed --node data-hot-4

# Check for problematic shards
xmover problematic-translogs --threshold 100

# Post-maintenance cluster overview
xmover test-connection --verbose
```

## Emergency recovery

If maintenance goes wrong:

1. **Check cluster status**:
   ```bash
   xmover test-connection --verbose
   ```

2. **Force allocation if needed**:
   ```sql
   ALTER CLUSTER SET "cluster.routing.allocation.enable" = 'all';
   ```

3. **Manual shard recovery**:
   ```sql
   -- Force retry failed shards
   ALTER CLUSTER REROUTE RETRY FAILED;
   ```

4. **Monitor recovery**:
   ```bash
   # Watch shard states
   SELECT node['name'], table_name, state, routing_state
   FROM sys.shards
   WHERE state != 'STARTED'
   ORDER BY node['name'];
   ```
