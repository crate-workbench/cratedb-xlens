(index)=

# CrateDB XLens

Welcome to the XLens documentation! XLens is a comprehensive utility for
analyzing CrateDB clusters. It provides powerful commands for shard
analysis, maintenance planning, and cluster optimization.

## Getting Started

XLens is designed to help CrateDB administrators make informed decisions about cluster maintenance, shard distribution, and performance optimization.

### Installation

```bash
# Install from source
git clone <repository-url>
cd xmover
uv install
```

### Basic Usage

```bash
# Connect to your CrateDB cluster
export CRATEDB_HOST=your-cluster-host
export CRATEDB_PORT=4200

# Run XLens commands
xmover cluster-overview
```

## Core Features

### 🔧 Node Maintenance Planning

Comprehensive analysis for safe node decommissioning and maintenance operations.

- **Capacity Analysis**: Check if cluster can handle node maintenance
- **Impact Assessment**: Understand what data will be affected
- **Time Estimation**: Get realistic recovery time estimates
- **Safety Checks**: Validate cluster health before maintenance

{ref}`Read the Node Maintenance Guide → <node-maintenance>`

### 📊 Shard Distribution Analysis

Deep insights into how data is distributed across your cluster.

- **Distribution Visualization**: See how shards are spread across nodes
- **Balance Assessment**: Identify uneven distributions
- **Performance Impact**: Understand query performance implications
- **Optimization Recommendations**: Get actionable suggestions

### 🚨 Problematic Shard Detection

Identify and resolve issues with shard allocation and recovery.

- **Translog Analysis**: Find shards with problematic transaction logs
- **Recovery Issues**: Detect stuck or failed shard recoveries
- **Command Generation**: Get SQL commands to fix issues
- **Health Monitoring**: Ongoing cluster health assessment

### 📈 Cluster Overview

High-level cluster health and capacity monitoring.

- **Health Status**: Overall cluster health assessment
- **Capacity Tracking**: Disk space and shard utilization
- **Node Status**: Individual node health and performance
- **Trend Analysis**: Capacity and performance trends

### 🔍 Data Readability Monitoring

Continuous monitoring of cluster data availability and write activity.

- **Health Indicators**: Active/slow/stale table detection
- **Performance Tracking**: Query response time monitoring
- **Activity Analysis**: Track write patterns across largest tables
- **Reliability Testing**: Fresh connections and retry logic

[Read the Read-Check Guide →](read-check.md)

## Quick Reference

### Common Commands

```bash
# Check cluster health
xmover cluster-overview

# Plan node maintenance
xmover check-maintenance --node data-node-1 --min-availability full

# Analyze shard distribution
xmover shard-distribution --detailed

# Find problematic shards
xmover problematic-translogs --threshold 100
```

### Connection Options

```bash
# Using environment variables (recommended)
export CRATEDB_HOST=cluster.example.com
export CRATEDB_PORT=4200
export CRATEDB_USERNAME=admin
export CRATEDB_PASSWORD=secret

# Or using command line options
xmover --host cluster.example.com --port 4200 --user admin cluster-overview
```

## Documentation

### Detailed Guides

- **{ref}`Node Maintenance Guide <node-maintenance>`** - Complete guide to planning and executing node maintenance
- **{ref}`Quick Reference <quick-reference>`** - Cheat sheet for common maintenance operations

### Reference Materials

- **Command Reference** - Detailed documentation for all commands
- **Configuration Guide** - Setup and configuration options
- **Troubleshooting** - Common issues and solutions
- **Best Practices** - Recommended operational procedures

## Key Concepts

### Min-Availability Levels

**Full Mode**

- Moves all shards (primaries and replicas) off the target node
- Use for: Hardware replacement, permanent node removal
- Impact: Slower but allows complete node shutdown

**Primaries Mode**

- Ensures primary shards have replicas on other nodes
- Use for: Software updates, temporary maintenance
- Impact: Faster with minimal data movement

### Capacity Constraints

**Disk Space Watermarks**

- Low (85%): No new shard allocation
- High (90%): Active shard relocation
- Flood (95%): All allocation blocked

**Shard Count Limits**

- `max_shards_per_node`: Prevents shard overload
- Default: 1000 shards per node
- Includes both primary and replica shards

### Availability Zones

- Shard movement respects zone boundaries
- Master nodes excluded from shard placement
- Cross-zone operations require special consideration

## Status Indicators

### Capacity Checks

- ✅ **Sufficient**: Maintenance can proceed safely
- ❌ **Insufficient**: Capacity issues detected, see recommendations

### Node Status

- ✅ **Available**: Node can accept shards
- ❌ **No space**: Disk watermark limits exceeded
- ❌ **Max shards**: Shard count limit reached
- ⚠️ **High usage**: Approaching capacity limits (>90%)
- ❌ **At capacity**: Multiple constraints active

## Safety Guidelines

### Pre-Maintenance Checklist

- [ ] Cluster health is GREEN
- [ ] No ongoing shard recoveries
- [ ] Low cluster load/traffic
- [ ] Maintenance window scheduled
- [ ] Recovery time estimate acceptable
- [ ] Backup procedures verified

### Emergency Procedures

- Monitor recovery progress during maintenance
- Have rollback plans ready
- Keep emergency contacts available
- Document all actions taken

## Performance Tuning

### Recovery Optimization

```sql
-- Increase recovery bandwidth (if network supports)
ALTER CLUSTER SET "indices.recovery.max_bytes_per_sec" = '100mb';

-- Increase concurrent recovery streams
ALTER CLUSTER SET "cluster.routing.allocation.node_concurrent_recoveries" = 4;
```

### Monitoring Commands

```sql
-- Check cluster health
SELECT health FROM sys.cluster;

-- Monitor active shard operations
SELECT * FROM sys.shards WHERE state != 'STARTED';

-- Track recovery progress
SELECT table_name, id, routing_state
FROM sys.shards
WHERE routing_state IN ('RELOCATING', 'INITIALIZING');
```

## Integration

XLens integrates well with existing CrateDB monitoring and management tools:

- **Monitoring Systems**: Export metrics for Prometheus, Grafana
- **Automation**: Use in CI/CD pipelines and automation scripts
- **Alerting**: Integrate capacity checks with alerting systems
- **Documentation**: Generate reports for compliance and audit

## Support and Contributing

### Getting Help

- Check the troubleshooting guide for common issues
- Review examples for similar scenarios
- Consult the detailed documentation for specific commands

### Best Practices

- Always test in non-production environments first
- Schedule maintenance during low-traffic periods
- Monitor cluster performance during and after maintenance
- Document all maintenance activities
- Keep XLens updated to the latest version

### Contributing

- Report issues and bugs
- Suggest improvements and new features
- Submit documentation improvements
- Share usage examples and case studies

## Version Information

This documentation covers XLens features and functionality. For the latest updates and release notes, check the project repository.

---

**Quick Start**: Jump to the {ref}`quick-reference` for immediate help with common operations.

**Deep Dive**: Read the {ref}`node-maintenance` for comprehensive understanding.


:::{toctree}
:caption: Handbook
:maxdepth: 1
:hidden:
Quick reference <maintenance-quick-reference>
Node maintenance <node-maintenance>
Data readability monitor <read-check>
:::
:::{toctree}
:caption: Development
:maxdepth: 1
:hidden:
Sandbox <sandbox>
Changelog <changes>
Notes <notes/index>
:::
