# CrateDB XLens

:::{div} sd-text-muted
XLens is a comprehensive looking-glass utility for analyzing CrateDB
clusters. It provides powerful commands for shard analysis, maintenance
planning, and cluster optimization.
:::

## Features

- **Cluster analysis**: Complete overview of shard distribution across nodes and zones
- **Shard distribution analysis**: Detect and rank distribution anomalies across the largest tables
- **Shard movement recommendations**: Intelligent suggestions for rebalancing with safety validation
- **AutoExec replica reset**: Automated replica reset operations for problematic translog shards with dry-run safety
- **Recovery monitoring**: Track ongoing shard recovery operations with progress details
- **Cluster health monitoring**: Monitor data readability by sampling from largest tables
- **Zone conflict detection**: Prevents moves that would violate CrateDB's zone awareness
- **Node decommissioning**: Plan safe node removal with automated shard relocation
- **Dry-run mode**: Test recommendations without generating actual SQL commands
- **Safety validation**: Comprehensive checks to ensure data availability during moves

## Documentation

:::{toctree}
:caption: Install
:maxdepth: 1

Standalone <install/package>
Container <install/container>
Kubernetes <install/kubernetes>
install/configure
:::
:::{toctree}
:caption: Handbook
:maxdepth: 1

Overview <overview>
Troubleshooting <troubleshooting>
Node maintenance <maintenance/index>
Data readability monitor <read-check>
:::
:::{toctree}
:caption: Development
:maxdepth: 1

Sandbox <sandbox>
Changelog <changes>
oci
Notes <notes/index>
:::
