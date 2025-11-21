"""
XMover Shard Size Monitor

A comprehensive tool for analyzing CrateDB shard sizes and generating optimization recommendations
based on configurable rules. This module can be used standalone or integrated with other tools.
"""

import csv
import os
import sys
import yaml
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Union
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text


@dataclass
class ShardSizeRule:
    """Represents a single shard size analysis rule."""
    name: str
    category: str
    severity: str  # 'critical', 'warning', 'info'
    condition: str  # Python expression to evaluate
    recommendation: str  # Template string with variables
    action_hint: Optional[str] = None


@dataclass
class RuleViolation:
    """Represents a violated rule with context."""
    rule_name: str
    category: str
    severity: str
    recommendation: str
    action_hint: Optional[str]
    table_identifier: str  # schema.table[partition]


@dataclass
class ShardAnalysisResult:
    """Analysis results for a single table/partition."""
    # Raw data from query
    table_schema: str
    table_name: str
    partition_ident: Optional[str]
    total_primary_size_gb: float
    avg_shard_size_gb: float
    min_shard_size_gb: float
    max_shard_size_gb: float
    num_shards_primary: int
    num_shards_replica: int
    num_shards_total: int
    num_columns: int
    partitioned_by: Optional[str]
    clustered_by: Optional[str]
    total_documents: int

    # Analysis results
    violations: List[RuleViolation] = field(default_factory=list)

    @property
    def table_identifier(self) -> str:
        """Get human-readable table identifier."""
        base = f"{self.table_schema}.{self.table_name}"
        if self.partition_ident and self.partition_ident != '':
            return f"{base}[{self.partition_ident}]"
        return base

    @property
    def has_critical_violations(self) -> bool:
        """Check if there are any critical violations."""
        return any(v.severity == 'critical' for v in self.violations)

    @property
    def has_warnings(self) -> bool:
        """Check if there are any warning violations."""
        return any(v.severity == 'warning' for v in self.violations)


@dataclass
class ClusterConfiguration:
    """Cluster-level configuration and metrics."""
    total_nodes: int
    total_cpu_cores: int
    total_memory_gb: float
    total_heap_gb: float
    max_shards_per_node_setting: int
    actual_max_shards_per_node: int
    total_shards: int
    disk_watermark_low: Optional[float] = None
    disk_watermark_high: Optional[float] = None
    disk_watermark_flood_stage: Optional[float] = None


@dataclass
class MonitoringReport:
    """Complete analysis report."""
    timestamp: datetime
    cluster_config: ClusterConfiguration
    table_results: List[ShardAnalysisResult]
    cluster_violations: List[RuleViolation]

    @property
    def total_violations_by_severity(self) -> Dict[str, int]:
        """Count violations by severity level."""
        counts = {'critical': 0, 'warning': 0, 'info': 0}

        # Count table-level violations
        for result in self.table_results:
            for violation in result.violations:
                counts[violation.severity] += 1

        # Count cluster-level violations
        for violation in self.cluster_violations:
            counts[violation.severity] += 1

        return counts


class RulesConfigValidator:
    """Validates rules configuration files."""

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> List[str]:
        """Validate rules configuration and return list of errors."""
        errors = []

        # Check required top-level fields
        required_fields = ['metadata', 'thresholds', 'rules']
        for field in required_fields:
            if field not in config:
                errors.append(f"Missing required field: {field}")

        if 'validation' in config and 'rule_required_fields' in config['validation']:
            rule_required_fields = config['validation']['rule_required_fields']
        else:
            rule_required_fields = ['name', 'category', 'severity', 'condition', 'recommendation']

        # Validate individual rules
        if 'rules' in config:
            for i, rule in enumerate(config['rules']):
                for field in rule_required_fields:
                    if field not in rule:
                        errors.append(f"Rule {i}: Missing required field '{field}'")

                # Validate severity
                if 'severity' in rule:
                    valid_severities = config.get('validation', {}).get('valid_severities',
                                                                       ['critical', 'warning', 'info'])
                    if rule['severity'] not in valid_severities:
                        errors.append(f"Rule {i} ({rule.get('name', 'unnamed')}): "
                                    f"Invalid severity '{rule['severity']}'")

                # Try to compile condition as Python expression
                if 'condition' in rule:
                    try:
                        compile(rule['condition'], '<rule_condition>', 'eval')
                    except SyntaxError as e:
                        errors.append(f"Rule {i} ({rule.get('name', 'unnamed')}): "
                                    f"Invalid condition syntax: {e}")

        # Validate cluster rules if present
        if 'cluster_rules' in config:
            for i, rule in enumerate(config['cluster_rules']):
                for field in rule_required_fields:
                    if field not in rule:
                        errors.append(f"Cluster rule {i}: Missing required field '{field}'")

        return errors


class ShardSizeMonitor:
    """Main shard size monitoring and analysis class."""

    SHARD_ANALYSIS_QUERY = """
    WITH columns AS (
        SELECT table_schema,
               table_name,
               COUNT(*) AS num_columns
        FROM information_schema.columns
        GROUP BY ALL
    ), tables AS (
        SELECT table_schema,
               table_name,
               partitioned_by,
               clustered_by
        FROM information_schema.tables
    ), shards AS (
        SELECT schema_name AS table_schema,
               table_name,
               partition_ident,
               SUM(size) FILTER (WHERE primary = TRUE) / POWER(1024, 3) AS total_primary_size_gb,
               AVG(size) / POWER(1024, 3) AS avg_shard_size_gb,
               MIN(size) / POWER(1024, 3) AS min_shard_size_gb,
               MAX(size) / POWER(1024, 3) AS max_shard_size_gb,
               COUNT(*) FILTER (WHERE primary = TRUE) AS num_shards_primary,
               COUNT(*) FILTER (WHERE primary = FALSE) AS num_shards_replica,
               COUNT(*) AS num_shards_total,
               SUM(num_docs) AS total_documents
        FROM sys.shards
        GROUP BY ALL
    )
    SELECT s.*,
           num_columns,
           partitioned_by[1] AS partitioned_by,
           clustered_by
    FROM shards s
    JOIN columns c ON s.table_name = c.table_name AND s.table_schema = c.table_schema
    JOIN tables t ON s.table_name = t.table_name AND s.table_schema = t.table_schema
    ORDER BY table_schema, table_name, partition_ident
    """

    def __init__(self, db_client, rules_config_path: Optional[str] = None):
        """Initialize monitor with database client and rules configuration."""
        self.db_client = db_client
        self.console = Console()

        # Load rules configuration
        if rules_config_path is None:
            # Use default rules file
            current_dir = Path(__file__).parent.parent.parent
            rules_config_path = current_dir / "config" / "shard_size_rules.yaml"

        self.rules_config = self._load_rules_config(rules_config_path)
        self.thresholds = self.rules_config.get('thresholds', {})
        self.table_rules = [ShardSizeRule(**rule) for rule in self.rules_config.get('rules', [])]
        self.cluster_rules = [ShardSizeRule(**rule) for rule in self.rules_config.get('cluster_rules', [])]

    def _load_rules_config(self, config_path: Union[str, Path]) -> Dict[str, Any]:
        """Load and validate rules configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)

            # Validate configuration
            validator = RulesConfigValidator()
            errors = validator.validate_config(config)

            if errors:
                self.console.print("[red]Configuration validation errors:[/red]")
                for error in errors:
                    self.console.print(f"  • {error}")
                sys.exit(1)

            return config

        except FileNotFoundError:
            self.console.print(f"[red]Rules configuration file not found: {config_path}[/red]")
            sys.exit(1)
        except yaml.YAMLError as e:
            self.console.print(f"[red]Error parsing YAML configuration: {e}[/red]")
            sys.exit(1)

    def analyze_cluster_shard_sizes(self, schema_filter: Optional[str] = None) -> MonitoringReport:
        """Run complete shard size analysis."""
        self.console.print("🔍 Gathering cluster configuration...")
        cluster_config = self._gather_cluster_config()

        self.console.print("📊 Analyzing shard sizes and table schemas...")
        table_results = self._analyze_table_shards(cluster_config, schema_filter)

        self.console.print("✅ Applying analysis rules...")
        cluster_violations = self._evaluate_cluster_rules(cluster_config, table_results)

        return MonitoringReport(
            timestamp=datetime.now(),
            cluster_config=cluster_config,
            table_results=table_results,
            cluster_violations=cluster_violations
        )

    def _gather_cluster_config(self) -> ClusterConfiguration:
        """Gather cluster-level configuration and metrics."""
        # Get cluster nodes info
        nodes_query = """
        SELECT
            COUNT(*) as total_nodes,
            SUM(os_info['available_processors']) as total_cpu_cores,
            SUM(mem['used'] + mem['free']) / POWER(1024, 3) as total_memory_gb,
            SUM(heap['max']) / POWER(1024, 3) as total_heap_gb
        FROM sys.nodes
        WHERE name IS NOT NULL
        """
        nodes_result = self.db_client.execute_query(nodes_query)
        nodes_data = nodes_result.get('rows', [])[0]

        # Get cluster settings - use default if sys.cluster is not accessible
        max_shards_setting = 1000  # CrateDB default

        try:
            settings_query = """
            SELECT settings['cluster']['max_shards_per_node'] as max_shards_per_node
            FROM sys.cluster
            """
            settings_result = self.db_client.execute_query(settings_query)

            rows = settings_result.get('rows', [])
            if rows and rows[0][0] is not None:
                max_shards_setting = int(rows[0][0])
        except Exception as e:
            # sys.cluster might not be accessible in CrateDB Cloud
            self.console.print(f"[yellow]Warning: Could not access cluster settings, using default max_shards_per_node=1000[/yellow]")

        # Get total shard count and max shards per node
        shards_query = """
        SELECT
            COUNT(*) as total_shards
        FROM sys.shards
        """
        shards_result = self.db_client.execute_query(shards_query)
        shards_data = shards_result.get('rows', [])[0]

        # Get actual max shards per node (current distribution)
        try:
            max_shards_query = """
            SELECT COALESCE(node['name'], 'unknown-' || COALESCE(node['id'], 'corrupted')), COUNT(*) as shard_count
            FROM sys.shards
            GROUP BY COALESCE(node['name'], 'unknown-' || COALESCE(node['id'], 'corrupted'))
            ORDER BY shard_count DESC
            LIMIT 1
            """
            max_shards_result = self.db_client.execute_query(max_shards_query)
            max_shards_rows = max_shards_result.get('rows', [])
            actual_max_shards_per_node = max_shards_rows[0][1] if max_shards_rows else 0
        except Exception as e:
            # Calculate approximate value: total_shards / total_nodes
            actual_max_shards_per_node = int(shards_data[0] / max(nodes_data[0], 1))
            self.console.print(f"[dim]Using approximate max shards per node: {actual_max_shards_per_node}[/dim]")

        return ClusterConfiguration(
            total_nodes=nodes_data[0],
            total_cpu_cores=nodes_data[1] or 0,
            total_memory_gb=nodes_data[2] or 0.0,
            total_heap_gb=nodes_data[3] or 0.0,
            max_shards_per_node_setting=max_shards_setting,
            actual_max_shards_per_node=actual_max_shards_per_node,
            total_shards=shards_data[0]
        )

    def _analyze_table_shards(self, cluster_config: ClusterConfiguration,
                            schema_filter: Optional[str] = None) -> List[ShardAnalysisResult]:
        """Analyze individual table shard configurations."""
        query = self.SHARD_ANALYSIS_QUERY

        if schema_filter:
            # Add WHERE clause for schema filtering
            query = query.replace(
                "ORDER BY table_schema",
                f"WHERE s.table_schema = '{schema_filter}' ORDER BY table_schema"
            )

        results = self.db_client.execute_query(query)

        table_results = []
        for row in results.get('rows', []):
            # Parse query results
            analysis_result = ShardAnalysisResult(
                table_schema=row[0],
                table_name=row[1],
                partition_ident=row[2],
                total_primary_size_gb=float(row[3] or 0),
                avg_shard_size_gb=float(row[4] or 0),
                min_shard_size_gb=float(row[5] or 0),
                max_shard_size_gb=float(row[6] or 0),
                num_shards_primary=int(row[7] or 0),
                num_shards_replica=int(row[8] or 0),
                num_shards_total=int(row[9] or 0),
                total_documents=int(row[10] or 0),
                num_columns=int(row[11] or 0),
                partitioned_by=row[12],
                clustered_by=row[13]
            )

            # Evaluate rules for this table
            analysis_result.violations = self._evaluate_table_rules(analysis_result, cluster_config)
            table_results.append(analysis_result)

        return table_results

    def _evaluate_table_rules(self, result: ShardAnalysisResult,
                            cluster_config: ClusterConfiguration) -> List[RuleViolation]:
        """Evaluate table-level rules against a single table/partition."""
        violations = []

        # Prepare evaluation context
        context = {
            # Table data
            'table_schema': result.table_schema,
            'table_name': result.table_name,
            'partition_ident': result.partition_ident,
            'total_primary_size_gb': result.total_primary_size_gb,
            'avg_shard_size_gb': result.avg_shard_size_gb,
            'min_shard_size_gb': result.min_shard_size_gb,
            'max_shard_size_gb': result.max_shard_size_gb,
            'num_shards_primary': result.num_shards_primary,
            'num_shards_replica': result.num_shards_replica,
            'num_shards_total': result.num_shards_total,
            'num_columns': result.num_columns,
            'partitioned_by': result.partitioned_by,
            'clustered_by': result.clustered_by,

            # Cluster context
            'cluster_config': {
                'total_nodes': cluster_config.total_nodes,
                'total_cpu_cores': cluster_config.total_cpu_cores,
                'total_memory_gb': cluster_config.total_memory_gb,
                'total_heap_gb': cluster_config.total_heap_gb,
                'max_shards_per_node': cluster_config.max_shards_per_node_setting,
                'total_shards': cluster_config.total_shards
            },

            # Thresholds
            'thresholds': self.thresholds
        }

        # Evaluate each rule
        for rule in self.table_rules:
            try:
                if eval(rule.condition, {"__builtins__": {}}, context):
                    # Create formatting context with flattened values
                    format_context = {
                        **context,
                        **self.thresholds,
                        'ratio': context['max_shard_size_gb'] / context['min_shard_size_gb'] if context['min_shard_size_gb'] > 0 else 0
                    }
                    # Add flattened cluster_config values for easier formatting
                    for key, value in context['cluster_config'].items():
                        format_context[f'cluster_config[{key}]'] = value

                    recommendation = rule.recommendation.format(**format_context)

                    violations.append(RuleViolation(
                        rule_name=rule.name,
                        category=rule.category,
                        severity=rule.severity,
                        recommendation=recommendation,
                        action_hint=rule.action_hint,
                        table_identifier=result.table_identifier
                    ))
            except Exception as e:
                self.console.print(f"[yellow]Warning: Error evaluating rule '{rule.name}': {e}[/yellow]")

        return violations

    def _evaluate_cluster_rules(self, cluster_config: ClusterConfiguration,
                               table_results: List[ShardAnalysisResult]) -> List[RuleViolation]:
        """Evaluate cluster-level rules."""
        violations = []

        # Prepare cluster-level context
        context = {
            'cluster_config': {
                'total_nodes': cluster_config.total_nodes,
                'total_cpu_cores': cluster_config.total_cpu_cores,
                'total_memory_gb': cluster_config.total_memory_gb,
                'total_heap_gb': cluster_config.total_heap_gb,
                'max_shards_per_node': cluster_config.actual_max_shards_per_node,
                'total_shards': cluster_config.total_shards
            },
            'thresholds': self.thresholds,
            'total_shards': cluster_config.total_shards,
            'total_heap_gb': cluster_config.total_heap_gb,
            'max_shards_per_node': cluster_config.actual_max_shards_per_node,
            'total_cpu_cores': cluster_config.total_cpu_cores
        }

        # Evaluate each cluster rule
        for rule in self.cluster_rules:
            try:
                if eval(rule.condition, {"__builtins__": {}}, context):
                    # Create formatting context with flattened values
                    format_context = {
                        **context,
                        **self.thresholds
                    }
                    # Add flattened cluster_config values for easier formatting
                    for key, value in context['cluster_config'].items():
                        format_context[f'cluster_config[{key}]'] = value

                    recommendation = rule.recommendation.format(**format_context)

                    violations.append(RuleViolation(
                        rule_name=rule.name,
                        category=rule.category,
                        severity=rule.severity,
                        recommendation=recommendation,
                        action_hint=rule.action_hint,
                        table_identifier="[CLUSTER]"
                    ))
            except Exception as e:
                self.console.print(f"[yellow]Warning: Error evaluating cluster rule '{rule.name}': {e}[/yellow]")

        return violations

    def display_report(self, report: MonitoringReport, severity_filter: Optional[str] = None):
        """Display analysis report to console."""
        # Header with cluster info
        self.console.print(Panel(
            f"[bold blue]CrateDB Shard Size Analysis Report[/bold blue]\n"
            f"[dim]Generated: {report.timestamp.strftime('%Y-%m-%d %H:%M:%S')}[/dim]\n\n"
            f"[bold]Cluster Overview:[/bold]\n"
            f"• Nodes: {report.cluster_config.total_nodes}\n"
            f"• Total Shards: {report.cluster_config.total_shards}\n"
            f"• CPU Cores: {report.cluster_config.total_cpu_cores}\n"
            f"• Heap Memory: {report.cluster_config.total_heap_gb:.1f}GB\n"
            f"• Max Shards/Node: {report.cluster_config.actual_max_shards_per_node} "
            f"(limit: {report.cluster_config.max_shards_per_node_setting})",
            expand=False
        ))

        # Summary of violations
        violation_counts = report.total_violations_by_severity
        if any(violation_counts.values()):
            summary_text = Text()
            if violation_counts['critical'] > 0:
                summary_text.append(f"🔴 {violation_counts['critical']} Critical  ", style="bold red")
            if violation_counts['warning'] > 0:
                summary_text.append(f"🟡 {violation_counts['warning']} Warning  ", style="bold yellow")
            if violation_counts['info'] > 0:
                summary_text.append(f"🔵 {violation_counts['info']} Info", style="bold blue")

            self.console.print(Panel(summary_text, title="Issue Summary"))
        else:
            self.console.print(Panel("✅ No issues found", style="green"))
            return

        # Cluster-level violations
        cluster_violations = [v for v in report.cluster_violations
                            if not severity_filter or v.severity == severity_filter]
        if cluster_violations:
            self.console.print("\n[bold]🏢 Cluster-Level Issues:[/bold]")
            for violation in cluster_violations:
                severity_color = {'critical': 'red', 'warning': 'yellow', 'info': 'blue'}[violation.severity]
                self.console.print(f"[{severity_color}]• [{violation.severity.upper()}] {violation.recommendation}[/{severity_color}]")
                if violation.action_hint:
                    self.console.print(f"  💡 {violation.action_hint}")

        # Table-level violations
        tables_with_violations = [r for r in report.table_results if r.violations]
        if severity_filter:
            tables_with_violations = [r for r in tables_with_violations
                                    if any(v.severity == severity_filter for v in r.violations)]

        if tables_with_violations:
            self.console.print(f"\n[bold]📊 Table/Partition Issues ({len(tables_with_violations)} affected):[/bold]")

            for result in tables_with_violations:
                violations_to_show = [v for v in result.violations
                                    if not severity_filter or v.severity == severity_filter]

                if not violations_to_show:
                    continue

                # Table header with key metrics
                table_info = (f"{result.table_identifier} "
                            f"({result.num_shards_primary}s/{result.num_shards_replica}r, "
                            f"{result.max_shard_size_gb:.1f}GB max, "
                            f"avg {result.avg_shard_size_gb:.1f}GB, "
                            f"{result.total_documents:,} docs, "
                            f"{result.num_columns} cols)")

                self.console.print(f"\n[bold cyan]{table_info}[/bold cyan]")

                for violation in violations_to_show:
                    severity_color = {'critical': 'red', 'warning': 'yellow', 'info': 'blue'}[violation.severity]
                    self.console.print(f"  [{severity_color}]• [{violation.severity.upper()}] {violation.recommendation}[/{severity_color}]")
                    if violation.action_hint:
                        self.console.print(f"    💡 {violation.action_hint}")

    def export_csv(self, report: MonitoringReport, filename: str):
        """Export analysis results to CSV file."""
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = [
                'timestamp', 'violation_level', 'table_schema', 'table_name', 'partition_ident',
                'severity', 'category', 'rule_name', 'recommendation', 'action_hint',
                'total_primary_size_gb', 'avg_shard_size_gb', 'min_shard_size_gb', 'max_shard_size_gb',
                'num_shards_primary', 'num_shards_replica', 'num_shards_total', 'num_columns', 'total_documents'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            # Write cluster-level violations
            for violation in report.cluster_violations:
                writer.writerow({
                    'timestamp': report.timestamp.isoformat(),
                    'violation_level': 'cluster',
                    'table_schema': None,
                    'table_name': None,
                    'partition_ident': None,
                    'severity': violation.severity,
                    'category': violation.category,
                    'rule_name': violation.rule_name,
                    'recommendation': violation.recommendation,
                    'action_hint': violation.action_hint,
                    'total_primary_size_gb': None,
                    'avg_shard_size_gb': None,
                    'min_shard_size_gb': None,
                    'max_shard_size_gb': None,
                    'num_shards_primary': None,
                    'num_shards_replica': None,
                    'num_shards_total': None,
                    'num_columns': None,
                    'total_documents': None
                })

            # Write table-level violations
            for result in report.table_results:
                if result.violations:
                    for violation in result.violations:
                        writer.writerow({
                            'timestamp': report.timestamp.isoformat(),
                            'violation_level': 'table',
                            'table_schema': result.table_schema,
                            'table_name': result.table_name,
                            'partition_ident': result.partition_ident,
                            'severity': violation.severity,
                            'category': violation.category,
                            'rule_name': violation.rule_name,
                            'recommendation': violation.recommendation,
                            'action_hint': violation.action_hint,
                            'total_primary_size_gb': result.total_primary_size_gb,
                            'avg_shard_size_gb': result.avg_shard_size_gb,
                            'min_shard_size_gb': result.min_shard_size_gb,
                            'max_shard_size_gb': result.max_shard_size_gb,
                            'num_shards_primary': result.num_shards_primary,
                            'num_shards_replica': result.num_shards_replica,
                            'num_shards_total': result.num_shards_total,
                            'num_columns': result.num_columns,
                            'total_documents': result.total_documents
                        })
                else:
                    # Include tables without violations for complete dataset
                    writer.writerow({
                        'timestamp': report.timestamp.isoformat(),
                        'violation_level': 'table',
                        'table_schema': result.table_schema,
                        'table_name': result.table_name,
                        'partition_ident': result.partition_ident,
                        'severity': None,
                        'category': None,
                        'rule_name': None,
                        'recommendation': None,
                        'action_hint': None,
                        'total_primary_size_gb': result.total_primary_size_gb,
                        'avg_shard_size_gb': result.avg_shard_size_gb,
                        'min_shard_size_gb': result.min_shard_size_gb,
                        'max_shard_size_gb': result.max_shard_size_gb,
                        'num_shards_primary': result.num_shards_primary,
                        'num_shards_replica': result.num_shards_replica,
                        'num_shards_total': result.num_shards_total,
                        'num_columns': result.num_columns,
                        'total_documents': result.total_documents
                    })


def validate_rules_file(config_path: str) -> bool:
    """Standalone function to validate a rules configuration file."""
    console = Console()

    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        validator = RulesConfigValidator()
        errors = validator.validate_config(config)

        if errors:
            console.print(f"[red]❌ Validation failed for {config_path}:[/red]")
            for error in errors:
                console.print(f"  • {error}")
            return False
        else:
            console.print(f"[green]✅ Configuration file {config_path} is valid[/green]")
            return True

    except FileNotFoundError:
        console.print(f"[red]❌ File not found: {config_path}[/red]")
        return False
    except yaml.YAMLError as e:
        console.print(f"[red]❌ YAML parsing error: {e}[/red]")
        return False
    except Exception as e:
        console.print(f"[red]❌ Unexpected error: {e}[/red]")
        return False
