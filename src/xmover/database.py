"""
Database connection and query functions for CrateDB
"""

import os
import json
import requests
import warnings
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class NodeInfo:
    """Information about a CrateDB node"""
    id: str
    name: str
    zone: str
    heap_used: int
    heap_max: int
    fs_total: int
    fs_used: int
    fs_available: int
    
    @property
    def heap_usage_percent(self) -> float:
        return (self.heap_used / self.heap_max) * 100 if self.heap_max > 0 else 0
    
    @property
    def disk_usage_percent(self) -> float:
        return (self.fs_used / self.fs_total) * 100 if self.fs_total > 0 else 0
    
    @property
    def available_space_gb(self) -> float:
        return self.fs_available / (1024**3)


@dataclass
class ShardInfo:
    """Information about a shard"""
    table_name: str
    schema_name: str
    shard_id: int
    node_id: str
    node_name: str
    zone: str
    is_primary: bool
    size_bytes: int
    size_gb: float
    num_docs: int
    state: str
    routing_state: str
    partition_ident: Optional[str] = None  # CRITICAL FIX: Add partition support
    partition_values: Optional[str] = None  # Human-readable partition values
    
    @property
    def shard_type(self) -> str:
        return "PRIMARY" if self.is_primary else "REPLICA"
    
    @property
    def full_table_identifier(self) -> str:
        """Unique table identifier including partition"""
        base = f"{self.schema_name}.{self.table_name}" if self.schema_name != "doc" else self.table_name
        if self.partition_values and self.partition_values.strip():
            return f"{base}[{self.partition_values}]"
        elif self.partition_ident and self.partition_ident.strip():
            return f"{base}[{self.partition_ident}]"
        else:
            return base
    
    @property
    def unique_shard_key(self) -> str:
        """Unique identifier for this specific shard including partition"""
        return f"{self.full_table_identifier}:shard_{self.shard_id}:{'P' if self.is_primary else 'R'}"


@dataclass
class RecoveryInfo:
    """Information about an active shard recovery"""
    schema_name: str
    table_name: str
    partition_values: Optional[str]  # Partition values for partitioned tables
    shard_id: int
    node_name: str
    node_id: str
    recovery_type: str  # PEER, DISK, etc.
    stage: str  # INIT, INDEX, VERIFY_INDEX, TRANSLOG, FINALIZE, DONE
    files_percent: float
    bytes_percent: float
    total_time_ms: int
    routing_state: str  # INITIALIZING, RELOCATING, etc.
    current_state: str  # from allocations
    is_primary: bool
    size_bytes: int
    source_node_name: Optional[str] = None  # Source node for PEER recoveries
    translog_size_bytes: int = 0  # Translog size in bytes
    translog_uncommitted_bytes: int = 0  # Translog uncommitted size in bytes
    max_seq_no: Optional[int] = None  # Sequence number for this shard
    primary_max_seq_no: Optional[int] = None  # Primary shard's sequence number for replica progress
    
    @property
    def overall_progress(self) -> float:
        """Calculate overall progress percentage"""
        return max(self.files_percent, self.bytes_percent)
    
    @property
    def size_gb(self) -> float:
        """Size in GB"""
        return self.size_bytes / (1024**3)
    
    @property
    def shard_type(self) -> str:
        return "PRIMARY" if self.is_primary else "REPLICA"
    
    @property
    def total_time_seconds(self) -> float:
        """Total time in seconds"""
        return self.total_time_ms / 1000.0
    
    @property
    def translog_size_gb(self) -> float:
        """Translog size in GB"""
        return self.translog_size_bytes / (1024**3)
    
    @property
    def translog_uncommitted_gb(self) -> float:
        """Translog uncommitted size in GB"""
        return self.translog_uncommitted_bytes / (1024**3)
    
    @property
    def translog_percentage(self) -> float:
        """Translog size as percentage of shard size"""
        return (self.translog_size_bytes / self.size_bytes * 100) if self.size_bytes > 0 else 0
    
    @property
    def translog_uncommitted_percentage(self) -> float:
        """Translog uncommitted size as percentage of total translog size"""
        return (self.translog_uncommitted_bytes / self.translog_size_bytes * 100) if self.translog_size_bytes > 0 else 0
    
    @property
    def seq_no_progress(self) -> Optional[float]:
        """Calculate replica progress based on sequence numbers (for replica shards only)"""
        if not self.is_primary and self.max_seq_no is not None and self.primary_max_seq_no is not None:
            if self.primary_max_seq_no == 0:
                return 100.0  # No operations on primary yet
            return min((self.max_seq_no / self.primary_max_seq_no * 100.0), 100.0)
        return None


@dataclass
class ActiveShardSnapshot:
    """Snapshot of active shard checkpoint data for tracking activity"""
    schema_name: str
    table_name: str
    shard_id: int
    node_name: str
    is_primary: bool
    partition_ident: str
    local_checkpoint: int
    global_checkpoint: int
    translog_uncommitted_bytes: int
    timestamp: float  # Unix timestamp when snapshot was taken
    
    @property
    def checkpoint_delta(self) -> int:
        """Current checkpoint delta (local - global)"""
        return self.local_checkpoint - self.global_checkpoint
    
    @property
    def translog_uncommitted_mb(self) -> float:
        """Translog uncommitted size in MB"""
        return self.translog_uncommitted_bytes / (1024 * 1024)
    
    @property
    def shard_identifier(self) -> str:
        """Unique identifier for this shard including partition"""
        shard_type = "P" if self.is_primary else "R"
        partition = f":{self.partition_ident}" if self.partition_ident else ""
        return f"{self.schema_name}.{self.table_name}:{self.shard_id}:{self.node_name}:{shard_type}{partition}"


@dataclass
class ActiveShardActivity:
    """Activity comparison between two snapshots of the same shard"""
    schema_name: str
    table_name: str
    shard_id: int
    node_name: str
    is_primary: bool
    partition_ident: str
    local_checkpoint_delta: int  # Change in local checkpoint between snapshots
    snapshot1: ActiveShardSnapshot
    snapshot2: ActiveShardSnapshot
    time_diff_seconds: float
    
    @property
    def activity_rate(self) -> float:
        """Activity rate as checkpoint changes per second"""
        if self.time_diff_seconds > 0:
            return self.local_checkpoint_delta / self.time_diff_seconds
        return 0.0
    
    @property
    def shard_type(self) -> str:
        return "PRIMARY" if self.is_primary else "REPLICA"
    
    @property
    def table_identifier(self) -> str:
        return f"{self.schema_name}.{self.table_name}"


class CrateDBClient:
    """Client for connecting to CrateDB and executing queries"""
    
    def __init__(self, connection_string: Optional[str] = None):
        load_dotenv()
        
        self.connection_string = connection_string or os.getenv('CRATE_CONNECTION_STRING')
        if not self.connection_string:
            raise ValueError("CRATE_CONNECTION_STRING not found in environment or provided")
        
        self.username = os.getenv('CRATE_USERNAME')
        self.password = os.getenv('CRATE_PASSWORD')
        
        # Auto-disable SSL verification for localhost connections
        is_localhost = 'localhost' in self.connection_string or '127.0.0.1' in self.connection_string
        ssl_verify_env = os.getenv('CRATE_SSL_VERIFY', 'true').lower()
        
        # Default to false for localhost, true for remote connections
        if ssl_verify_env == 'auto':
            self.ssl_verify = not is_localhost
        else:
            self.ssl_verify = ssl_verify_env == 'true'
        
        # For localhost, disable SSL verification by default unless explicitly enabled
        if is_localhost and ssl_verify_env == 'true' and os.getenv('CRATE_SSL_VERIFY') is None:
            self.ssl_verify = False
        
        # Suppress SSL warnings when SSL verification is disabled
        if not self.ssl_verify:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        
        # Ensure connection string ends with _sql endpoint
        if not self.connection_string.endswith('/_sql'):
            self.connection_string = self.connection_string.rstrip('/') + '/_sql'
    
    def execute_query(self, query: str, parameters: Optional[List] = None) -> Dict[str, Any]:
        """Execute a SQL query against CrateDB"""
        payload = {
            'stmt': query
        }
        
        if parameters:
            payload['args'] = parameters
        
        # Handle authentication - only use auth if both username and password are provided
        # For CrateDB, username without password should not use auth
        auth = None
        if self.username and self.password:
            auth = (self.username, self.password)
        elif self.username and not self.password:
            # For CrateDB 'crate' user without password, don't use auth
            auth = None
        
        try:
            response = requests.post(
                self.connection_string,
                json=payload,
                auth=auth,
                verify=self.ssl_verify,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.SSLError as e:
            # Provide helpful SSL error message for localhost connections
            if 'localhost' in self.connection_string or '127.0.0.1' in self.connection_string:
                raise Exception(f"SSL certificate error for localhost connection. "
                              f"Try setting CRATE_SSL_VERIFY=false in your .env file. Error: {e}")
            else:
                raise Exception(f"SSL error: {e}")
        except requests.exceptions.ConnectionError as e:
            raise Exception(f"Connection error - check if CrateDB is running and accessible: {e}")
        except requests.exceptions.RequestException as e:
            raise Exception(f"Failed to execute query: {e}")
    
    def get_nodes_info(self) -> List[NodeInfo]:
        """Get information about all nodes in the cluster with robust error handling"""
        nodes = []
        nodes_with_missing_metadata = []
        
        # First, get list of all node names
        try:
            name_query = "SELECT id, name FROM sys.nodes WHERE name IS NOT NULL ORDER BY name"
            name_result = self.execute_query(name_query)
        except Exception:
            return nodes
        
        # Process each node individually to handle corrupted metadata gracefully
        for row in name_result.get('rows', []):
            node_id, node_name = row
            
            try:
                # Try to get full node information
                detailed_query = """
                SELECT 
                    id,
                    name,
                    COALESCE(attributes['zone'], 'unknown') as zone,
                    COALESCE(heap['used'], 0) as heap_used,
                    COALESCE(heap['max'], 1) as heap_max,
                    COALESCE(fs['total']['size'], 0) as fs_total,
                    COALESCE(fs['total']['used'], 0) as fs_used,
                    COALESCE(fs['total']['available'], 0) as fs_available
                FROM sys.nodes 
                WHERE name = ?
                """
                
                detailed_result = self.execute_query(detailed_query, [node_name])
                
                if detailed_result.get('rows'):
                    detail_row = detailed_result['rows'][0]
                    nodes.append(NodeInfo(
                        id=detail_row[0],
                        name=detail_row[1],
                        zone=detail_row[2] or 'unknown',
                        heap_used=detail_row[3] or 0,
                        heap_max=detail_row[4] or 0,
                        fs_total=detail_row[5] or 0,
                        fs_used=detail_row[6] or 0,
                        fs_available=detail_row[7] or 0
                    ))
                else:
                    raise Exception("No detailed data available")
                    
            except Exception:
                # Fallback: create node with default values for corrupted metadata
                nodes_with_missing_metadata.append(node_name)
                nodes.append(NodeInfo(
                    id=node_id,
                    name=node_name,
                    zone='unknown',
                    heap_used=0,
                    heap_max=1,
                    fs_total=0,
                    fs_used=0,
                    fs_available=0
                ))
        
        # Log nodes with missing metadata if any
        if nodes_with_missing_metadata:
            print(f"⚠️  Warning: {len(nodes_with_missing_metadata)} node(s) have corrupted/missing metadata:")
            for node_name in nodes_with_missing_metadata:
                print(f"   • {node_name}: Using default values (heap, filesystem, zone data unavailable)")
            print("   💡 This may indicate node issues - check CrateDB logs for details")
        
        return nodes
    
    def get_shards_info(self, table_name: Optional[str] = None, 
                       min_size_gb: Optional[float] = None,
                       max_size_gb: Optional[float] = None,
                       for_analysis: bool = False) -> List[ShardInfo]:
        """Get information about shards, optionally filtered by table and size
        
        Args:
            table_name: Filter by specific table
            min_size_gb: Minimum shard size in GB
            max_size_gb: Maximum shard size in GB
            for_analysis: If True, includes all shards regardless of state (for cluster analysis)
                         If False, only includes healthy shards suitable for operations
        """
        
        where_conditions = []
        if not for_analysis:
            # For operations, only include healthy shards
            where_conditions.extend([
                "s.routing_state = 'STARTED'",
                "s.recovery['files']['percent'] = 100.0"
            ])
        parameters = []
        
        if table_name:
            where_conditions.append("s.table_name = ?")
            parameters.append(table_name)
        
        if min_size_gb is not None:
            where_conditions.append("s.size >= ?")
            parameters.append(int(min_size_gb * 1024**3))  # Convert GB to bytes
        
        if max_size_gb is not None:
            where_conditions.append("s.size <= ?")
            parameters.append(int(max_size_gb * 1024**3))  # Convert GB to bytes
        
        where_clause = ""
        if where_conditions:
            where_clause = f"WHERE {' AND '.join(where_conditions)}"
        
        query = f"""
        SELECT 
            s.table_name,
            s.schema_name,
            s.id as shard_id,
            s.partition_ident,           -- CRITICAL FIX: Include partition info
            translate(p.values::text, ':{{{{}}}}', '=()') as partition_values,
            COALESCE(s.node['id'], 'corrupted') as node_id,
            COALESCE(s.node['name'], 'unknown-' || COALESCE(s.node['id'], 'corrupted')) as node_name,
            COALESCE(n.attributes['zone'], 'unknown') as zone,
            s."primary" as is_primary,
            s.size as size_bytes,
            s.size / 1024.0^3 as size_gb,
            s.num_docs,
            s.state,
            s.routing_state
        FROM sys.shards s
        JOIN sys.nodes n ON s.node['id'] = n.id
        LEFT JOIN information_schema.table_partitions p 
            ON s.table_name = p.table_name 
            AND s.schema_name = p.table_schema 
            AND s.partition_ident = p.partition_ident
        {where_clause}
        ORDER BY s.table_name, s.schema_name, s.partition_ident, s.id, s."primary" DESC
        """
        
        result = self.execute_query(query, parameters)
        shards = []
        
        for row in result.get('rows', []):
            shards.append(ShardInfo(
                table_name=row[0],
                schema_name=row[1],
                shard_id=row[2],
                node_id=row[5],
                node_name=row[6],
                zone=row[7] or 'unknown',
                is_primary=row[8],
                size_bytes=row[9] or 0,
                size_gb=float(row[10] or 0),
                num_docs=row[11] or 0,
                state=row[12],
                routing_state=row[13],
                partition_ident=row[3],       # CRITICAL FIX: Add partition_ident
                partition_values=row[4]       # Human-readable partition values
            ))
        
        return shards
    
    def get_shard_distribution_summary(self, for_analysis: bool = True) -> Dict[str, Any]:
        """Get a summary of shard distribution across nodes and zones
        
        Args:
            for_analysis: If True, includes all shards for complete cluster analysis
                         If False, only includes operational shards
        """
        where_clause = ""
        if not for_analysis:
            where_clause = """
        WHERE s.routing_state = 'STARTED'
            AND s.recovery['files']['percent'] = 100.0"""
        
        query = f"""
        SELECT 
            n.attributes['zone'] as zone,
            COALESCE(s.node['name'], 'unknown-' || COALESCE(s.node['id'], 'corrupted')) as node_name,
            CASE WHEN s."primary" = true THEN 'PRIMARY' ELSE 'REPLICA' END as shard_type,
            COUNT(*) as shard_count,
            SUM(s.size) / 1024.0^3 as total_size_gb,
            AVG(s.size) / 1024.0^3 as avg_size_gb
        FROM sys.shards s
        JOIN sys.nodes n ON COALESCE(s.node['id'], 'corrupted') = n.id{where_clause}
        GROUP BY n.attributes['zone'], COALESCE(s.node['name'], 'unknown-' || COALESCE(s.node['id'], 'corrupted')), s."primary"
        ORDER BY zone, node_name, shard_type DESC
        """
        
        result = self.execute_query(query)
        
        summary = {
            'by_zone': {},
            'by_node': {},
            'totals': {'primary': 0, 'replica': 0, 'total_size_gb': 0}
        }
        
        for row in result.get('rows', []):
            zone = row[0] or 'unknown'
            node_name = row[1]
            shard_type = row[2]
            shard_count = row[3]
            total_size_gb = float(row[4] or 0)
            avg_size_gb = float(row[5] or 0)
            
            # By zone summary
            if zone not in summary['by_zone']:
                summary['by_zone'][zone] = {'PRIMARY': 0, 'REPLICA': 0, 'total_size_gb': 0}
            summary['by_zone'][zone][shard_type] += shard_count
            summary['by_zone'][zone]['total_size_gb'] += total_size_gb
            
            # By node summary
            if node_name not in summary['by_node']:
                summary['by_node'][node_name] = {
                    'zone': zone,
                    'PRIMARY': 0,
                    'REPLICA': 0,
                    'total_size_gb': 0
                }
            summary['by_node'][node_name][shard_type] += shard_count
            summary['by_node'][node_name]['total_size_gb'] += total_size_gb
            
            # Overall totals
            if shard_type == 'PRIMARY':
                summary['totals']['primary'] += shard_count
            else:
                summary['totals']['replica'] += shard_count
            summary['totals']['total_size_gb'] += total_size_gb
        
        return summary
    
    def get_cluster_health_summary(self) -> Optional[dict]:
        """Get comprehensive cluster health summary with underreplicated shards"""
        try:
            query = """
            SELECT
                (SELECT health FROM sys.health ORDER BY severity DESC LIMIT 1) AS cluster_health,
                COUNT(*) FILTER (WHERE health = 'GREEN') AS green_entities,
                SUM(underreplicated_shards) FILTER (WHERE health = 'GREEN') AS green_underreplicated_shards,
                COUNT(*) FILTER (WHERE health = 'YELLOW') AS yellow_entities,
                SUM(underreplicated_shards) FILTER (WHERE health = 'YELLOW') AS yellow_underreplicated_shards,
                COUNT(*) FILTER (WHERE health = 'RED') AS red_entities,
                SUM(underreplicated_shards) FILTER (WHERE health = 'RED') AS red_underreplicated_shards,
                COUNT(*) FILTER (WHERE health NOT IN ('GREEN', 'YELLOW', 'RED')) AS other_entities,
                SUM(underreplicated_shards) FILTER (WHERE health NOT IN ('GREEN', 'YELLOW', 'RED')) AS other_underreplicated_shards,
                (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('sys', 'information_schema', 'pg_catalog')) AS total_tables,
                (SELECT COUNT(*) FROM information_schema.table_partitions) AS total_partitions
            FROM sys.health
            """
            
            result = self.execute_query(query)
            if result.get('rows'):
                row = result['rows'][0]
                return {
                    'cluster_health': row[0],
                    'green_entities': row[1] or 0,
                    'green_underreplicated_shards': row[2] or 0,
                    'yellow_entities': row[3] or 0,
                    'yellow_underreplicated_shards': row[4] or 0,
                    'red_entities': row[5] or 0,
                    'red_underreplicated_shards': row[6] or 0,
                    'other_entities': row[7] or 0,
                    'other_underreplicated_shards': row[8] or 0,
                    'total_tables': row[9] or 0,
                    'total_partitions': row[10] or 0
                }
        except Exception:
            pass
        return None

    def test_connection(self) -> bool:
        """Test the connection to CrateDB"""
        try:
            result = self.execute_query("SELECT 1")
            return result.get('rowcount', 0) >= 0
        except Exception as e:
            # Log the actual error for debugging
            print(f"Connection test failed: {e}")
            return False
    
    def get_cluster_watermarks(self) -> Dict[str, Any]:
        """Get cluster disk watermark settings"""
        query = """
        SELECT settings['cluster']['routing']['allocation']['disk']['watermark']
        FROM sys.cluster
        """
        
        try:
            result = self.execute_query(query)
            if result.get('rows'):
                watermarks = result['rows'][0][0] or {}
                return {
                    'low': watermarks.get('low', 'Not set'),
                    'high': watermarks.get('high', 'Not set'),
                    'flood_stage': watermarks.get('flood_stage', 'Not set'),
                    'enable_for_single_data_node': watermarks.get('enable_for_single_data_node', 'Not set')
                }
            return {}
        except Exception:
            return {}
    
    def get_cluster_watermark_config(self) -> Dict[str, Any]:
        """Get complete cluster disk watermark configuration including threshold enabled status"""
        watermark_query = """
        SELECT settings['cluster']['routing']['allocation']['disk']['watermark']
        FROM sys.cluster
        """
        
        threshold_query = """
        SELECT settings['cluster']['routing']['allocation']['disk']['threshold_enabled']
        FROM sys.cluster
        """
        
        try:
            # Get watermark settings
            watermark_result = self.execute_query(watermark_query)
            threshold_result = self.execute_query(threshold_query)
            
            watermarks = {}
            threshold_enabled = False
            
            if watermark_result.get('rows'):
                watermarks = watermark_result['rows'][0][0] or {}
            
            if threshold_result.get('rows'):
                threshold_enabled = threshold_result['rows'][0][0] or False
                
            return {
                'threshold_enabled': threshold_enabled,
                'watermarks': {
                    'low': watermarks.get('low', '85%'),
                    'high': watermarks.get('high', '90%'), 
                    'flood_stage': watermarks.get('flood_stage', '95%'),
                    'enable_for_single_data_node': watermarks.get('enable_for_single_data_node', False)
                }
            }
        except Exception:
            # Return defaults if query fails
            return {
                'threshold_enabled': True,
                'watermarks': {
                    'low': '85%',
                    'high': '90%',
                    'flood_stage': '95%',
                    'enable_for_single_data_node': False
                }
            }
    
    def get_master_node_id(self) -> Optional[str]:
        """Get the current master node ID from sys.cluster"""
        query = """
        SELECT master_node FROM sys.cluster
        """
        
        try:
            result = self.execute_query(query)
            if result.get('rows') and result['rows'][0][0]:
                return result['rows'][0][0]
            return None
        except Exception:
            return None
    
    def get_cluster_name(self) -> Optional[str]:
        """Get the cluster name from sys.cluster"""
        query = """
        SELECT name FROM sys.cluster
        """
        
        try:
            result = self.execute_query(query)
            if result.get('rows') and result['rows'][0][0]:
                return result['rows'][0][0]
            return None
        except Exception:
            return None
    
    def get_active_recoveries(self, table_name: Optional[str] = None,
                            node_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get shards that are currently in recovery states from sys.allocations"""
        
        where_conditions = ["current_state != 'STARTED'"]
        parameters = []
        
        if table_name:
            where_conditions.append("table_name = ?")
            parameters.append(table_name)
        
        if node_name:
            where_conditions.append("node_id = (SELECT id FROM sys.nodes WHERE name = ?)")
            parameters.append(node_name)
        
        where_clause = f"WHERE {' AND '.join(where_conditions)}"
        
        query = f"""
        SELECT 
            table_name,
            shard_id,
            current_state,
            explanation,
            node_id
        FROM sys.allocations
        {where_clause}
        ORDER BY current_state, table_name, shard_id
        """
        
        result = self.execute_query(query, parameters)
        
        allocations = []
        for row in result.get('rows', []):
            allocations.append({
                'schema_name': 'doc',  # Default schema since not available in sys.allocations
                'table_name': row[0], 
                'shard_id': row[1],
                'current_state': row[2],
                'explanation': row[3],
                'node_id': row[4]
            })
        
        return allocations
    
    def get_recovery_details(self, schema_name: str, table_name: str, shard_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed recovery information for a specific shard from sys.shards"""
        
        # Query for shards that are actively recovering (not completed)
        # Use COALESCE to handle corrupted node metadata that causes 500 errors
        query = """
        SELECT 
            s.table_name,
            s.schema_name,
            translate(p.values::text, ':{}', '=()') as partition_values,
            s.id as shard_id,
            COALESCE(s.node['name'], 'unknown-' || COALESCE(s.node['id'], 'corrupted')) as node_name,
            COALESCE(s.node['id'], 'corrupted') as node_id,
            s.routing_state,
            s.state,
            s.recovery,
            s.size,
            s."primary",
            s.translog_stats['size'] as translog_size,
            s.translog_stats['uncommitted_size'] as translog_uncommitted_size,
            s.seq_no_stats['max_seq_no'] as max_seq_no
        FROM sys.shards s
        LEFT JOIN information_schema.table_partitions p 
            ON s.table_name = p.table_name 
            AND s.schema_name = p.table_schema 
            AND s.partition_ident = p.partition_ident
        WHERE s.table_name = ? AND s.id = ?
        AND (s.state = 'RECOVERING' OR s.routing_state IN ('INITIALIZING', 'RELOCATING'))
        ORDER BY s.schema_name
        LIMIT 1
        """
        
        result = self.execute_query(query, [table_name, shard_id])
        
        if not result.get('rows'):
            return None
            
        row = result['rows'][0]
        return {
            'table_name': row[0],
            'schema_name': row[1],
            'partition_values': row[2],
            'shard_id': row[3],
            'node_name': row[4],
            'node_id': row[5],
            'routing_state': row[6],
            'state': row[7],
            'recovery': row[8],
            'size': row[9],
            'primary': row[10],
            'translog_size': row[11] or 0,
            'translog_uncommitted_size': row[12] or 0,
            'max_seq_no': row[13]
        }
    
    def _get_primary_max_seq_no(self, schema_name: str, table_name: str, shard_id: int) -> Optional[int]:
        """Get the max_seq_no of the primary shard for replica progress comparison"""
        try:
            query = """
            SELECT s.seq_no_stats['max_seq_no'] as primary_max_seq_no
            FROM sys.shards s
            WHERE s.schema_name = ? AND s.table_name = ? AND s.id = ? 
            AND s."primary" = true
            AND s.state = 'STARTED'
            LIMIT 1
            """
            
            result = self.execute_query(query, [schema_name, table_name, shard_id])
            
            if result.get('rows'):
                return result['rows'][0][0]
            return None
            
        except Exception:
            # If query fails, return None
            return None
    
    def get_all_recovering_shards(self, table_name: Optional[str] = None, 
                                node_name: Optional[str] = None,
                                include_transitioning: bool = False) -> List[RecoveryInfo]:
        """Get comprehensive recovery information by combining sys.allocations and sys.shards data"""
        
        # Step 1: Get active recoveries from allocations (efficient)
        active_allocations = self.get_active_recoveries(table_name, node_name)
        
        if not active_allocations:
            return []
        
        recoveries = []
        
        # Step 2: Get detailed recovery info for each active recovery
        for allocation in active_allocations:
            recovery_detail = self.get_recovery_details(
                allocation['schema_name'],  # This will be 'doc' default
                allocation['table_name'], 
                allocation['shard_id']
            )
            
            if recovery_detail and recovery_detail.get('recovery'):
                # Update allocation with actual schema from sys.shards
                allocation['schema_name'] = recovery_detail['schema_name']
                recovery_info = self._parse_recovery_info(allocation, recovery_detail)
                
                # For replica recoveries, get primary sequence number for progress tracking
                if not recovery_info.is_primary and recovery_info.recovery_type == 'PEER':
                    primary_seq_no = self._get_primary_max_seq_no(
                        recovery_detail['schema_name'],
                        recovery_detail['table_name'],
                        recovery_detail['shard_id']
                    )
                    # Create updated recovery info with primary sequence number
                    recovery_info = RecoveryInfo(
                        schema_name=recovery_info.schema_name,
                        table_name=recovery_info.table_name,
                        partition_values=recovery_info.partition_values,
                        shard_id=recovery_info.shard_id,
                        node_name=recovery_info.node_name,
                        node_id=recovery_info.node_id,
                        recovery_type=recovery_info.recovery_type,
                        stage=recovery_info.stage,
                        files_percent=recovery_info.files_percent,
                        bytes_percent=recovery_info.bytes_percent,
                        total_time_ms=recovery_info.total_time_ms,
                        routing_state=recovery_info.routing_state,
                        current_state=recovery_info.current_state,
                        is_primary=recovery_info.is_primary,
                        size_bytes=recovery_info.size_bytes,
                        source_node_name=recovery_info.source_node_name,
                        translog_size_bytes=recovery_info.translog_size_bytes,
                        translog_uncommitted_bytes=recovery_info.translog_uncommitted_bytes,
                        max_seq_no=recovery_info.max_seq_no,
                        primary_max_seq_no=primary_seq_no
                    )
                
                # Filter out completed recoveries unless include_transitioning is True
                if include_transitioning or not self._is_recovery_completed(recovery_info):
                    recoveries.append(recovery_info)
        
        # Sort by recovery type, then by progress
        return sorted(recoveries, key=lambda r: (r.recovery_type, -r.overall_progress))
    
    def _parse_recovery_info(self, allocation: Dict[str, Any], 
                           shard_detail: Dict[str, Any]) -> RecoveryInfo:
        """Parse recovery information from allocation and shard data"""
        
        recovery = shard_detail.get('recovery', {})
        
        # Extract recovery progress information
        files_info = recovery.get('files', {})
        size_info = recovery.get('size', {})
        
        files_percent = float(files_info.get('percent', 0.0))
        bytes_percent = float(size_info.get('percent', 0.0))
        
        # Calculate actual progress based on recovered vs used
        files_recovered = files_info.get('recovered', 0)
        files_used = files_info.get('used', 1)  # Avoid division by zero
        size_recovered = size_info.get('recovered', 0)
        size_used = size_info.get('used', 1)  # Avoid division by zero
        
        # Use actual progress if different from reported percent
        actual_files_percent = (files_recovered / files_used * 100.0) if files_used > 0 else files_percent
        actual_size_percent = (size_recovered / size_used * 100.0) if size_used > 0 else bytes_percent
        
        # Use the more conservative (lower) progress value
        final_files_percent = min(files_percent, actual_files_percent)
        final_bytes_percent = min(bytes_percent, actual_size_percent)
        
        # Get source node for PEER recoveries
        source_node = None
        if recovery.get('type') == 'PEER':
            source_node = self._find_source_node_for_recovery(
                shard_detail['schema_name'],
                shard_detail['table_name'], 
                shard_detail['shard_id'],
                shard_detail['node_id']
            )

        return RecoveryInfo(
            schema_name=shard_detail['schema_name'],
            table_name=shard_detail['table_name'],
            partition_values=shard_detail.get('partition_values'),
            shard_id=shard_detail['shard_id'],
            node_name=shard_detail['node_name'],
            node_id=shard_detail['node_id'],
            recovery_type=recovery.get('type', 'UNKNOWN'),
            stage=recovery.get('stage', 'UNKNOWN'),
            files_percent=final_files_percent,
            bytes_percent=final_bytes_percent,
            total_time_ms=recovery.get('total_time', 0),
            routing_state=shard_detail['routing_state'],
            current_state=allocation['current_state'],
            is_primary=shard_detail['primary'],
            size_bytes=shard_detail.get('size', 0),
            source_node_name=source_node,
            translog_size_bytes=shard_detail.get('translog_size', 0),
            translog_uncommitted_bytes=shard_detail.get('translog_uncommitted_size', 0),
            max_seq_no=shard_detail.get('max_seq_no'),
            primary_max_seq_no=None  # Will be populated later for replicas
        )
    
    def _find_source_node_for_recovery(self, schema_name: str, table_name: str, shard_id: int, target_node_id: str) -> Optional[str]:
        """Find source node for PEER recovery by looking for primary or other replicas"""
        try:
            # First try to find the primary shard of the same table/shard
            query = """
            SELECT COALESCE(node['name'], 'unknown-' || COALESCE(node['id'], 'corrupted')) as node_name
            FROM sys.shards
            WHERE schema_name = ? AND table_name = ? AND id = ?
            AND state = 'STARTED' AND node['id'] != ?
            AND "primary" = true
            LIMIT 1
            """
            
            result = self.execute_query(query, [schema_name, table_name, shard_id, target_node_id])
            
            if result.get('rows'):
                return result['rows'][0][0]
            
            # If no primary found, look for any started replica
            query_replica = """
            SELECT COALESCE(node['name'], 'unknown-' || COALESCE(node['id'], 'corrupted')) as node_name
            FROM sys.shards
            WHERE schema_name = ? AND table_name = ? AND id = ?
            AND state = 'STARTED' AND node['id'] != ?
            LIMIT 1
            """
            
            result = self.execute_query(query_replica, [schema_name, table_name, shard_id, target_node_id])
            
            if result.get('rows'):
                return result['rows'][0][0]
                
        except Exception:
            # If query fails, just return None
            pass
            
        return None

    def _is_recovery_completed(self, recovery_info: RecoveryInfo) -> bool:
        """Check if a recovery is completed but still transitioning"""
        return (recovery_info.stage == 'DONE' and 
                recovery_info.files_percent >= 100.0 and 
                recovery_info.bytes_percent >= 100.0)

    def get_problematic_shards(self, table_name: Optional[str] = None, 
                             node_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get shards that need attention but aren't actively recovering"""
        
        where_conditions = ["s.state != 'STARTED'"]
        parameters = []
        
        if table_name:
            where_conditions.append("s.table_name = ?")
            parameters.append(table_name)
            
        if node_name:
            where_conditions.append("COALESCE(s.node['name'], 'unknown-' || COALESCE(s.node['id'], 'corrupted')) = ?")
            parameters.append(node_name)
        
        where_clause = f"WHERE {' AND '.join(where_conditions)}"
        
        query = f"""
        SELECT 
            s.schema_name, 
            s.table_name, 
            translate(p.values::text, ':{{}}', '=()') as partition_values,
            s.id as shard_id,
            s.state, 
            s.routing_state, 
            COALESCE(s.node['name'], 'unknown-' || COALESCE(s.node['id'], 'corrupted')) as node_name,
            COALESCE(s.node['id'], 'corrupted') as node_id,
            s."primary"
        FROM sys.shards s
        LEFT JOIN information_schema.table_partitions p 
            ON s.table_name = p.table_name 
            AND s.schema_name = p.table_schema 
            AND s.partition_ident = p.partition_ident
        {where_clause}
        ORDER BY s.state, s.table_name, s.id
        """
        
        result = self.execute_query(query, parameters)
        
        problematic_shards = []
        for row in result.get('rows', []):
            problematic_shards.append({
                'schema_name': row[0] or 'doc',
                'table_name': row[1], 
                'partition_values': row[2],
                'shard_id': row[3],
                'state': row[4],
                'routing_state': row[5],
                'node_name': row[6],
                'node_id': row[7],
                'primary': row[8]
            })
        
        return problematic_shards
    
    def get_active_shards_snapshot(self, min_checkpoint_delta: int = 1000) -> List[ActiveShardSnapshot]:
        """Get a snapshot of all started shards for activity monitoring
        
        Note: This captures ALL started shards regardless of current activity level.
        The min_checkpoint_delta parameter is kept for backwards compatibility but
        filtering is now done during snapshot comparison to catch shards that
        become active between observations.
        
        Args:
            min_checkpoint_delta: Kept for compatibility - filtering now done in comparison
            
        Returns:
            List of ActiveShardSnapshot objects for all started shards
        """
        import time
        
        query = """
        SELECT
            sh.schema_name,
            sh.table_name,
            sh.id AS shard_id,
            sh."primary",
            COALESCE(node['name'], 'unknown-' || COALESCE(node['id'], 'corrupted')) as node_name,
            sh.partition_ident,
            sh.translog_stats['uncommitted_size'] AS translog_uncommitted_bytes,
            sh.seq_no_stats['local_checkpoint'] AS local_checkpoint,
            sh.seq_no_stats['global_checkpoint'] AS global_checkpoint
        FROM
            sys.shards AS sh
        WHERE
            sh.state = 'STARTED'
        ORDER BY
            sh.schema_name, sh.table_name, sh.id, COALESCE(sh.node['name'], 'unknown-' || COALESCE(sh.node['id'], 'corrupted'))
        """
        
        try:
            result = self.execute_query(query)
            snapshots = []
            current_time = time.time()
            
            for row in result.get('rows', []):
                snapshot = ActiveShardSnapshot(
                    schema_name=row[0],
                    table_name=row[1],
                    shard_id=row[2],
                    is_primary=row[3],
                    node_name=row[4],
                    partition_ident=row[5] or '',
                    translog_uncommitted_bytes=row[6] or 0,
                    local_checkpoint=row[7] or 0,
                    global_checkpoint=row[8] or 0,
                    timestamp=current_time
                )
                snapshots.append(snapshot)
                
            return snapshots
            
        except Exception as e:
            print(f"Error getting active shards snapshot: {e}")
            return []