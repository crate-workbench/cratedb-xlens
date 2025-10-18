# Queries

## Shard Distribution over Nodes

```sql
select node['name'], sum(size) / 1024^3, count(id)  from sys.shards  group by 1  order by 1 asc;
+--------------+-----------------------------+-----------+
| node['name'] | (sum(size) / 1.073741824E9) | count(id) |
+--------------+-----------------------------+-----------+
| data-hot-0   |          1862.5866614403203 |       680 |
| data-hot-1   |          1866.0331328986213 |       684 |
| data-hot-2   |          1856.6581886671484 |      1043 |
| data-hot-3   |          1208.932889252901  |       477 |
| data-hot-4   |          1861.7727940855548 |       674 |
| data-hot-5   |          1863.4315695902333 |       744 |
| data-hot-6   |          1851.3522544233128 |       948 |
| NULL         |             0.0             |        35 |
+--------------+-----------------------------+-----------+
SELECT 8 rows in set (0.061 sec)
```

## Shard Distribution PRIMARY/REPLICAS over nodes

```sql

select node['name'], primary,  sum(size) / 1024^3, count(id)  from sys.shards  group by 1,2  order by 1 asc;
+--------------+---------+-----------------------------+-----------+
| node['name'] | primary | (sum(size) / 1.073741824E9) | count(id) |
+--------------+---------+-----------------------------+-----------+
| data-hot-0   | TRUE    |       1459.3267894154415    |       447 |
| data-hot-0   | FALSE   |        403.25987202487886   |       233 |
| data-hot-1   | TRUE    |       1209.6781993638724    |       374 |
| data-hot-1   | FALSE   |        656.3549335347489    |       310 |
| data-hot-2   | TRUE    |       1624.9012612393126    |       995 |
| data-hot-2   | FALSE   |        231.5014410642907    |        48 |
| data-hot-3   | TRUE    |          6.339549297466874  |        58 |
| data-hot-3   | FALSE   |       1202.486775631085     |       419 |
| data-hot-4   | FALSE   |        838.5498185381293    |       225 |
| data-hot-4   | TRUE    |       1023.1511942362413    |       449 |
| data-hot-5   | FALSE   |       1002.365406149067     |       422 |
| data-hot-5   | TRUE    |        860.9174101138487    |       322 |
| data-hot-6   | FALSE   |       1850.3959310995415    |       940 |
| data-hot-6   | TRUE    |          0.9159421799704432 |         8 |
| NULL         | FALSE   |          0.0                |        35 |
+--------------+---------+-----------------------------+-----------+

```

## Nodes available Space

```sql
+------------+--------------------+-----------------------------------------------+
| name       | attributes['zone'] | (fs[1]['disks']['available'] / 1.073741824E9) |
+------------+--------------------+-----------------------------------------------+
| data-hot-5 | us-west-2a         |                            142.3342628479004  |
| data-hot-0 | us-west-2a         |                            142.03089141845703 |
| data-hot-6 | us-west-2b         |                            159.68728256225586 |
| data-hot-3 | us-west-2b         |                            798.8147850036621  |
| data-hot-2 | us-west-2b         |                            156.79160690307617 |
| data-hot-1 | us-west-2c         |                            145.73613739013672 |
| data-hot-4 | us-west-2c         |                            148.39511108398438 |
+------------+--------------------+-----------------------------------------------+
```

## List biggest SHARDS on a particular Nodes

```sql
select node['name'], table_name, schema_name, id,  sum(size) / 1024^3 from sys.shards
    where node['name'] = 'data-hot-2'
    AND routing_state = 'STARTED'
    AND recovery['files']['percent'] = 0
    group by 1,2,3,4  order by 5  desc limit 8;
+--------------+-----------------------+-------------+----+-----------------------------+
| node['name'] | table_name            | schema_name | id | (sum(size) / 1.073741824E9) |
+--------------+-----------------------+-------------+----+-----------------------------+
| data-hot-2   | bottleFieldData    | curvo          |  5 |         135.568662205711    |
| data-hot-2   | bottleFieldData    | curvo          |  8 |         134.813782049343    |
| data-hot-2   | bottleFieldData    | curvo          |  3 |         133.43549298401922  |
| data-hot-2   | bottleFieldData    | curvo          | 11 |         130.10448653809726  |
| data-hot-2   | turtleFieldData    | curvo          | 31 |          54.642812703736126 |
| data-hot-2   | turtleFieldData    | curvo          | 29 |          54.06101848650724  |
| data-hot-2   | turtleFieldData    | curvo          |  5 |          53.96749582327902  |
| data-hot-2   | turtleFieldData    | curvo          | 21 |          53.72262619435787  |
+--------------+-----------------------+-------------+----+-----------------------------+
SELECT 8 rows in set (0.062 sec)
```

## Move REROUTE

```sql

alter table "curvo"."bottlefieldData" reroute move shard 21 from 'data-hot-2' to 'data-hot-3';
```

---

```sql

WITH shard_summary AS (
    SELECT
        node['name'] AS node_name,
        table_name,
        schema_name,
        CASE
            WHEN "primary" = true THEN 'PRIMARY'
            ELSE 'REPLICA'
        END AS shard_type,
        COUNT(*) AS shard_count,
        SUM(size) / 1024^3 AS total_size_gb
    FROM sys.shards
    WHERE table_name = 'orderffD'
        AND routing_state = 'STARTED'
        AND recovery['files']['percent'] = 0
    GROUP BY node['name'], table_name, schema_name, "primary"
)
SELECT
    node_name,
    table_name,
    schema_name,
    shard_type,
    shard_count,
    ROUND(total_size_gb, 2) AS total_size_gb,
    ROUND(total_size_gb / shard_count, 2) AS avg_shard_size_gb
FROM shard_summary
ORDER BY node_name, shard_type DESC, total_size_gb DESC;
```

```sql
-- Comprehensive shard distribution showing both node and zone details
SELECT
    n.attributes['zone'] AS zone,
    s.node['name'] AS node_name,
    s.table_name,
    s.schema_name,
    CASE
        WHEN s."primary" = true THEN 'PRIMARY'
        ELSE 'REPLICA'
    END AS shard_type,
    s.id AS shard_id,
    s.size / 1024^3 AS shard_size_gb,
    s.num_docs,
    s.state
FROM sys.shards s
JOIN sys.nodes n ON s.node['id'] = n.id
WHERE s.table_name = 'your_table_name'  -- Replace with your specific table name
    AND s.routing_state = 'STARTED'
    AND s.recovery['files']['percent'] = 0
ORDER BY
    n.attributes['zone'],
    s.node['name'],
    s."primary" DESC,  -- Primary shards first
    s.id;

-- Summary by zone and shard type
SELECT
    n.attributes['zone'] AS zone,
    CASE
        WHEN s."primary" = true THEN 'PRIMARY'
        ELSE 'REPLICA'
    END AS shard_type,
    COUNT(*) AS shard_count,
    COUNT(DISTINCT s.node['name']) AS nodes_with_shards,
    ROUND(SUM(s.size) / 1024^3, 2) AS total_size_gb,
    ROUND(AVG(s.size) / 1024^3, 3) AS avg_shard_size_gb,
    SUM(s.num_docs) AS total_documents
FROM sys.shards s
JOIN sys.nodes n ON s.node['id'] = n.id
WHERE s.table_name = 'orderffD'  -- Replace with your specific table name
    AND s.routing_state = 'STARTED'
    AND s.recovery['files']['percent'] = 0
GROUP BY n.attributes['zone'], s."primary"
ORDER BY zone, shard_type DESC;

```

## Relocation

```sql
cr> select node['name'], id, recovery['stage'], recovery['size'], routing_state, state, primary, table_name, relocating_node, size / 1024^3 as size_gb, partition_ident
            from sys.shards
            where routing_state in ('RELOCATING', 'INITIALIZING')
            order by id;
```

```sql
SELECT
        table_name,
        shard_id,
        current_state,
        explanation,
        node_id
    FROM sys.allocations
    WHERE current_state != 'STARTED' and table_name = 'dispatchio'            and shard_id = 19
    ORDER BY current_state, table_name, shard_id;

+-----------------------+----------+---------------+-------------+------------------------+
| table_name            | shard_id | current_state | explanation | node_id                |
+-----------------------+----------+---------------+-------------+------------------------+
| dispatchio            |       19 | RELOCATING    |        NULL | ZH6fBanGSjanGqeSh-sw0A |
+-----------------------+----------+---------------+-------------+------------------------+
```

```sql
SELECT
        COUNT(*) as recovering_shards
    FROM sys.shards
    WHERE state = 'RECOVERING' OR routing_state IN ('INITIALIZING', 'RELOCATING');

```

```sql

SELECT
        table_name,
        shard_id,
        current_state,
        explanation,
        node_id
    FROM sys.allocations
    WHERE current_state != 'STARTED' and table_name = 'dispatchio            and shard_id = 19
    ORDER BY current_state, table_name, shard_id;

```

# "BIGDUDES" Focuses on your **biggest storage consumers** and shows how their shards are distributed across nodes.

´´´sql
WITH largest_tables AS (
SELECT
schema_name,
table_name,
SUM(CASE WHEN "primary" = true THEN size ELSE 0 END) as total_primary_size
FROM sys.shards
WHERE schema_name NOT IN ('sys', 'information_schema', 'pg_catalog')
GROUP BY schema_name, table_name
ORDER BY total_primary_size DESC
LIMIT 10
)
SELECT
s.schema_name,
s.table_name,
s.node['name'] as node_name,
COUNT(CASE WHEN s."primary" = true THEN 1 END) as primary_shards,
COUNT(CASE WHEN s."primary" = false THEN 1 END) as replica_shards,
COUNT(\*) as total_shards,
ROUND(SUM(s.size) / 1024.0 / 1024.0 / 1024.0, 2) as total_size_gb,
ROUND(SUM(CASE WHEN s."primary" = true THEN s.size ELSE 0 END) / 1024.0 / 1024.0 / 1024.0, 2) as primary_size_gb,
ROUND(SUM(CASE WHEN s."primary" = false THEN s.size ELSE 0 END) / 1024.0 / 1024.0 / 1024.0, 2) as replica_size_gb,
SUM(s.num_docs) as total_documents
FROM sys.shards s
INNER JOIN largest_tables lt ON (s.schema_name = lt.schema_name AND s.table_name = lt.table_name)
GROUP BY s.schema_name, s.table_name, s.node['name']
ORDER BY s.schema_name, s.table_name, s.node['name'];

````

# Shard Distribution

```sql

SELECT
        CASE
            WHEN size < 1*1024*1024*1024::bigint THEN '<1GB'
            WHEN size < 5*1024*1024*1024::bigint THEN '1GB-5GB'
            WHEN size < 10*1024*1024*1024::bigint THEN '5GB-10GB'
            WHEN size < 50*1024*1024*1024::bigint THEN '10GB-50GB'
            ELSE '>=50GB'
        END AS size_bucket,
        COUNT(*) AS shards_in_bucket,
        ROUND(AVG(size)::numeric / 1024 / 1024 / 1024, 2) AS avg_bucket_size_gb
    FROM sys.shards
    WHERE state = 'STARTED'
    GROUP BY size_bucket
    ORDER BY
        CASE size_bucket
            WHEN '<1GB' THEN 1
            WHEN '1GB-5GB' THEN 2
            WHEN '5GB-10GB' THEN 3
            WHEN '10GB-50GB' THEN 4
            ELSE 5
        END;
````

## Shard Distribution by Node

```sql

SELECT
        s.node['name'] as node_name,
        CASE
            WHEN size < 1*1024*1024*1024::bigint THEN '<1GB'
            WHEN size < 5*1024*1024*1024::bigint THEN '1GB-5GB'
            WHEN size < 10*1024*1024*1024::bigint THEN '5GB-10GB'
            WHEN size < 50*1024*1024*1024::bigint THEN '10GB-50GB'
            ELSE '>=50GB'
        END AS size_bucket,
        COUNT(*) AS shards_in_bucket,
        ROUND(AVG(size)::numeric / 1024 / 1024 / 1024, 2) AS avg_bucket_size_gb
    FROM sys.shards s
    WHERE state = 'STARTED'
    GROUP BY node_name, size_bucket
    ORDER BY node_name, size_bucket;
```

## Active Shard detection

```sql

SELECT
        sh.schema_name,
        sh.table_name,
        sh.id AS shard_id, primary, node['name'],
        sh.partition_ident,
        sh.translog_stats['uncommitted_size'] / 1024^2 AS translog_uncommitted_bytes,
        sh.seq_no_stats['local_checkpoint'] - sh.seq_no_stats['global_checkpoint'] AS checkpoint_delta
    FROM
        sys.shards AS sh
    WHERE
        sh.state = 'STARTED'
        AND sh.translog_stats['uncommitted_size'] > 10 * 1024 ^2  -- threshold: e.g., 10MB
        OR (sh.seq_no_stats['local_checkpoint'] - sh.seq_no_stats['global_checkpoint'] > 1000) -- significant lag
    ORDER BY
        sh.translog_stats['uncommitted_size'] DESC,
        checkpoint_delta DESC
        limit 10;
```

```sql
partition-id / values from information_schema table by using a join
ALTER TABLE "TURVO"."shipmentFormFieldData" REROUTE CANCEL SHARD 11 on 'data-hot-8' WITH (allow_primary=False);
```

```sql

SELECT
                    sh.schema_name,
                    sh.table_name,
                    translate(p.values::text, ':{}', '=()') as partition_values,
                    sh.id AS shard_id,
                    node['name'],
                    sh.translog_stats['uncommitted_size'] / 1024^2 AS translog_uncommitted_mb
                FROM
                    sys.shards AS sh
                LEFT JOIN information_schema.table_partitions p
                    ON sh.table_name = p.table_name
                    AND sh.schema_name = p.table_schema
                    AND sh.partition_ident = p.partition_ident
                WHERE
                    sh.state = 'STARTED'
                    AND sh.translog_stats['uncommitted_size'] > 300 * 1024 ^2  -- threshold: e.g., 10MB
                    AND primary=FALSE
                ORDER BY
                    6 DESC LIMIT 10;
+-------------+------------------------------+----------------------------+----------+--------------+-------------------------+
| schema_name | table_name                   | partition_values           | shard_id | node['name'] | translog_uncommitted_mb |
+-------------+------------------------------+----------------------------+----------+--------------+-------------------------+
| TURVO       | shipmentFormFieldData        | NULL                       |       14 | data-hot-6   |     7011.800104141235   |
| TURVO       | shipmentFormFieldData        | NULL                       |       27 | data-hot-7   |     5131.491161346436   |
| TURVO       | shipmentFormFieldData        | NULL                       |        0 | data-hot-9   |     2460.8706073760986  |
| TURVO       | shipmentFormFieldData        | NULL                       |        7 | data-hot-2   |     1501.8993682861328  |
| TURVO       | shipmentFormFieldData        | NULL                       |       10 | data-hot-5   |      504.0952272415161  |
| TURVO       | shipmentFormFieldData        | NULL                       |       29 | data-hot-3   |      501.0663766860962  |
| TURVO       | shipmentFormFieldData        | NULL                       |       16 | data-hot-8   |      497.5628480911255  |
| TURVO       | shipmentFormFieldData_events | ("sync_day"=1757376000000) |        3 | data-hot-2   |      481.20221996307373 |
| TURVO       | shipmentFormFieldData_events | ("sync_day"=1757376000000) |        4 | data-hot-4   |      473.12464427948    |
| TURVO       | orderFormFieldData           | NULL                       |        5 | data-hot-1   |      469.4924907684326  |
+-------------+------------------------------+----------------------------+----------+--------------+-------------------------+

```

# Segements per Shard

```sql
SELECT
        shard_id,
        table_schema,
        table_name,
        COUNT(*) AS segment_count
    FROM sys.segments
    GROUP BY shard_id, table_schema, table_name
    ORDER BY segment_count DESC
    LIMIT 10;
```

```sql

SELECT
        s.node['name'] AS node_name,
        CASE
            WHEN size < 512*1024*1024::bigint THEN '<512MB'
            WHEN size < 2.5*1024*1024*1024::bigint THEN '512MB-2.5GB'
            WHEN size < 5*1024*1024*1024::bigint THEN '2.5GB-5GB'
            WHEN size < 25*1024*1024*1024::bigint THEN '5GB-25GB'
            ELSE '>=25GB'
        END AS size_bucket,
        COUNT(*) AS segments_in_bucket,
        ROUND(AVG(size)::numeric / 1024 / 1024 / 1024, 2) AS avg_segment_size_gb
    FROM sys.segments s
    GROUP BY node_name, size_bucket
    ORDER BY node_name, size_bucket;
```

### Count retention_lease

### for a partition

```sql
cr> SELECT array_length(retention_leases['leases'], 1) as cnt_leases, id from sys.shards WHERE table_name = 'shipmentFormFieldData' AND partition_ident = '04732dpl6or3gd1
    o60o30c1g' order by array_length(retention_leases['leases'], 1);
+------------+----+
| cnt_leases | id |
+------------+----+
|          1 |  5 |
|          1 |  4 |
|          1 |  7 |
|          1 |  0 |
|          1 |  3 |
|          1 |  6 |
|          1 |  1 |
|          1 |  2 |
+------------+----+
SELECT 8 rows in set (0.038 sec)
cr>

```

### for a table

```sql

SELECT array_length(retention_leases['leases'], 1) as cnt_leases, id from sys.shards WHERE table_name = 'shipmentFormFieldData' AND array_length(retention_leases['leases'], 1) > 1 order by 1;
```

#### list partition ids

```sql
cr> SELECT partition_ident, values
    FROM information_schema.table_partitions
    WHERE table_schema = 'TURVO'
      AND table_name = 'shipmentFormFieldData' limit 100;
+--------------------------+--------------------------------+
| partition_ident          | values                         |
+--------------------------+--------------------------------+
| 04732dhi6srjedhg60o30c1g | {"id_ts_month": 1627776000000} |
| 04732d9o60qj2d9i60o30c1g | {"id_ts_month": 1580515200000} |
| 04732dhj6krj4d1o60o30c1g | {"id_ts_month": 1635724800000} |
| 04732dhg64qj2c1k60o30c1g | {"id_ts_month": 1601510400000} |
| 04732dhk60sjid9i60o30c1g | {"id_ts_month": 1640995200000} |
```

cr> SELECT partition*ident, values
FROM information_schema.table_partitions
WHERE table_schema = 'TURVO'
AND table_name = 'shipmentFormFieldData' limit 100;
+--------------------------+--------------------------------+
| partition_ident | values |
+--------------------------+--------------------------------+
| 04732dhi6srjedhg60o30c1g | {"id_ts_month": 1627776000000} |
| 04732d9o60qj2d9i60o30c1g | {"id_ts_month": 1580515200000} |
| 04732dhj6krj4d1o60o30c1g | {"id_ts_month": 1635724800000} |
| 04732dhg64qj2c1k60o30c1g | {"id_ts_month": 1601510400000} |
| 04732dhk60sjid9i60o30c1g | {"id_ts_month": 1640995200000} |
| 04732dpk60rjgdpi60o30c1g | {"id_ts_month": 1740787200000} |
| 04732dhp6ooj2e1k60o30c1g | {"id_ts_month": 1696118400000} |
| 04732dhl6or36cpm60o30c1g | {"id_ts_month": 1656633600000} |
| 04732d9p6op38c1g60o30c1g | {"id_ts_month": 1596240000000} |
| 04732dhl6go38c9m60o30c1g | {"id_ts_month": 1654041600000} |
| 04732dpg6orj8d9m60o30c1g | {"id_ts_month": 1706745600000} |
| 04732d9p60sjce9m60o30c1g | {"id_ts_month": 1590969600000} |
| 04732dhi6ko3idpm60o30c1g | {"id_ts_month": 1625097600000} |
| 04732dpj6kr3ge9m60o30c1g | {"id_ts_month": 1735689600000} |
| 04732dhm74s3acho60o30c1g | {"id_ts_month": 1669852800000} |
| 04732dpi6koj8e1o60o30c1g | {"id_ts_month": 1725148800000} |
| 04732dhg6orjgc1o60o30c1g | {"id_ts_month": 1606780800000} |
| 04732dhm6gqjgchk60o30c1g | {"id_ts_month": 1664582400000} |
| 04732d9p70sj2e1k60o30c1g | {"id_ts_month": 1598918400000} |
| 04732dhk6cr3ecpm60o30c1g | {"id_ts_month": 1643673600000} |
| 04732d9o6kr3ie9i60o30c1g | {"id_ts_month": 1585699200000} |
| 04732dhp60s38e1g60o30c1g | {"id_ts_month": 1690848000000} |
| 04732dhn6kp30e9m60o30c1g | {"id_ts_month": 1675209600000} |
| 04732dpk6oo3adpm60o30c1g | {"id_ts_month": 1746057600000} |
| 04732dpg74p3ac9i60o30c1g | {"id_ts_month": 1709251200000} |
| 04732dph6gqj4c9m60o30c1g | {"id_ts_month": 1714521600000} |
| 04732dhn68qj6c9i60o30c1g | {"id_ts_month": 1672531200000} |
| 04732dhm6sp3cc1o60o30c1g | {"id_ts_month": 1667260800000} |
| 04732dhl64pjccpi60o30c1g | {"id_ts_month": 1651363200000} |
| 04732dph6sp30c1g60o30c1g | {"id_ts_month": 1717200000000} |
| 04732dph74rjichg60o30c1g | {"id_ts_month": 1719792000000} |
| 04732dpj6co32c9i60o30c1g | {"id_ts_month": 1733011200000} |
| 04732dpg64pjge1o60o30c1g | {"id_ts_month": 1701388800000} |
| 04732dpj70pjce1g60o30c1g | {"id_ts_month": 1738368000000} |
| 04732dpk6cq3cd9m60o30c1g | {"id_ts_month": 1743465600000} |
| 04732dhh6sp36d9i60o30c1g | {"id_ts_month": 1617235200000} |
| 04732dpi68q3ec1k60o30c1g | {"id_ts_month": 1722470400000} |
| 04732dho70ojce9m60o30c1g | {"id_ts_month": 1688169600000} |
| 04732dhg6gojge1o60o30c1g | {"id_ts_month": 1604188800000} |
| 04732dhk70rjec9i60o30c1g | {"id_ts_month": 1648771200000} |
| 04732dhj70pj2dho60o30c1g | {"id_ts_month": 1638316800000} |
| 04732dho60pj0dpi60o30c1g | {"id_ts_month": 1680307200000} |
| 04732d9o6co34c1o60o30c1g | {"id_ts_month": 1583020800000} |
| 04732dhj60q3ad1k60o30c1g | {"id_ts_month": 1630454400000} |
| 04732dhg74q3ae9i60o30c1g | {"id_ts_month": 1609459200000} |
| 04732dhl74pj2chg60o30c1g | {"id_ts_month": 1659312000000} |
| 04732dpi6srj8c1o60o30c1g | {"id_ts_month": 1727740800000} |
*| 04732dpl6go30dhk60o30c1g | {"id*ts_month": 1754006400000} |
| 04732dhp70rjidho60o30c1g | {"id_ts_month": 1698796800000} |
| 04732dhi68qj0d9m60o30c1g | {"id_ts_month": 1622505600000} |
| 04732d9p6cqjcc9m60o30c1g | {"id_ts_month": 1593561600000} |
| 04732dpg6go3cdpi60o30c1g | {"id_ts_month": 1704067200000} |
| 04732dho68s3ie9i60o30c1g | {"id_ts_month": 1682899200000} |
| 04732d9n6ss36dho60o30c1g | {"id_ts_month": 1577836800000} |
| 04732dpj60q32e9i60o30c1g | {"id_ts_month": 1730419200000} |
| 04732dhm64sjic1k60o30c1g | {"id_ts_month": 1661990400000} |
| 04732dhh6gqjadho60o30c1g | {"id_ts_month": 1614556800000} |
| 04732dho6kqjedpm60o30c1g | {"id_ts_month": 1685577600000} |
| 04732dhn6sr34e1o60o30c1g | {"id_ts_month": 1677628800000} |
| 04732dph64sj4e9m60o30c1g | {"id_ts_month": 1711929600000} |
| 04732dhp6cqj4dhk60o30c1g | {"id_ts_month": 1693526400000} |
| 04732dpk70rj6dhg60o30c1g | {"id_ts_month": 1748736000000} |
| 04732dpl64pj4e1g60o30c1g | {"id_ts_month": 1751328000000} |
*| 04732dpl6or3gd1o60o30c1g | {"id_ts_month": 1756684800000} |
| 04732dhh74s34dpi60o30c1g | {"id_ts_month": 1619827200000} |
| 04732dhj6co38dhk60o30c1g | {"id_ts_month": 1633046400000} |
| 04732dhk6oo3icho60o30c1g | {"id_ts_month": 1646092800000} |
| 04732dhh68oj6dpm60o30c1g | {"id_ts_month": 1612137600000} |
+--------------------------+--------------------------------+
SELECT 68 rows in set (0.006 sec)

## Disable Rebalancing

SET GLOBAL PERSISTENT "cluster.routing.rebalance.enable"='xxx'; -- all / none
[data-hot-7] updating [cluster.routing.rebalance.enable] from [all] to [none]`

### Report on schema, tables, sizes, ...

```sql
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
           COUNT(*) AS num_shards_total
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
```

---

partition_ident | values |
+--------------------------+--------------------------------+
| 04732dhp6ooj2e1k60o30c1g | {"id_ts_month": 1696118400000} |
| 04732dpk60rjgdpi60o30c1g | {"id_ts_month": 1740787200000} |
| 04732dhl6or36cpm60o30c1g | {"id_ts_month": 1656633600000} |
| 04732dpi6srj8c1o60o30c1g | {"id_ts_month": 1727740800000} |
| 04732dhl74pj2chg60o30c1g | {"id_ts_month": 1659312000000} |
| 04732dhl6go38c9m60o30c1g | {"id_ts_month": 1654041600000} |
| 04732dpg6orj8d9m60o30c1g | {"id_ts_month": 1706745600000} |
| 04732dpl6go30dhk60o30c1g | {"id_ts_month": 1754006400000} |
| 04732dhp70rjidho60o30c1g | {"id_ts_month": 1698796800000} |
| 04732dpj6kr3ge9m60o30c1g | {"id_ts_month": 1735689600000} |
| 04732dhm74s3acho60o30c1g | {"id_ts_month": 1669852800000} |
| 04732dpi6koj8e1o60o30c1g | {"id_ts_month": 1725148800000} |
| 04732dhm6gqjgchk60o30c1g | {"id_ts_month": 1664582400000} |
| 04732dpg6go3cdpi60o30c1g | {"id_ts_month": 1704067200000} |
| 04732dho68s3ie9i60o30c1g | {"id_ts_month": 1682899200000} |
| 04732dhp60s38e1g60o30c1g | {"id_ts_month": 1690848000000} |
| 04732dhn6kp30e9m60o30c1g | {"id_ts_month": 1675209600000} |
| 04732dpk6oo3adpm60o30c1g | {"id_ts_month": 1746057600000} |
| 04732dpj60q32e9i60o30c1g | {"id_ts_month": 1730419200000} |
| 04732dpl74p3edho60o30c1g | {"id_ts_month": 1759276800000} |
| 04732dhm64sjic1k60o30c1g | {"id_ts_month": 1661990400000} |
| 04732dpg74p3ac9i60o30c1g | {"id_ts_month": 1709251200000} |
| 04732dph6gqj4c9m60o30c1g | {"id_ts_month": 1714521600000} |
| 04732dhn68qj6c9i60o30c1g | {"id_ts_month": 1672531200000} |
| 04732dhm6sp3cc1o60o30c1g | {"id_ts_month": 1667260800000} |
| 04732dhl64pjccpi60o30c1g | {"id_ts_month": 1651363200000} |
| 04732dho6kqjedpm60o30c1g | {"id_ts_month": 1685577600000} |
| 04732dhn6sr34e1o60o30c1g | {"id_ts_month": 1677628800000} |
| 04732dph74rjichg60o30c1g | {"id_ts_month": 1719792000000} |
| 04732dph6sp30c1g60o30c1g | {"id_ts_month": 1717200000000} |
| 04732dph64sj4e9m60o30c1g | {"id_ts_month": 1711929600000} |
| 04732dpj6co32c9i60o30c1g | {"id_ts_month": 1733011200000} |
| 04732dhp6cqj4dhk60o30c1g | {"id_ts_month": 1693526400000} |
| 04732dpg64pjge1o60o30c1g | {"id_ts_month": 1701388800000} |
| 04732dpk70rj6dhg60o30c1g | {"id_ts_month": 1748736000000} |
| 04732dpl64pj4e1g60o30c1g | {"id_ts_month": 1751328000000} |
| 04732dpj70pjce1g60o30c1g | {"id_ts_month": 1738368000000} |
| 04732dpl6or3gd1o60o30c1g | {"id_ts_month": 1756684800000} |
| 04732dpk6cq3cd9m60o30c1g | {"id_ts_month": 1743465600000} |
| 04732dpi68q3ec1k60o30c1g | {"id_ts_month": 1722470400000} |
| 04732dho70ojce9m60o30c1g | {"id_ts_month": 1688169600000} |
| 04732dho60pj0dpi60o30c1g | {"id_ts_month": 1680307200000} |
+--------------------------+--------------------------------+

## set translog_flush_size

```sql
SELECT
      'ALTER TABLE "' || table_schema || '"."' || table_name ||
      '" PARTITION (id_ts_month = ' || "values"['id_ts_month'] ||
      ') SET ("translog.flush_threshold_size" = ''2gb'');' AS alter_stmt
    FROM information_schema.table_partitions
    WHERE table_schema = 'TURVO'
      AND table_name = 'shipmentFormFieldData';
```

## Cluster Health

```sql

cr> SELECT
        (SELECT health FROM sys.health ORDER BY severity DESC LIMIT 1) AS cluster_health,
        COUNT(*) FILTER (WHERE health = 'GREEN') AS green_entities,
        COUNT(*) FILTER (WHERE health = 'YELLOW') AS yellow_entities,
        COUNT(*) FILTER (WHERE health = 'RED') AS red_entities,
        COUNT(*) FILTER (WHERE health NOT IN ('GREEN', 'YELLOW', 'RED')) AS other_entities,
        (SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('sys', 'information_schema', 'pg_catalog')) AS total_tables,
        (SELECT COUNT(*) FROM information_schema.table_partitions) AS total_partitions
    FROM sys.health;

+----------------+----------------+-----------------+--------------+----------------+--------------+------------------+
| cluster_health | green_entities | yellow_entities | red_entities | other_entities | total_tables | total_partitions |
+----------------+----------------+-----------------+--------------+----------------+--------------+------------------+
| GREEN          |            566 |               0 |            0 |              0 |          590 |              100 |
+----------------+----------------+-----------------+--------------+----------------+--------------+------------------+
SELECT 1 row in set (0.074 sec)

```

```sql

cr> select * from sys.health;
+--------+----------------+--------------------------+----------+-------------------------------------------------------------+------------------------------------+------------------------+
| health | missing_shards | partition_ident          | severity | table_name                                                  | table_schema                       | underreplicated_shards |
+--------+----------------+--------------------------+----------+-------------------------------------------------------------+------------------------------------+------------------------+
| GREEN  |              0 | NULL                     |        1 | temp_filter                                                 | replication_third_filter           |                      0 |
| GREEN  |              0 | 04732dpm60pj2cpm60o30c1g |        1 | shipmentFormFieldData_events                                | TURVO                              |                      0 |
| GREEN  |              0 | NULL                     |        1 | shipment_carrier_order_external_id                          | replication_first_materialized     |                      0 |
| GREEN  |              0 | 04732dhp60s38e1g60o30c1g |        1 | shipmentFormFieldData                                       | TURVO                              |                      0 |
| GREEN  |              0 | NULL                     |        1 | account_tags                                                | replication_first_materialized     |                      0 |
```

```sql

cr> SELECT
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
    FROM sys.health;

cluster_health                | GREEN
green_entities                | 566
green_underreplicated_shards  | 0
yellow_entities               | 0
yellow_underreplicated_shards | NULL
red_entities                  | 0
red_underreplicated_shards    | NULL
other_entities                | 0
other_underreplicated_shards  | NULL
total_tables                  | 590
total_partitions              | 100
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
```

## Segements

Careful these are expensive.

```sql

cr> SELECT
        table_schema,
        table_name,
        shard_id,
        partition_ident,
        node['name'] AS node,
        COUNT(*) AS num_segments,
        SUM(size) AS total_size_bytes,
        SUM(num_docs) AS total_docs,
        SUM(deleted_docs) AS total_deleted_docs,
        ROUND(
            CASE
                WHEN SUM(num_docs) + SUM(deleted_docs) = 0 THEN 0
                ELSE SUM(deleted_docs) * 100.0 / (SUM(num_docs) + SUM(deleted_docs))
            END,
        2) AS deleted_ratio
    FROM sys.segments
    WHERE table_schema = 'replication_first_materialized'
      AND table_name = 'orders_external_ids'
      AND shard_id IN (0,1,2,3,4)
    GROUP BY node['name'], table_schema, table_name, shard_id, partition_ident
    ORDER BY table_schema, table_name, deleted_ratio DESC;

+--------------------------------+---------------------+----------+-----------------+-------------+--------------+------------------+------------+--------------------+---------------+
| table_schema                   | table_name          | shard_id | partition_ident | node        | num_segments | total_size_bytes | total_docs | total_deleted_docs | deleted_ratio |
+--------------------------------+---------------------+----------+-----------------+-------------+--------------+------------------+------------+--------------------+---------------+
| replication_first_materialized | orders_external_ids |        0 |                 | data-hot-10 |           31 |      18142010447 |   53709076 |           16549742 |         23.56 |
| replication_first_materialized | orders_external_ids |        0 |                 | data-hot-5  |           31 |      18142010447 |   53709076 |           16549742 |         23.56 |
| replication_first_materialized | orders_external_ids |        2 |                 | data-hot-3  |           26 |      17939585648 |   53720030 |           16157118 |         23.12 |
| replication_first_materialized | orders_external_ids |        2 |                 | data-hot-11 |           26 |      17939585648 |   53720030 |           16157118 |         23.12 |
| replication_first_materialized | orders_external_ids |        3 |                 | data-hot-2  |           28 |      18016906587 |   53719225 |           15678177 |         22.59 |
| replication_first_materialized | orders_external_ids |        3 |                 | data-hot-11 |           28 |      18016906587 |   53719225 |           15678177 |         22.59 |
| replication_first_materialized | orders_external_ids |        4 |                 | data-hot-7  |           28 |      17987623213 |   53698689 |           15545694 |         22.45 |
| replication_first_materialized | orders_external_ids |        4 |                 | data-hot-8  |           28 |      17987623213 |   53698689 |           15545694 |         22.45 |
| replication_first_materialized | orders_external_ids |        1 |                 | data-hot-5  |           30 |      17924402947 |   53716575 |           15137731 |         21.99 |
| replication_first_materialized | orders_external_ids |        1 |                 | data-hot-3  |           30 |      17924402947 |   53716575 |           15137731 |         21.99 |
+--------------------------------+---------------------+----------+-----------------+-------------+--------------+------------------+------------+--------------------+---------------+
```

```sql

cr> SELECT
        table_schema,
        table_name,
        shard_id, segment_name,
        partition_ident,
        node['name'] AS node,
        COUNT(*) AS num_segments,
        SUM(size) AS total_size_bytes,
        SUM(num_docs) AS total_docs,
        SUM(deleted_docs) AS total_deleted_docs,
        ROUND(
            CASE
                WHEN SUM(num_docs) + SUM(deleted_docs) = 0 THEN 0
                ELSE SUM(deleted_docs) * 100.0 / (SUM(num_docs) + SUM(deleted_docs))
            END,
        2) AS deleted_ratio
    FROM sys.segments
    WHERE table_schema = 'replication_first_materialized'
      AND table_name = 'orders_items'
      AND shard_id IN (0)
    GROUP BY node['name'], table_schema, segment_name, shard_id, table_name, partition_ident
    ORDER BY table_schema, table_name, deleted_ratio DESC;

+--------------------------------+--------------+----------+--------------+-----------------+------------+--------------+------------------+------------+--------------------+---------------+
| table_schema                   | table_name   | shard_id | segment_name | partition_ident | node       | num_segments | total_size_bytes | total_docs | total_deleted_docs | deleted_ratio |
+--------------------------------+--------------+----------+--------------+-----------------+------------+--------------+------------------+------------+--------------------+---------------+
| replication_first_materialized | orders_items |        0 | _yb5         |                 | data-hot-2 |            1 |          3540107 |       4725 |              27740 |         85.45 |
| replication_first_materialized | orders_items |        0 | _yb5         |                 | data-hot-8 |            1 |          3540107 |       4725 |              27740 |         85.45 |
| replication_first_materialized | orders_items |        0 | _yci         |                 | data-hot-2 |            1 |         12848803 |      21477 |              28810 |         57.29 |
| replication_first_materialized | orders_items |        0 | _yci         |                 | data-hot-8 |            1 |         12848803 |      21477 |              28810 |         57.29 |
| replication_first_materialized | orders_items |        0 | _ybf         |                 | data-hot-2 |            1 |          9122185 |      13905 |              17059 |         55.09 |
| replication_first_materialized | orders_items |        0 | _ybf         |                 | data-hot-8 |            1 |          9122185 |      13905 |              17059 |         55.09 |
| replication_first_materialized | orders_items |        0 | _yaw         |                 | data-hot-2 |            1 |         77757363 |     147539 |             177513 |         54.61 |
| replication_first_materialized | orders_items |        0 | _yaw         |                 | data-hot-8 |            1 |         77757363 |     147539 |             177513 |         54.61 |
```

## table settings

detect odd max_thread_count

```sql
SELECT 209 rows in set (0.172 sec)
cr> SELECT
      table_schema,
      table_name,
      partitioned_by,
      clustered_by
      --settings['merge']['scheduler']['max_thread_count'] AS max_thread_count
    FROM information_schema.tables
    WHERE clustered_by != '_id';

```

detect custom routing column

```sql

| TURVO        | slotMetaDataDocument_failures                 | NULL           |                1 |
| TURVO        | tasks_failures_1                              | NULL           |                1 |
| TURVO        | orderFormFieldData_events                     | ["sync_day"]   |                1 |
| TURVO        | approvalFormFieldData_transforms              | NULL           |                1 |
+--------------+-----------------------------------------------+----------------+------------------+
SELECT 158 rows in set (0.261 sec)
cr> SELECT
          table_schema,
          table_name,
          partitioned_by,
          settings['merge']['scheduler']['max_thread_count'] AS max_thread_count
        FROM information_schema.tables
        WHERE settings['merge']['scheduler']['max_thread_count'] = 1
```

#### generate reset max_thread_count statements...

```sql
SELECT
  'ALTER TABLE "' || table_schema || '"."' || table_name || '" RESET ("merge.scheduler.max_thread_count");' AS reset_statement
FROM information_schema.tables
WHERE settings['merge']['scheduler']['max_thread_count'] = 1;

```
