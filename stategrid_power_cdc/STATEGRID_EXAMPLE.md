# 国网电力数据 Flink CDC 实时数仓分层实战示例

## 一、业务背景

本示例演示如何使用 Flink CDC 从 PostgreSQL 源库实时捕获电力数据变更，经过数仓分层处理（ODS → DWD → DWS），最终将聚合结果写入目标 PostgreSQL 数据库。

### 数仓分层设计

```
ODS层（原始数据层）
├── ods_power_user（用户原始表）- CDC 实时同步
└── ods_power_consumption（消费记录原始表）- CDC 实时同步

DWD层（明细数据层）
└── dwd_power_consumption_detail（消费明细，关联用户维度）

DWS层（汇总数据层）
├── dws_region_daily_stats（地区日汇总）
└── dws_user_ranking（用户用电排名）
```

## 二、快速开始

### 前置要求

当前 Docker 环境已预装：
- Flink 1.18+（/opt/flink）
- PostgreSQL（已配置 wal2json 插件）
- Flink CDC PostgreSQL 连接器（/opt/flink/lib）

### 启动步骤

#### 1. 启动 Flink 和 PostgreSQL

```bash
cd /opt/data
./start-flink-fluss.sh
```

#### 2. 初始化 PostgreSQL 表

```bash
cd /opt/data/stategrid_power_cdc
./init-postgres-tables.sh
```

这会创建：
- 源表：`power_user`、`power_consumption`
- Sink 表：`dwd_power_consumption_detail`、`dws_region_daily_stats`、`dws_user_ranking`

#### 3. 创建 Flink CDC 表

```bash
./create-flink-tables.sh
```

#### 4. 启动数据处理作业

```bash
# 一键启动所有作业（推荐）
./run-all-jobs.sh
```

或分步启动：

```bash
./run-ods-to-dwd.sh    # ODS → DWD
./run-dwd-to-dws.sh    # DWD → DWS
```

#### 5. 插入测试数据

```bash
./insert-test-data.sh
```

#### 6. 查询结果

```bash
./query-results.sh
```

## 三、数据流说明

### 1. CDC 源表配置

```sql
CREATE TABLE ods_power_user (
    user_id BIGINT,
    user_name STRING,
    usage_type STRING,
    region_id INT,
    region_name STRING,
    ...
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'localhost',
    'port' = '5432',
    'database-name' = 'stategrid_db',
    'slot.name' = 'flink_stategrid_user',
    'decoding.plugin.name' = 'wal2json'
);
```

### 2. ODS → DWD：关联查询

```sql
INSERT INTO dwd_power_consumption_detail
SELECT
    c.consumption_id,
    c.user_id,
    u.user_name,
    c.consumption_amount,
    c.consumption_cost,
    c.consumption_date,
    u.region_id,
    u.region_name,
    u.usage_type,
    CURRENT_TIMESTAMP AS etl_time
FROM ods_power_consumption AS c
    LEFT JOIN ods_power_user AS u
        ON c.user_id = u.user_id;
```

### 3. DWD → DWS：聚合统计

**地区日汇总：**

```sql
INSERT INTO dws_region_daily_stats
SELECT
    region_id,
    region_name,
    CAST(TUMBLE_START(consumption_date, INTERVAL '1' DAY) AS DATE) AS stat_date,
    SUM(consumption_amount) AS total_consumption,
    SUM(consumption_cost) AS total_cost,
    COUNT(DISTINCT user_id) AS user_count,
    ROUND(AVG(consumption_amount), 2) AS avg_consumption,
    MAX(consumption_amount) AS max_consumption,
    MIN(consumption_amount) AS min_consumption
FROM dwd_power_consumption_detail
GROUP BY
    region_id,
    region_name,
    TUMBLE(consumption_date, INTERVAL '1' DAY);
```

**用户用电排名：**

```sql
INSERT INTO dws_user_ranking
SELECT
    user_id,
    user_name,
    region_id,
    region_name,
    CAST(TUMBLE_START(consumption_date, INTERVAL '1' DAY) AS DATE) AS stat_date,
    SUM(consumption_amount) AS total_consumption,
    SUM(consumption_cost) AS total_cost,
    ROW_NUMBER() OVER (
        PARTITION BY TUMBLE(consumption_date, INTERVAL '1' DAY), region_id
        ORDER BY SUM(consumption_amount) DESC
    ) AS ranking
FROM dwd_power_consumption_detail
GROUP BY
    user_id,
    user_name,
    region_id,
    region_name,
    TUMBLE(consumption_date, INTERVAL '1' DAY);
```

## 四、监控与验证

### Flink Web UI

访问 http://localhost:8081 查看：
- 作业运行状态（RUNNING/FAILED）
- 数据处理吞吐量
- Checkpoint 状态

### 查询结果

```bash
# 查询 DWD 层数据
psql -h localhost -U postgres -d stategrid_db -c \
  "SELECT * FROM dwd_power_consumption_detail ORDER BY consumption_date DESC LIMIT 10;"

# 查询地区汇总
psql -h localhost -U postgres -d stategrid_db -c \
  "SELECT * FROM dws_region_daily_stats ORDER BY stat_date, region_id;"

# 查询用户排名
psql -h localhost -U postgres -d stategrid_db -c \
  "SELECT * FROM dws_user_ranking ORDER BY stat_date, ranking LIMIT 10;"
```

## 五、性能优化

### 并发度配置

在 `create-flink-tables.sql` 中设置：

```sql
SET 'parallelism.default' = '4';
```

### Checkpoint 配置

```sql
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '10s';
SET 'state.backend' = 'rocksdb';
```

### PostgreSQL Sink 批量写入

```sql
'sink.buffer-flush.max-size' = '10mb',
'sink.buffer-flush.max-rows' = '1000',
'sink.buffer-flush.interval' = '5s'
```

## 六、故障排查

### CDC 不同步数据

1. 检查 PostgreSQL WAL 配置：
   ```sql
   SHOW wal_level;  -- 应为 logical
   ```

2. 检查 replication slots：
   ```sql
   SELECT * FROM pg_replication_slots;
   ```

3. 删除旧 slot：
   ```sql
   SELECT pg_drop_replication_slot('flink_stategrid_user');
   ```

### Flink 作业频繁重启

1. 检查 Flink 日志：`tail -f /opt/flink/log/flink-*.log`
2. 检查 PostgreSQL 连接
3. 增加内存配置

## 七、参考资料

- Flink CDC 文档：https://ververica.github.io/flink-cdc-connectors/
- Flink SQL 文档：https://nightlies.apache.org/flink/flink-docs-release-1.18/
- PostgreSQL CDC：https://www.postgresql.org/docs/current/logicaldecoding.html
