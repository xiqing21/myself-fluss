# 国网电力数据 Flink CDC 实时数仓分层实战示例

## 一、业务背景

国家电网作为电力行业的领军企业，每天产生海量的电力消费数据。本示例模拟国网电力数据的实时处理场景，通过 Flink CDC 实时捕获 PostgreSQL 源库中的数据变更，经过数仓分层处理（ODS → DWD → DWS），最终将聚合结果写入目标 PostgreSQL 数据库。

### 业务场景

1. **用户管理**：管理电力用户基本信息（用户ID、姓名、用电类型、所属地区等）
2. **消费记录**：记录用户每次的用电量和消费金额
3. **实时统计**：实时统计各地区用电总量、用户用电排名等指标

### 数仓分层设计

```
ODS层（原始数据层）
├── ods_power_user（用户原始表）- 通过 CDC 从源库实时同步
└── ods_power_consumption（消费记录原始表）- 通过 CDC 从源库实时同步

DWD层（明细数据层）
├── dwd_power_user_info（用户明细）
└── dwd_power_consumption_detail（消费明细，关联用户信息，添加维度）

DWS层（汇总数据层）
├── dws_region_daily_stats（地区日汇总：地区ID、日期、总用电量、总金额、用户数）
└── dws_user_ranking（用户用电排名：日期、用户ID、地区、总用电量、排名）
```

## 二、技术架构

```
┌─────────────────┐
│  PostgreSQL     │ ─── CDC ───┐
│  (源数据库)      │            │
└─────────────────┘            │
                              ▼
                    ┌─────────────────────┐
                    │  Apache Flink 1.18  │
                    │                     │
                    │  ┌─────────────┐   │
                    │  │ CDC Source │   │
                    │  │ (PostgreSQL)│   │
                    │  └──────┬──────┘   │
                    │         │           │
                    │         ▼           │
                    │  ┌─────────────┐   │
                    │  │ ODS Layer   │   │
                    │  └──────┬──────┘   │
                    │         │           │
                    │         ▼           │
                    │  ┌─────────────┐   │
                    │  │ DWD Layer   │   │
                    │  │ (Join +     │   │
                    │  │ Transform)  │   │
                    │  └──────┬──────┘   │
                    │         │           │
                    │         ▼           │
                    │  ┌─────────────┐   │
                    │  │ DWS Layer   │   │
                    │  │ (Agg +      │   │
                    │  │ Window)     │   │
                    │  └──────┬──────┘   │
                    │         │           │
                    │         ▼           │
                    │  ┌─────────────┐   │
                    │  │ Postgres    │   │
                    │  │ Sink        │   │
                    │  └──────┬──────┘   │
                    └─────────┼──────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │  PostgreSQL     │
                    │  (目标数据库)    │
                    └─────────────────┘
```

## 三、方式一：使用 Docker 环境测试（推荐）

### 1. 环境准备

- 类 Unix 操作系统（Linux、Mac OS X）
- 内存建议至少 8 GB，磁盘建议至少 10 GB
- Docker 已安装并运行

### 2. 启动服务

在项目根目录执行：

```bash
# 启动 Flink 和 PostgreSQL 服务
./start-services.sh
```

这将启动：
- **Flink 1.18**：包含 CDC PostgreSQL 连接器、JDBC Sink
- **PostgreSQL 14**：配置了 CDC 插件（wal2json），预置了测试数据

服务端口：
- Flink Web UI：http://localhost:8081
- PostgreSQL：localhost:5432
  - 用户名：postgres
  - 密码：postgres
  - 数据库：stategrid_db

### 3. 初始化数据库

```bash
# 创建 PostgreSQL 源表和 Sink 表
./init-postgres-tables.sh
```

这会创建：
- 源表：`power_user`、`power_consumption`
- Sink 表：`dwd_power_consumption_detail`、`dws_region_daily_stats`、`dws_user_ranking`

### 4. 创建 Flink 表

```bash
# 创建 Flink CDC 表和 Sink 表
./create-flink-tables.sh
```

### 5. 启动数据处理作业

#### 方式A：一键启动（推荐）

```bash
# 启动所有数据处理作业
./run-all-jobs.sh
```

#### 方式B：分步启动

```bash
# 1. 启动 ODS → DWD CDC 同步作业
./run-ods-to-dwd.sh

# 2. 启动 DWD → DWS 聚合作业
./run-dwd-to-dws.sh
```

### 6. 插入测试数据

```bash
# 向源表插入测试数据
./insert-test-data.sh
```

### 7. 观察结果

**方式1：通过 Flink UI 观察**

访问 http://localhost:8081，可以看到：
- CDC 同步作业正在运行
- 聚合作业正在处理数据
- 数据流处理状态

**方式2：通过 PostgreSQL 查询**

```bash
# 进入 PostgreSQL 容器
docker exec -it postgres psql -U postgres -d stategrid_db

# 查询 DWD 层明细数据
SELECT * FROM dwd_power_consumption_detail ORDER BY consumption_date;

# 查询 DWS 层地区日汇总
SELECT * FROM dws_region_daily_stats ORDER BY stat_date, region_id;

# 查询 DWS 层用户排名
SELECT * FROM dws_user_ranking ORDER BY stat_date, total_consumption DESC LIMIT 10;
```

### 8. 停止服务

```bash
# 停止所有服务
./stop-services.sh
```

## 四、方式二：手工搭建环境测试

### 1. 环境准备

#### 1.1 软件要求

- Java 11 或更高版本
- Apache Flink 1.18+
- PostgreSQL 14+（配置 wal2json 插件）
- Maven 3.6+

#### 1.2 下载依赖

**Flink CDC 连接器**

下载 Flink CDC PostgreSQL 连接器：

```bash
# 从 Maven Central 下载
wget https://repo1.maven.org/maven2/com/ververica/flink-connector-postgres-cdc/2.6.0/flink-connector-postgres-cdc-2.6.0.jar

# 复制到 Flink lib 目录
cp flink-connector-postgres-cdc-2.6.0.jar $FLINK_HOME/lib/
```

**PostgreSQL JDBC 驱动**

```bash
wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar
cp postgresql-42.6.0.jar $FLINK_HOME/lib/
```

#### 1.3 配置 PostgreSQL CDC

1. 启用 PostgreSQL WAL 日志

编辑 `postgresql.conf`：

```conf
wal_level = logical
max_replication_slots = 4
max_wal_senders = 4
```

2. 安装 wal2json 插件

```bash
# Debian/Ubuntu
apt-get install postgresql-14-wal2json

# 或从源码编译
git clone https://github.com/eulerto/wal2json.git
cd wal2json
make
sudo make install PG_CONFIG=/usr/lib/postgresql/14/bin/pg_config
```

3. 重启 PostgreSQL

```bash
sudo systemctl restart postgresql
```

### 2. 服务启动

#### 2.1 启动 Flink

```bash
cd $FLINK_HOME
./bin/start-cluster.sh
```

检查 http://localhost:8081 是否可访问。

#### 2.2 启动 PostgreSQL

```bash
sudo systemctl start postgresql
```

### 3. 创建数据库和表

#### 3.1 创建数据库

```bash
psql -U postgres
```

```sql
CREATE DATABASE stategrid_db;
\c stategrid_db

-- 创建 CDC 用户
CREATE USER cdc_user WITH PASSWORD 'cdc_password';
GRANT REPLICATION TO cdc_user;
GRANT ALL PRIVILEGES ON DATABASE stategrid_db TO cdc_user;
```

#### 3.2 创建源表

执行 `init-postgres-tables.sql` 文件：

```bash
psql -U postgres -d stategrid_db -f init-postgres-tables.sql
```

### 4. 创建 Flink 表和启动作业

#### 4.1 创建 Flink 表

```bash
cd $FLINK_HOME
./bin/sql-client.sh -f /path/to/stategrid_power_cdc/create-flink-tables.sql
```

#### 4.2 启动 ODS → DWD 作业

```bash
./bin/sql-client.sh -f /path/to/stategrid_power_cdc/run-ods-to-dwd.sql
```

#### 4.3 启动 DWD → DWS 作业

```bash
./bin/sql-client.sh -f /path/to/stategrid_power_cdc/run-dwd-to-dws.sql
```

### 5. 插入测试数据

```bash
psql -U postgres -d stategrid_db -f insert-test-data.sql
```

### 6. 观察结果

参考"方式一"中的观察步骤。

## 五、关键 SQL 说明

### 5.1 ODS 层 CDC 配置

```sql
CREATE TABLE ods_power_user (
    user_id BIGINT,
    user_name STRING,
    usage_type STRING,
    region_id INT,
    region_name STRING,
    update_time TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'postgres',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'stategrid_db',
    'table-name' = 'power_user',
    'slot.name' = 'flink_stategrid_cdc',
    'decoding.plugin.name' = 'wal2json'
);
```

### 5.2 DWD 层关联查询

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

### 5.3 DWS 层聚合计算

```sql
-- 地区日汇总
INSERT INTO dws_region_daily_stats
SELECT
    region_id,
    region_name,
    TUMBLE_START(consumption_date, INTERVAL '1' DAY) AS stat_date,
    SUM(consumption_amount) AS total_consumption,
    SUM(consumption_cost) AS total_cost,
    COUNT(DISTINCT user_id) AS user_count,
    MAX(consumption_amount) AS max_consumption
FROM dwd_power_consumption_detail
GROUP BY
    region_id,
    region_name,
    TUMBLE(consumption_date, INTERVAL '1' DAY);

-- 用户用电排名
INSERT INTO dws_user_ranking
SELECT
    user_id,
    user_name,
    region_id,
    region_name,
    TUMBLE_START(consumption_date, INTERVAL '1' DAY) AS stat_date,
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

## 六、性能优化建议

### 6.1 并发度配置

```sql
-- 在作业启动时设置并行度
SET 'parallelism.default' = '4';
```

### 6.2 Checkpoint 配置

```sql
-- 启用精确一次语义
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.timeout' = '5min';
SET 'state.backend' = 'rocksdb';
```

### 6.3 PostgreSQL Sink 批量写入

```sql
'sink.buffer-flush.max-size' = '10mb',
'sink.buffer-flush.max-rows' = '1000',
'sink.buffer-flush.interval' = '5s'
```

## 七、监控与运维

### 7.1 监控指标

- Flink 作业状态：RUNNING、FAILED 等
- 数据延迟：Source 到 Sink 的延迟时间
- Checkpoint 成功率：确保数据一致性
- 数据吞吐量：TPS（每秒处理事务数）

### 7.2 日志查看

```bash
# Flink 日志
tail -f $FLINK_HOME/log/flink-*.log

# PostgreSQL 日志
tail -f /var/log/postgresql/postgresql-14-main.log
```

## 八、常见问题

### Q1: CDC 无法同步数据

**原因**：
- PostgreSQL replication slot 不存在或已被占用
- WAL 日志级别未设置为 logical

**解决**：
```sql
-- 检查 replication slots
SELECT * FROM pg_replication_slots;

-- 删除旧 slot
SELECT pg_drop_replication_slot('flink_stategrid_cdc');
```

### Q2: Flink 作业频繁重启

**原因**：
- 内存不足
- Checkpoint 失败

**解决**：
```bash
# 增加 TaskManager 内存
export FLINK_TM_HEAP="2048m"
```

### Q3: 数据更新延迟高

**原因**：
- Source 端数据产生慢
- Sink 端写入瓶颈

**解决**：
- 增加 Sink 并行度
- 优化批量写入参数

## 九、扩展场景

本示例可扩展到以下国网业务场景：

1. **实时负荷监控**：监控各区域的电力负荷
2. **异常用电检测**：实时检测用电异常行为
3. **分布式能源管理**：管理太阳能、风能等分布式能源
4. **智能计量**：智能电表数据的实时处理
5. **需求响应**：实时响应电力需求变化

## 十、参考资料

- Flink CDC 文档：https://ververica.github.io/flink-cdc-connectors/
- Flink SQL 文档：https://nightlies.apache.org/flink/flink-docs-release-1.18/
- PostgreSQL CDC：https://www.postgresql.org/docs/current/logicaldecoding.html
