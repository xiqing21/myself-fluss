# 国网电力数据 Flink CDC + Fluss 分层 + PostgreSQL Sink 实时数仓示例

本示例演示了如何使用 Flink CDC 从 PostgreSQL 捕获电力数据，通过 Fluss 进行数仓分层（ODS→DWD→DWS→ADS），最终将结果写入 PostgreSQL Sink，并使用 Python 实现 CRUD 和性能监控。

## 技术栈

- **Flink 2.2.0**：流计算引擎
- **Fluss 0.8.0**：流存储引擎，用于数仓分层
- **Flink CDC 3.5.0**：PostgreSQL CDC 连接器
- **PostgreSQL JDBC 42.7.1**：JDBC Sink
- **Python 3**：CRUD 和性能监控

## 数仓分层架构

```
PostgreSQL (Source)
      ↓ CDC
    [ODS层 - Fluss]
  ├── ods_power_user
  └── ods_power_consumption
      ↓ Join/Transform
    [DWD层 - Fluss]
  └── dwd_power_consumption_detail
      ↓ Aggregate
    [DWS层 - Fluss]
  ├── dws_region_daily_stats
  └── dws_user_ranking
      ↓ Final Transform
    [ADS层 - Fluss]
  └── ads_power_dashboard
      ↓ JDBC Sink
PostgreSQL (Sink)
```

## 快速开始

### 前置条件

- PostgreSQL 已启动（当前环境已运行）
- Flink 和 Fluss 服务正常
- 已安装 Python 3 和依赖包

### 1. 启动 Flink 和 Fluss

```bash
cd /opt/data
./start-flink-fluss.sh
```

### 2. 创建 PostgreSQL 数据库和配置 CDC

```bash
cd /opt/data/stategrid_power_cdc
./init-postgres-and-cdc.sh
```

这会：
- 创建 `stategrid_db` 数据库
- 创建源表和 Sink 表
- 配置 PostgreSQL CDC（wal_level、replication slot）

### 3. 创建 Fluss 分层表

```bash
./create-fluss-tables.sh
```

创建 ODS、DWD、DWS、ADS 层的 Fluss 表。

### 4. 启动数据处理作业

#### 方式一：一键启动所有作业（推荐）

```bash
./run-all-jobs.sh
```

#### 方式二：分层启动（逐步调试）

```bash
# [1/5] 启动 ODS 层 CDC 同步
./01-run-ods-cdc.sh

# [2/5] 启动 DWD 层转换
./02-run-dwd-transform.sh

# [3/5] 启动 DWS 层聚合
./03-run-dws-aggregate.sh

# [4/5] 启动 ADS 层转换
./04-run-ads-transform.sh

# [5/5] 启动 Sink 层写入
./05-run-fluss-to-postgres.sh
```

这将启动：
- ODS 层 CDC 同步（PostgreSQL → Fluss）
- DWD 层关联处理（ODS → DWD）
- DWS 层聚合统计（DWD → DWS）
- ADS 层结果处理（DWS → ADS）
- Sink 层写入（ADS → PostgreSQL）

### 5. 安装 Python 依赖

```bash
pip install -r requirements.txt
```

### 6. 运行性能监控和 CRUD

```bash
# 终端1：启动性能监控
python monitor_performance.py

# 终端2：执行 CRUD 测试
python test_crud.py
```

## 文件说明

### SQL 文件

- `01-run-ods-cdc.sql` - ODS 层 CDC 同步（PostgreSQL → Fluss）
- `02-run-dwd-transform.sql` - DWD 层转换（ODS → DWD）
- `03-run-dws-aggregate.sql` - DWS 层聚合（DWD → DWS）
- `04-run-ads-transform.sql` - ADS 层转换（DWS → ADS）
- `05-run-fluss-to-postgres.sql` - Sink 层（Fluss → PostgreSQL）
- `init-postgres-tables.sql` - PostgreSQL 源表和 Sink 表初始化
- `configure-postgres-cdc.sql` - PostgreSQL CDC 配置
- `create-fluss-tables.sql` - Fluss 分层表定义（ODS/DWD/DWS/ADS）

### Shell 脚本

- `check-jobs.sh` - 验证 Flink 作业状态（通过 REST API）
- `01-run-ods-cdc.sh` - 启动 ODS 层 CDC 同步作业
- `02-run-dwd-transform.sh` - 启动 DWD 层转换作业
- `03-run-dws-aggregate.sh` - 启动 DWS 层聚合作业
- `04-run-ads-transform.sh` - 启动 ADS 层转换作业
- `05-run-fluss-to-postgres.sh` - 启动 Sink 层作业
- `run-all-jobs.sh` - 一键启动所有作业
- `init-postgres-and-cdc.sh` - 初始化 PostgreSQL 和 CDC
- `create-fluss-tables.sh` - 创建 Fluss 分层表

### Python 脚本

- `monitor_performance.py` - 性能监控（延迟、吞吐量）
- `test_crud.py` - CRUD 测试和性能验证
- `insert-test-data.py` - 批量插入测试数据
- `requirements.txt` - Python 依赖

## 数据流说明

### 1. ODS 层（原始数据层）

通过 Flink CDC 实时捕获 PostgreSQL 源表数据，写入 Fluss ODS 层。

### 2. DWD 层（明细数据层）

关联用户维度信息，生成完整的消费明细。

### 3. DWS 层（汇总数据层）

按地区和日期聚合，生成汇总统计和用户排名。

### 4. ADS 层（应用数据层）

面向应用的最终数据，包含仪表盘所需的各类指标。

### 5. Sink 层

将 ADS 层数据写入 PostgreSQL，供业务系统使用。

## 性能监控

### 延迟监控

监控数据从 PostgreSQL Source 到 PostgreSQL Sink 的端到端延迟。

### 吞吐量监控

监控每秒处理的记录数（TPS）。

### 监控指标

- E2E Latency（端到端延迟）
- Records/sec（每秒记录数）
- Checkpoint 成功率
- Flink 作业状态

## 验证数据

### 查询源表数据

```sql
-- PostgreSQL Source
SELECT * FROM power_user LIMIT 10;
SELECT * FROM power_consumption ORDER BY consumption_date DESC LIMIT 10;
```

### 查询 Fluss 分层数据

```sql
-- Fluss ODS
SELECT * FROM fluss_catalog.stategrid_db.ods_power_user;
SELECT * FROM fluss_catalog.stategrid_db.ods_power_consumption;

-- Fluss DWD
SELECT * FROM fluss_catalog.stategrid_db.dwd_power_consumption_detail;

-- Fluss DWS
SELECT * FROM fluss_catalog.stategrid_db.dws_region_daily_stats;
SELECT * FROM fluss_catalog.stategrid_db.dws_user_ranking;

-- Fluss ADS
SELECT * FROM fluss_catalog.stategrid_db.ads_power_dashboard;
```

### 查询 Sink 表数据

```sql
-- PostgreSQL Sink
SELECT * FROM ads_power_dashboard ORDER BY stat_date, region_id;
```

## 性能目标

- **端到端延迟**：< 1 秒
- **吞吐量**：> 1000 TPS
- **Checkpoint 成功率**：> 99%

## 常见问题

### Q1: CDC 不同步数据
- 检查 PostgreSQL WAL 配置：`wal_level = logical`
- 检查 replication slots
- 验证数据库连接信息

### Q2: Fluss 表写入失败
- 检查 Fluss 服务状态
- 确认 Fluss 连接器版本兼容

### Q3: 性能不达标
- 增加 Flink 并行度
- 优化 Fluss bucket key
- 调整批量写入参数

### Q4: SQL Client 执行成功但作业失败

**重要说明：**

1. **SQL Client 退出码不可靠**
   - `bash /opt/flink/bin/sql-client.sh -f xxx.sql` 的退出码 `$?` 不能准确反映作业是否成功
   - 即使作业启动失败，SQL Client 可能仍返回退出码 0

2. **正确验证方式**
   - 使用 `check-jobs.sh` 脚本验证作业状态
   - 或访问 Flink Web UI 检查作业状态
   - Flink Web UI: http://localhost:8081
   - 检查作业是否为 `RUNNING` 状态
   - 检查是否有异常日志

3. **使用 check-jobs.sh**

```bash
# 检查所有作业
./check-jobs.sh

# 检查特定层作业
./check-jobs.sh "ODS"
./check-jobs.sh "DWD"
./check-jobs.sh "DWS"
./check-jobs.sh "ADS"
./check-jobs.sh "Sink"
```

**说明：**
- `check-jobs.sh` 通过 Flink REST API 获取作业真实状态
- 显示每个作业的 ID、名称和状态
- 失败时显示异常信息
- 退出码 0 表示成功，1 表示有作业失败

4. **常见错误**
   - `Cannot find table`：Fluss Catalog 未创建或表不存在
   - `ValidationException`：SQL 语法错误或表结构不匹配
   - `Connection refused`：服务未启动或端口错误

5. **Catalog 创建**
   - 每个 SQL 文件都会创建 `fluss_catalog`（幂等操作，不会报错）
   - 使用 `USE CATALOG fluss_catalog` 切换
   - 如果出现 Catalog 不存在的错误，检查 Fluss 服务是否启动

## 扩展场景

本架构可扩展到以下国网业务场景：
1. **实时负荷监控**：监控各区域电力负荷
2. **智能计量**：智能电表数据实时处理
3. **需求响应**：实时响应电力需求变化
4. **分布式能源**：太阳能、风能等分布式能源管理
