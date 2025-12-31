# 国网电力数据 Flink CDC 实时数仓分层示例

本示例演示了如何使用 Flink CDC + PostgreSQL + 数仓分层（ODS/DWD/DWS）+ PostgreSQL Sink 构建国网电力数据的实时数据处理管道。

## 业务场景

模拟国家电网电力数据实时处理场景，包括：
- **电力用户信息**：用户ID、姓名、用电类型、地区等
- **电力消费数据**：消费ID、用户ID、用电量、消费金额、时间等
- **业务目标**：实时统计各地区的用电情况、用户用电排名等

## 数仓分层设计

```
ODS层（原始数据层）
├── ods_power_user（用户原始表）
└── ods_power_consumption（消费记录原始表）

DWD层（明细数据层）
├── dwd_power_user_info（用户明细）
├── dwd_power_consumption_detail（消费明细，关联用户信息）

DWS层（汇总数据层）
├── dws_region_daily_stats（地区日汇总）
└── dws_user_ranking（用户用电排名）
```

## 环境准备

### 1. 启动 Flink 和 PostgreSQL 容器

```bash
# 启动所有服务
./start-services.sh
```

这将启动：
- Flink（8081端口）
- PostgreSQL（5432端口，数据库：stategrid_db）

### 2. 初始化数据库表结构

```bash
# 创建 PostgreSQL 源表和 Sink 表
./init-postgres-tables.sh
```

### 3. 创建 Flink CDC 表结构

```bash
# 创建 Flink 表
./create-flink-tables.sh
```

## 运行步骤

### 方式一：使用提供的脚本自动执行

```bash
# 一键启动所有处理任务
./run-all-jobs.sh
```

### 方式二：分步手动执行

#### 步骤1：启动 CDC 同步作业（ODS → DWD）

```bash
# 创建 ODS → DWD 实时同步
./run-ods-to-dwd.sh
```

在 Flink UI（http://localhost:8081）可以看到 CDC 作业正在运行。

#### 步骤2：启动 DWD → DWS 聚合作业

```bash
# 创建 DWD → DWS 实时聚合
./run-dwd-to-dws.sh
```

#### 步骤3：插入测试数据

```bash
# 向 PostgreSQL 源表插入测试数据
./insert-test-data.sh
```

#### 步骤4：查看结果

连接 PostgreSQL 查询 Sink 表中的结果：

```bash
# 进入 PostgreSQL 容器
docker exec -it postgres psql -U postgres -d stategrid_db

# 查询地区日汇总
SELECT * FROM dws_region_daily_stats ORDER BY stat_date, region_id;

# 查询用户用电排名
SELECT * FROM dws_user_ranking ORDER BY stat_date, total_consumption DESC;
```

## 文件说明

### 文档
- `STATEGRID_EXAMPLE.md` - 详细示例文档，包含手工搭建环境步骤

### SQL 文件
- `init-postgres-tables.sql` - PostgreSQL 数据库初始化脚本
- `create-flink-tables.sql` - Flink CDC 表定义脚本
- `run-ods-to-dwd.sql` - ODS 到 DWD 层 CDC 同步 SQL
- `run-dwd-to-dws.sql` - DWD 到 DWS 层实时聚合 SQL
- `insert-test-data.sql` - 测试数据插入脚本

### Shell 脚本
- `start-services.sh` - 启动 Docker 服务（Flink + PostgreSQL）
- `init-postgres-tables.sh` - 初始化 PostgreSQL 表
- `create-flink-tables.sh` - 创建 Flink 表
- `run-ods-to-dwd.sh` - 运行 ODS→DWD 作业
- `run-dwd-to-dws.sh` - 运行 DWD→DWS 作业
- `insert-test-data.sh` - 插入测试数据
- `run-all-jobs.sh` - 一键运行所有作业
- `query-results.sh` - 查询处理结果
- `stop-services.sh` - 停止所有服务

## 快速开始

```bash
# 1. 启动服务
./start-services.sh

# 2. 初始化数据库
./init-postgres-tables.sh

# 3. 创建 Flink 表
./create-flink-tables.sh

# 4. 运行所有作业
./run-all-jobs.sh

# 5. 插入测试数据
./insert-test-data.sh

# 6. 查询结果
./query-results.sh
```

## 验证数据流

1. **源数据（ODS）**：PostgreSQL 的 `power_user` 和 `power_consumption` 表
2. **CDC 同步（ODS→DWD）**：Flink 实时同步到 `dwd_power_consumption_detail` 表
3. **实时聚合（DWD→DWS）**：Flink 实时聚合到 `dws_region_daily_stats` 和 `dws_user_ranking` 表
4. **结果写入**：PostgreSQL 的 Sink 表

## 注意事项

1. 确保 Docker 已安装并运行
2. 确保端口 8081（Flink）和 5432（PostgreSQL）未被占用
3. Flink CDC 需要的连接器依赖已在容器中预置
4. 测试数据插入后会自动触发 CDC 同步和聚合计算

## 故障排查

### Flink 作业失败
- 检查 Flink UI（http://localhost:8081）查看作业日志
- 确认 PostgreSQL 连接正常

### CDC 不同步
- 检查 PostgreSQL 的 CDC 插件是否已安装（wal2json）
- 确认数据库 replication slot 是否正常

### 数据不更新
- 确认源表数据是否发生变化
- 检查 Flink 作业状态是否为 RUNNING
