#!/bin/bash
# 启动 Docker 服务脚本（Flink + PostgreSQL）

echo "=========================================="
echo "启动国网电力 CDC 示例服务"
echo "=========================================="

# 检查 Docker 是否运行
if ! docker info > /dev/null 2>&1; then
    echo "错误：Docker 未运行，请先启动 Docker"
    exit 1
fi

# 停止并删除旧容器
echo "停止旧容器..."
docker stop flink-jobmanager flink-taskmanager postgres 2>/dev/null
docker rm flink-jobmanager flink-taskmanager postgres 2>/dev/null

# 创建 Docker 网络
echo "创建 Docker 网络..."
docker network create stategrid-network 2>/dev/null || echo "网络已存在"

# 创建数据目录
mkdir -p /tmp/stategrid-flink/checkpoints
mkdir -p /tmp/stategrid-flink/savepoints

# 启动 PostgreSQL 容器
echo "启动 PostgreSQL..."
docker run -d \
    --name postgres \
    --network stategrid-network \
    -e POSTGRES_DB=stategrid_db \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres \
    -v /tmp/stategrid-postgres:/var/lib/postgresql/data \
    -p 5432:5432 \
    postgres:14 \
    -c wal_level=logical \
    -c max_replication_slots=10 \
    -c max_wal_senders=10 \
    -c max_replication_slots=4

# 等待 PostgreSQL 启动
echo "等待 PostgreSQL 启动..."
sleep 10

# 验证 PostgreSQL 连接
until docker exec postgres pg_isready -U postgres > /dev/null 2>&1; do
    echo "等待 PostgreSQL 就绪..."
    sleep 2
done
echo "PostgreSQL 已就绪！"

# 启动 Flink JobManager
echo "启动 Flink JobManager..."
docker run -d \
    --name flink-jobmanager \
    --network stategrid-network \
    -e JOB_MANAGER_RPC_ADDRESS=flink-jobmanager \
    -e FLINK_PROPERTIES="jobmanager.rpc.address: flink-jobmanager\njobmanager.rpc.port: 6123\njobmanager.memory.process.size: 1600m\ntaskmanager.memory.process.size: 1728m\ntaskmanager.numberOfTaskSlots: 4\nparallelism.default: 2\nstate.backend: rocksdb\nstate.checkpoints.dir: file:///opt/flink/checkpoints\nrest.address: 0.0.0.0\nrest.port: 8081" \
    -v /tmp/stategrid-flink/checkpoints:/opt/flink/checkpoints \
    -p 8081:8081 \
    flink:1.18.0 \
    jobmanager

# 等待 JobManager 启动
sleep 5

# 启动 Flink TaskManager
echo "启动 Flink TaskManager..."
docker run -d \
    --name flink-taskmanager \
    --network stategrid-network \
    -e JOB_MANAGER_RPC_ADDRESS=flink-jobmanager \
    -e FLINK_PROPERTIES="jobmanager.rpc.address: flink-jobmanager\njobmanager.rpc.port: 6123\ntaskmanager.numberOfTaskSlots: 4\nstate.backend: rocksdb\nstate.checkpoints.dir: file:///opt/flink/checkpoints" \
    -v /tmp/stategrid-flink/checkpoints:/opt/flink/checkpoints \
    flink:1.18.0 \
    taskmanager

# 等待 TaskManager 启动
sleep 5

# 复制 Flink CDC 连接器和 PostgreSQL JDBC 驱动到容器
echo "安装 Flink CDC 连接器..."
docker exec flink-jobmanager bash -c "mkdir -p /opt/flink/lib"

# 下载并安装 Flink CDC PostgreSQL 连接器
echo "下载 flink-connector-postgres-cdc..."
docker exec flink-jobmanager bash -c "cd /opt/flink/lib && \
    curl -L -o flink-connector-postgres-cdc-2.6.0.jar \
    https://repo1.maven.org/maven2/com/ververica/flink-connector-postgres-cdc/2.6.0/flink-connector-postgres-cdc-2.6.0.jar"

# 下载并安装 PostgreSQL JDBC 驱动
echo "下载 PostgreSQL JDBC 驱动..."
docker exec flink-jobmanager bash -c "cd /opt/flink/lib && \
    curl -L -o postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar"

echo "=========================================="
echo "服务启动完成！"
echo "=========================================="
echo "Flink Web UI: http://localhost:8081"
echo "PostgreSQL: localhost:5432"
echo "  数据库: stategrid_db"
echo "  用户名: postgres"
echo "  密码: postgres"
echo "=========================================="
echo ""
echo "下一步操作："
echo "1. 初始化 PostgreSQL 表: ./init-postgres-tables.sh"
echo "2. 创建 Flink 表: ./create-flink-tables.sh"
echo "3. 运行数据处理作业: ./run-all-jobs.sh"
