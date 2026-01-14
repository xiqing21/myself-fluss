#!/bin/bash

# ========================================
# 完整执行脚本：启动服务 -> 建表 -> 运行作业 -> 插入数据
# ========================================

echo "===== 开始执行完整流程 ====="

# 1. 启动 Fluss 和 Flink
echo "[1/4] 启动 Fluss 和 Flink..."
# 防止之前启动过，先都尝试停止 Fluss 和 Flink
bash /opt/fluss/bin/local-cluster.sh stop 2>/dev/null
bash /opt/flink/bin/stop-cluster.sh 2>/dev/null

rm -rf /tmp/fluss-*
rm -rf /tmp/zookeeper

# 启动 Fluss 和 Flink
bash /opt/fluss/bin/local-cluster.sh start
bash /opt/flink/bin/start-cluster.sh

echo "等待服务启动完成..."
sleep 10
echo "服务启动完成"

# 2. 创建 Fluss 表
echo "[2/4] 创建 Fluss 表..."
bash /opt/flink/bin/sql-client.sh -f /opt/data/prepare_table.sql
echo "表创建完成"

# 3. 启动 Delta Join 作业
echo "[3/4] 启动 Delta Join 作业..."
bash /opt/flink/bin/sql-client.sh -f /opt/data/run_delta_join.sql
echo "Delta Join 作业启动完成"

# 4. 插入数据
echo "[4/4] 插入数据..."
bash /opt/flink/bin/sql-client.sh -f /opt/data/insert_data.sql
echo "数据插入完成"

echo "===== 完整流程执行完毕 ====="
