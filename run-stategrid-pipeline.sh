#!/bin/bash
# 完整执行脚本：从启动服务到运行完整的 CDC 数据管道 (ODS -> DWD -> DWS -> ADS -> PostgreSQL)
# 包含：启动 Fluss + Flink、初始化 PostgreSQL、创建表、运行 5 个处理作业

echo "=========================================="
echo "国网电力 CDC 完整数据管道执行脚本"
echo "=========================================="
echo "执行流程："
echo "  1. 启动 Fluss 和 Flink"
echo "  2. 创建 Fluss 分层表"
echo "  3. [1/5] ODS 层 CDC 同步"
echo "  4. [2/5] DWD 层转换"
echo "  5. [3/5] DWS 层聚合"
echo "  6. [4/5] ADS 层转换"
echo "  7. [5/5] Sink 层 (Fluss -> PostgreSQL)"
echo "=========================================="
echo ""

# 获取脚本所在目录
SCRIPT_DIR="/opt/data/stategrid_power_cdc"

# 阶段 1: 启动 Fluss 和 Flink
echo "=========================================="
echo "[阶段 1/7] 启动 Fluss 和 Flink"
echo "=========================================="
echo "防止之前启动过，先停止 Fluss 和 Flink..."
bash /opt/fluss/bin/local-cluster.sh stop
bash /opt/flink/bin/stop-cluster.sh

rm -rf /tmp/fluss-*
rm -rf /tmp/zookeeper

echo "启动 Fluss 和 Flink..."
bash /opt/fluss/bin/local-cluster.sh start
bash /opt/flink/bin/start-cluster.sh

if [ $? -eq 0 ]; then
    echo -e "\033[0;32m✓\033[0m Fluss 和 Flink 启动成功"
else
    echo -e "\033[0;31m✗\033[0m Fluss 和 Flink 启动失败"
    exit 1
fi
echo ""

# 阶段 2: 创建 Fluss 分层表
echo "=========================================="
echo "[阶段 2/7] 创建 Fluss 分层表"
echo "=========================================="
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/create-fluss-tables.sql"

if [ $? -eq 0 ]; then
    echo -e "\033[0;32m✓\033[0m Fluss 分层表创建成功"
else
    echo "错误：Fluss 表创建失败"
    exit 1
fi
echo ""

# 阶段 3: [1/5] 启动 ODS 层 CDC 同步
echo "=========================================="
echo "[阶段 3/7] [1/5] 启动 ODS 层 CDC 同步"
echo "=========================================="
echo "说明: 将 PostgreSQL 原始数据实时同步到 Fluss ODS 层"
echo ""
echo "正在执行 SQL 脚本..."
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/01-run-ods-cdc.sql"

sleep 5
echo -e "\033[0;32m✓\033[0m ODS 层作业已提交"
echo ""

# 阶段 4: [2/5] 启动 DWD 层转换
echo "=========================================="
echo "[阶段 4/7] [2/5] 启动 DWD 层转换"
echo "=========================================="
echo "说明: 从 Fluss ODS 层读取数据,进行 Join 转换后写入 Fluss DWD 层"
echo ""
echo "正在执行 SQL 脚本..."
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/02-run-dwd-transform.sql"

sleep 5
echo -e "\033[0;32m✓\033[0m DWD 层作业已提交"
echo ""

# 阶段 5: [3/5] 启动 DWS 层聚合
echo "=========================================="
echo "[阶段 5/7] [3/5] 启动 DWS 层聚合"
echo "=========================================="
echo "说明: 从 Fluss DWD 层读取数据,进行聚合计算后写入 Fluss DWS 层"
echo ""
echo "正在执行 SQL 脚本..."
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/03-run-dws-aggregate.sql"

sleep 5
echo -e "\033[0;32m✓\033[0m DWS 层作业已提交"
echo ""

# 阶段 6: [4/5] 启动 ADS 层转换
echo "=========================================="
echo "[阶段 6/7] [4/5] 启动 ADS 层转换"
echo "=========================================="
echo "说明: 从 Fluss DWS 层读取数据,进行最终转换后写入 Fluss ADS 层"
echo ""
echo "正在执行 SQL 脚本..."
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/04-run-ads-transform.sql"

sleep 5
echo -e "\033[0;32m✓\033[0m ADS 层作业已提交"
echo ""

# 阶段 7: [5/5] 启动 Sink 层 (Fluss -> PostgreSQL)
echo "=========================================="
echo "[阶段 7/7] [5/5] 启动 Sink 层 (Fluss -> PostgreSQL)"
echo "=========================================="
echo "说明: 将 Fluss ADS 层数据同步回 PostgreSQL"
echo ""
echo "正在执行 SQL 脚本..."
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/05-run-fluss-to-postgres.sql"

sleep 5
echo -e "\033[0;32m✓\033[0m Sink 层作业已提交"
echo ""

# 执行完成
echo ""
echo "=========================================="
echo -e "\033[0;32m✓✓✓ 所有阶段执行完成！\033[0m"
echo "=========================================="
echo ""
echo "数据管道已成功启动："
echo "  Fluss Web UI: http://localhost:8180"
echo "  Flink Web UI: http://localhost:8081"
echo ""
echo "作业完成后可以执行："
echo "  cd /opt/data/stategrid_power_cdc"
echo "  ./insert-test-data.sh    - 插入测试数据"
echo "  python monitor_performance.py  - 监控性能"
echo "  python test_crud.py       - 测试 CRUD 操作"
echo ""
echo "数据分层："
echo "  ODS 层: ods_power_user, ods_power_consumption"
echo "  DWD 层: dwd_power_consumption_detail"
echo "  DWS 层: dws_region_daily_stats, dws_user_ranking"
echo "  ADS 层: ads_power_dashboard"
echo "  Sink: PostgreSQL ads_power_dashboard"
echo "=========================================="
