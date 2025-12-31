#!/bin/bash
# 一键运行所有数据处理作业

echo "=========================================="
echo "启动所有数据处理作业"
echo "=========================================="

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 启动各层作业
echo ""
echo "[1/5] 启动 ODS 层 CDC 同步..."
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/run-ods-cdc.sql"
sleep 3

echo ""
echo "[2/5] 启动 DWD 层转换..."
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/run-dwd-transform.sql"
sleep 3

echo ""
echo "[3/5] 启动 DWS 层聚合..."
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/run-dws-aggregate.sql"
sleep 3

echo ""
echo "[4/5] 启动 ADS 层转换..."
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/run-ads-transform.sql"
sleep 3

echo ""
echo "[5/5] 启动 Sink 层 (Fluss -> PostgreSQL)..."
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/run-fluss-to-postgres.sql"
sleep 3

echo ""
echo "=========================================="
echo "所有作业启动完成！"
echo "=========================================="
echo ""
echo "作业列表："
echo "  1. ODS: PostgreSQL CDC -> Fluss"
echo "  2. DWD: Fluss ODS -> Fluss DWD (Join)"
echo "  3. DWS: Fluss DWD -> Fluss DWS (Aggregate)"
echo "  4. ADS: Fluss DWS -> Fluss ADS"
echo "  5. Sink: Fluss ADS -> PostgreSQL"
echo ""
echo "查看作业状态：http://localhost:8081"
echo ""
echo "下一步："
echo "  ./insert-test-data.sh"
echo "  python monitor_performance.py"
echo "  python test_crud.py"
echo "=========================================="
