#!/bin/bash
# [3/5] 启动 DWS 层聚合
# Fluss DWD -> Fluss DWS (Aggregate)

echo "=========================================="
echo "[3/5] 启动 DWS 层聚合"
echo "=========================================="
echo "说明: 从 Fluss DWD 层读取数据,进行聚合计算后写入 Fluss DWS 层"
echo ""

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 启动 DWS 层作业
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/03-run-dws-aggregate.sql"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ DWS 层聚合作业启动成功！"
    echo "查看作业状态: http://localhost:8081"
else
    echo ""
    echo "✗ DWS 层聚合作业启动失败！"
    exit 1
fi

echo "=========================================="
