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
echo "正在执行 SQL 脚本..."
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/03-run-dws-aggregate.sql"

# 等待作业提交
sleep 5

echo ""
echo "=========================================="
echo "验证作业状态..."
echo "=========================================="

# 使用 check-jobs.sh 验证作业状态
bash "$SCRIPT_DIR/check-jobs.sh" "DWS"

if [ $? -eq 0 ]; then
    echo ""
    echo -e "\033[0;32m✓\033[0m DWS 层作业运行正常"
    echo "查看作业状态: http://localhost:8081"
else
    echo ""
    echo -e "\033[0;31m✗\033[0m DWS 层作业启动失败！"
    echo "请检查 Flink Web UI: http://localhost:8081"
    exit 1
fi
echo "=========================================="

echo "=========================================="
