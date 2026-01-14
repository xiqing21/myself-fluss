#!/bin/bash
# [4/5] 启动 ADS 层转换
# Fluss DWS -> Fluss ADS

echo "=========================================="
echo "[4/5] 启动 ADS 层转换"
echo "=========================================="
echo "说明: 从 Fluss DWS 层读取数据,进行最终转换后写入 Fluss ADS 层"
echo ""

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 启动 ADS 层作业
echo "正在执行 SQL 脚本..."
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/04-run-ads-transform.sql"

# 等待作业提交
sleep 5

echo ""
echo "=========================================="
echo "验证作业状态..."
echo "=========================================="

# 使用 check-jobs.sh 验证作业状态
bash "$SCRIPT_DIR/check-jobs.sh" "ADS"

if [ $? -eq 0 ]; then
    echo ""
    echo -e "\033[0;32m✓\033[0m ADS 层作业运行正常"
    echo "查看作业状态: http://localhost:8081"
else
    echo ""
    echo -e "\033[0;31m✗\033[0m ADS 层作业启动失败！"
    echo "请检查 Flink Web UI: http://localhost:8081"
    exit 1
fi
echo "=========================================="

echo "=========================================="
