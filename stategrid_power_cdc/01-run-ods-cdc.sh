#!/bin/bash
# [1/5] 启动 ODS 层 CDC 同步
# PostgreSQL CDC -> Fluss

echo "=========================================="
echo "[1/5] 启动 ODS 层 CDC 同步"
echo "=========================================="
echo "说明: 将 PostgreSQL 原始数据实时同步到 Fluss ODS 层"
echo ""

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 启动 ODS 层作业
echo "正在执行 SQL 脚本..."
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/01-run-ods-cdc.sql"

# 等待作业提交
sleep 5

echo ""
echo "=========================================="
echo "验证作业状态..."
echo "=========================================="

# 使用 check-jobs.sh 验证作业状态
bash "$SCRIPT_DIR/check-jobs.sh" "ODS"

if [ $? -eq 0 ]; then
    echo ""
    echo -e "\033[0;32m✓\033[0m ODS 层作业运行正常"
    echo "查看作业状态: http://localhost:8081"
else
    echo ""
    echo -e "\033[0;31m✗\033[0m ODS 层作业启动失败！"
    echo "请检查 Flink Web UI: http://localhost:8081"
    exit 1
fi
echo "=========================================="

echo "=========================================="
