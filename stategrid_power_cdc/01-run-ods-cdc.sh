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
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/01-run-ods-cdc.sql"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ ODS 层 CDC 同步作业启动成功！"
    echo "查看作业状态: http://localhost:8081"
else
    echo ""
    echo "✗ ODS 层 CDC 同步作业启动失败！"
    exit 1
fi

echo "=========================================="
