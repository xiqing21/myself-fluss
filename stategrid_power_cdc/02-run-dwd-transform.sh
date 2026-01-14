#!/bin/bash
# [2/5] 启动 DWD 层转换
# Fluss ODS -> Fluss DWD (Join)

echo "=========================================="
echo "[2/5] 启动 DWD 层转换"
echo "=========================================="
echo "说明: 从 Fluss ODS 层读取数据,进行 Join 转换后写入 Fluss DWD 层"
echo ""

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 启动 DWD 层作业
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/run-dwd-transform.sql"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ DWD 层转换作业启动成功！"
    echo "查看作业状态: http://localhost:8081"
else
    echo ""
    echo "✗ DWD 层转换作业启动失败！"
    exit 1
fi

echo "=========================================="
