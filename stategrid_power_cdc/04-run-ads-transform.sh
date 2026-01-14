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
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/04-run-ads-transform.sql"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ ADS 层转换作业启动成功！"
    echo "查看作业状态: http://localhost:8081"
else
    echo ""
    echo "✗ ADS 层转换作业启动失败！"
    exit 1
fi

echo "=========================================="
