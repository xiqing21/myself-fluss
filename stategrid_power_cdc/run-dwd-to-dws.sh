#!/bin/bash
# 运行 DWD → DWS 层实时聚合作业

echo "=========================================="
echo "启动 DWD → DWS 层实时聚合作业"
echo "=========================================="

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 执行 SQL
echo "启动实时聚合作业..."
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/run-dwd-to-dws.sql"

if [ $? -eq 0 ]; then
    echo "=========================================="
    echo "DWD → DWS 作业已启动"
    echo "=========================================="
    echo "Flink Web UI: http://localhost:8081"
    echo ""
    echo "作业说明："
    echo "  - 地区日汇总：按地区和日期聚合统计"
    echo "  - 用户排名：按用电量计算排名"
    echo "  - 使用 1 天滚动窗口"
    echo "=========================================="
else
    echo "错误：作业启动失败"
    exit 1
fi
