#!/bin/bash
# 创建 Fluss 分层表

echo "=========================================="
echo "创建 Fluss 分层表 (ODS -> DWD -> DWS -> ADS)"
echo "=========================================="

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 执行 SQL
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/create-flink-tables.sql"

if [ $? -eq 0 ]; then
    echo "=========================================="
    echo "Fluss 分层表创建成功！"
    echo "=========================================="
    echo "ODS 层："
    echo "  - ods_power_user"
    echo "  - ods_power_consumption"
    echo ""
    echo "DWD 层："
    echo "  - dwd_power_consumption_detail"
    echo ""
    echo "DWS 层："
    echo "  - dws_region_daily_stats"
    echo "  - dws_user_ranking"
    echo ""
    echo "ADS 层："
    echo "  - ads_power_dashboard"
    echo "=========================================="
    echo ""
    echo "下一步：./run-all-jobs.sh"
else
    echo "错误：Fluss 表创建失败"
    exit 1
fi
