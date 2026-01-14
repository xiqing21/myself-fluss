#!/bin/bash
# 创建 Fluss 分层表
# 注意：此脚本会 DROP 已存在的表，确保重建使用最新的表结构

echo "=========================================="
echo "创建 Fluss 分层表 (ODS -> DWD -> DWS -> ADS)"
echo "=========================================="

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 执行 SQL
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/create-fluss-tables.sql"

if [ $? -eq 0 ]; then
    echo "=========================================="
    echo "Fluss 分层表创建成功！"
    echo "=========================================="
    echo "ODS 层（DataGen 源）："
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
    echo "  - ads_power_dashboard_sink (TEMPORARY, 写入 PostgreSQL)"
    echo ""
    echo "说明："
    echo "  - 本项目使用 DataGen 作为数据源，不依赖 PostgreSQL CDC"
    echo "  - 如需 PostgreSQL Sink，需要先运行 init-postgres-tables.sql"
    echo "  - 脚本已包含 DROP TABLE，确保使用最新表结构"
    echo "=========================================="
    echo ""
    echo "下一步：./run-all-jobs.sh"
else
    echo "错误：Fluss 表创建失败"
    exit 1
fi
