#!/bin/bash
# 初始化 PostgreSQL 表脚本

echo "=========================================="
echo "初始化 PostgreSQL 表"
echo "=========================================="

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 执行 SQL 脚本
echo "执行初始化 SQL..."
psql -h localhost -U postgres -d stategrid_db < "$SCRIPT_DIR/init-postgres-tables.sql"

if [ $? -eq 0 ]; then
    echo "=========================================="
    echo "PostgreSQL 表初始化成功！"
    echo "=========================================="
    echo "已创建表："
    echo "  - power_user（用户表）"
    echo "  - power_consumption（消费记录表）"
    echo "  - dwd_power_consumption_detail（DWD层明细表）"
    echo "  - dws_region_daily_stats（DWS层地区汇总表）"
    echo "  - dws_user_ranking（DWS层用户排名表）"
    echo "=========================================="
else
    echo "错误：初始化失败，请检查数据库连接"
    exit 1
fi
