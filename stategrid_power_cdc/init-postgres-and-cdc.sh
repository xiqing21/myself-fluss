#!/bin/bash
# 初始化 PostgreSQL 和 CDC 配置

echo "=========================================="
echo "初始化 PostgreSQL 和 CDC 配置"
echo "=========================================="

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 1. 初始化表结构
echo ""
echo "[1/2] 初始化 PostgreSQL 表..."
psql -h localhost -U postgres -f "$SCRIPT_DIR/init-postgres-tables.sql"

if [ $? -ne 0 ]; then
    echo "错误：表初始化失败"
    exit 1
fi

# 2. 配置 CDC
echo ""
echo "[2/2] 配置 PostgreSQL CDC..."
psql -h localhost -U postgres -f "$SCRIPT_DIR/configure-postgres-cdc.sql" 2>&1 | grep -v "already exists"

if [ $? -ne 0 ]; then
    echo "注意：CDC 配置可能存在警告，但不影响使用"
fi

echo ""
echo "=========================================="
echo "初始化完成！"
echo "=========================================="
echo ""
echo "已创建："
echo "  数据库：stategrid_db"
echo "  源表：power_user, power_consumption"
echo "  Sink 表：ads_power_dashboard"
echo "  CDC Slots：flink_stategrid_user, flink_stategrid_consumption"
echo ""
echo "下一步：./create-fluss-tables.sh"
echo "=========================================="
