#!/bin/bash
# 插入测试数据脚本

echo "=========================================="
echo "插入测试数据"
echo "=========================================="

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 执行 SQL 脚本
echo "插入测试数据到源表..."
psql -h localhost -U postgres -d stategrid_db < "$SCRIPT_DIR/insert-test-data.sql"

if [ $? -eq 0 ]; then
    echo "=========================================="
    echo "测试数据插入成功！"
    echo "=========================================="
    echo "已插入数据："
    echo "  - 22 个电力用户"
    echo "  - 50 条消费记录"
    echo "  - 分布在 8 个地区，5 天内"
    echo ""
    echo "CDC 同步将自动捕获这些变更..."
    echo "=========================================="
    echo ""
    echo "查看结果："
    echo "  ./query-results.sh"
    echo ""
    echo "或直接连接 PostgreSQL："
    echo "  psql -h localhost -U postgres -d stategrid_db"
    echo "=========================================="
else
    echo "错误：数据插入失败，请检查数据库连接"
    exit 1
fi
