#!/bin/bash
# 插入测试数据脚本

echo "=========================================="
echo "插入测试数据"
echo "=========================================="

# 检查容器是否运行
if ! docker ps | grep -q postgres; then
    echo "错误：PostgreSQL 容器未运行"
    exit 1
fi

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 执行 SQL 脚本
echo "插入测试数据到源表..."
docker exec -i postgres psql -U postgres -d stategrid_db < "$SCRIPT_DIR/insert-test-data.sql"

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
    echo "  docker exec -it postgres psql -U postgres -d stategrid_db"
    echo ""
    echo "查询示例："
    echo "  -- 查看 DWD 层数据"
    echo "  SELECT * FROM dwd_power_consumption_detail ORDER BY consumption_date;"
    echo ""
    echo "  -- 查看地区汇总"
    echo "  SELECT * FROM dws_region_daily_stats ORDER BY stat_date, region_id;"
    echo ""
    echo "  -- 查看用户排名"
    echo "  SELECT * FROM dws_user_ranking ORDER BY stat_date, total_consumption DESC;"
    echo "=========================================="
else
    echo "错误：数据插入失败"
    exit 1
fi
