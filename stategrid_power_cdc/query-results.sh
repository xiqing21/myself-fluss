#!/bin/bash
# 查询处理结果脚本

echo "=========================================="
echo "查询国网电力 CDC 处理结果"
echo "=========================================="

# 检查容器是否运行
if ! docker ps | grep -q postgres; then
    echo "错误：PostgreSQL 容器未运行"
    exit 1
fi

echo ""
echo "=========================================="
echo "1. 源表数据统计"
echo "=========================================="
docker exec postgres psql -U postgres -d stategrid_db -c "
    SELECT 
        '用户表' AS table_name,
        COUNT(*) AS record_count
    FROM power_user
    UNION ALL
    SELECT 
        '消费记录表' AS table_name,
        COUNT(*) AS record_count
    FROM power_consumption;
"

echo ""
echo "=========================================="
echo "2. DWD 层消费明细（最近10条）"
echo "=========================================="
docker exec postgres psql -U postgres -d stategrid_db -c "
    SELECT 
        consumption_id,
        user_id,
        user_name,
        consumption_amount,
        consumption_cost,
        consumption_date,
        region_name
    FROM dwd_power_consumption_detail
    ORDER BY consumption_date DESC
    LIMIT 10;
"

echo ""
echo "=========================================="
echo "3. DWS 层地区日汇总"
echo "=========================================="
docker exec postgres psql -U postgres -d stategrid_db -c "
    SELECT 
        region_name,
        stat_date,
        ROUND(total_consumption::numeric, 2) AS total_consumption,
        ROUND(total_cost::numeric, 2) AS total_cost,
        user_count,
        ROUND(avg_consumption::numeric, 2) AS avg_consumption
    FROM dws_region_daily_stats
    ORDER BY stat_date, region_id;
"

echo ""
echo "=========================================="
echo "4. DWS 层用户用电排名（Top 10）"
echo "=========================================="
docker exec postgres psql -U postgres -d stategrid_db -c "
    SELECT 
        stat_date,
        region_name,
        user_name,
        ROUND(total_consumption::numeric, 2) AS total_consumption,
        ROUND(total_cost::numeric, 2) AS total_cost,
        ranking
    FROM dws_user_ranking
    ORDER BY stat_date, ranking
    LIMIT 10;
"

echo ""
echo "=========================================="
echo "查询完成！"
echo "=========================================="
echo ""
echo "进入 PostgreSQL 命令行进行自定义查询："
echo "  docker exec -it postgres psql -U postgres -d stategrid_db"
echo "=========================================="
