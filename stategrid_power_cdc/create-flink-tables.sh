#!/bin/bash
# 创建 Flink 表脚本

echo "=========================================="
echo "创建 Flink 表"
echo "=========================================="

# 检查容器是否运行
if ! docker ps | grep -q flink-jobmanager; then
    echo "错误：Flink 容器未运行，请先运行 ./start-services.sh"
    exit 1
fi

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 将 SQL 文件复制到容器
docker cp "$SCRIPT_DIR/create-flink-tables.sql" flink-jobmanager:/tmp/create-flink-tables.sql

# 执行 SQL
echo "创建 Flink 表..."
docker exec flink-jobmanager bash -c "cd /opt/flink && ./bin/sql-client.sh -f /tmp/create-flink-tables.sql"

if [ $? -eq 0 ]; then
    echo "=========================================="
    echo "Flink 表创建成功！"
    echo "=========================================="
    echo "已创建表："
    echo "ODS层："
    echo "  - ods_power_user（用户CDC表）"
    echo "  - ods_power_consumption（消费记录CDC表）"
    echo ""
    echo "DWD层："
    echo "  - dwd_power_consumption_detail（消费明细表）"
    echo ""
    echo "DWS层："
    echo "  - dws_region_daily_stats（地区日汇总表）"
    echo "  - dws_user_ranking（用户排名表）"
    echo "=========================================="
    echo ""
    echo "下一步：运行数据处理作业 ./run-all-jobs.sh"
else
    echo "错误：Flink 表创建失败"
    exit 1
fi
