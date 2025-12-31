#!/bin/bash
# 运行 DWD → DWS 层实时聚合作业

echo "=========================================="
echo "启动 DWD → DWS 层实时聚合作业"
echo "=========================================="

# 检查容器是否运行
if ! docker ps | grep -q flink-jobmanager; then
    echo "错误：Flink 容器未运行"
    exit 1
fi

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 将 SQL 文件复制到容器
docker cp "$SCRIPT_DIR/run-dwd-to-dws.sql" flink-jobmanager:/tmp/run-dwd-to-dws.sql

# 执行 SQL
echo "启动实时聚合作业..."
docker exec -d flink-jobmanager bash -c "cd /opt/flink && ./bin/sql-client.sh -f /tmp/run-dwd-to-dws.sql"

# 等待作业启动
sleep 5

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
