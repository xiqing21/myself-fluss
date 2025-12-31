#!/bin/bash
# 运行 ODS → DWD 层 CDC 同步作业

echo "=========================================="
echo "启动 ODS → DWD 层 CDC 同步作业"
echo "=========================================="

# 检查容器是否运行
if ! docker ps | grep -q flink-jobmanager; then
    echo "错误：Flink 容器未运行"
    exit 1
fi

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 将 SQL 文件复制到容器
docker cp "$SCRIPT_DIR/run-ods-to-dwd.sql" flink-jobmanager:/tmp/run-ods-to-dwd.sql

# 执行 SQL
echo "启动 CDC 同步作业..."
docker exec -d flink-jobmanager bash -c "cd /opt/flink && ./bin/sql-client.sh -f /tmp/run-ods-to-dwd.sql"

# 等待作业启动
sleep 5

echo "=========================================="
echo "ODS → DWD 作业已启动"
echo "=========================================="
echo "Flink Web UI: http://localhost:8081"
echo ""
echo "作业说明："
echo "  - 通过 CDC 实时捕获 power_consumption 表变更"
echo "  - 关联 power_user 表获取用户维度信息"
echo "  - 写入 dwd_power_consumption_detail 表"
echo "=========================================="
