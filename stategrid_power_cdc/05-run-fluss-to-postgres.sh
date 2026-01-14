#!/bin/bash
# [5/5] 启动 Sink 层 (Fluss -> PostgreSQL)
# Fluss ADS -> PostgreSQL

echo "=========================================="
echo "[5/5] 启动 Sink 层 (Fluss -> PostgreSQL)"
echo "=========================================="
echo "说明: 将 Fluss ADS 层数据同步回 PostgreSQL"
echo ""

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 启动 Sink 层作业
bash /opt/flink/bin/sql-client.sh -f "$SCRIPT_DIR/05-run-fluss-to-postgres.sql"

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Sink 层作业启动成功！"
    echo "查看作业状态: http://localhost:8081"
else
    echo ""
    echo "✗ Sink 层作业启动失败！"
    exit 1
fi

echo "=========================================="
echo ""
echo "作业完成后可以执行："
echo "  ./insert-test-data.sh    - 插入测试数据"
echo "  python monitor_performance.py  - 监控性能"
echo "  python test_crud.py       - 测试 CRUD 操作"
echo "=========================================="
