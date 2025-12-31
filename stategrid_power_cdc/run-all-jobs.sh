#!/bin/bash
# 一键运行所有数据处理作业

echo "=========================================="
echo "一键启动所有数据处理作业"
echo "=========================================="

# 获取脚本所在目录
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# 启动 ODS → DWD 作业
echo ""
echo "[1/2] 启动 ODS → DWD 作业..."
bash "$SCRIPT_DIR/run-ods-to-dwd.sh"

# 等待作业启动
sleep 3

# 启动 DWD → DWS 作业
echo ""
echo "[2/2] 启动 DWD → DWS 作业..."
bash "$SCRIPT_DIR/run-dwd-to-dws.sh"

# 等待作业启动
sleep 3

echo ""
echo "=========================================="
echo "所有作业启动完成！"
echo "=========================================="
echo ""
echo "作业列表："
echo "  1. ODS → DWD CDC 同步作业"
echo "  2. DWD → DWS 实时聚合作业"
echo ""
echo "查看作业状态：http://localhost:8081"
echo ""
echo "下一步：插入测试数据 ./insert-test-data.sh"
echo "=========================================="
