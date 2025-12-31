#!/bin/bash
# 停止 Docker 服务脚本

echo "=========================================="
echo "停止国网电力 CDC 示例服务"
echo "=========================================="

# 停止容器
echo "停止容器..."
docker stop flink-jobmanager flink-taskmanager postgres 2>/dev/null

# 删除容器
echo "删除容器..."
docker rm flink-jobmanager flink-taskmanager postgres 2>/dev/null

# 删除网络
echo "删除 Docker 网络..."
docker network rm stategrid-network 2>/dev/null

# 询问是否删除数据
read -p "是否删除数据目录？[y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "删除数据目录..."
    rm -rf /tmp/stategrid-flink
    rm -rf /tmp/stategrid-postgres
    echo "数据目录已删除"
fi

echo "=========================================="
echo "服务已停止"
echo "=========================================="
