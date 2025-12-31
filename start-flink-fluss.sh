# 防止之前启动过，先都尝试停止 Fluss 和 Flink
bash /opt/fluss/bin/local-cluster.sh stop
bash /opt/flink/bin/stop-cluster.sh

rm -rf /tmp/fluss-*
rm -rf /tmp/zookeeper

# 启动 Fluss 和 Fluss
bash /opt/fluss/bin/local-cluster.sh start
bash /opt/flink/bin/start-cluster.sh