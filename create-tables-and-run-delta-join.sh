# 创建 Fluss 表
bash /opt/flink/bin/sql-client.sh -f /opt/data/prepare_table.sql

# 启动 Delta Join 作业
bash /opt/flink/bin/sql-client.sh -f /opt/data/run_delta_join.sql