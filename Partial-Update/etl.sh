# 将recommendations、impressions及clicks三张以user_id
# 为共享主键的表数据分别插入以user_id为主键的大宽表user_rec_wide中。
bash /opt/flink/bin/sql-client.sh -f /opt/data/Partial-Update/etl.sql