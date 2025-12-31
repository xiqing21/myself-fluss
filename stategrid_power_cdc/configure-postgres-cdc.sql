-- PostgreSQL CDC 配置脚本
-- 需要在 postgres 超级用户下执行

-- 1. 创建数据库
CREATE DATABASE stategrid_db;

-- 2. 连接到新数据库
\c stategrid_db

-- 3. 创建 CDC 用户（可选，也可以使用 postgres 用户）
-- CREATE USER cdc_user WITH PASSWORD 'cdc_password';
-- GRANT REPLICATION TO cdc_user;
-- GRANT ALL PRIVILEGES ON DATABASE stategrid_db TO cdc_user;

-- 4. 创建 replication slots
-- 注意：如果 slot 已存在，需要先删除
SELECT pg_create_logical_replication_slot('flink_stategrid_user', 'wal2json');
SELECT pg_create_logical_replication_slot('flink_stategrid_consumption', 'wal2json');

-- 5. 查看配置
SELECT slot_name, slot_type, active, restart_lsn
FROM pg_replication_slots
WHERE slot_name LIKE 'flink_%';

-- 6. 显示 WAL 配置
SHOW wal_level;
SHOW max_replication_slots;
SHOW max_wal_senders;

-- 注意事项：
-- 1. postgresql.conf 中需要配置：
--    wal_level = logical
--    max_replication_slots = 10
--    max_wal_senders = 10
--
-- 2. 如果需要删除 slot：
--    SELECT pg_drop_replication_slot('flink_stategrid_user');
--    SELECT pg_drop_replication_slot('flink_stategrid_consumption');
--
-- 3. CDC 用户权限：
--    GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
--    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;
