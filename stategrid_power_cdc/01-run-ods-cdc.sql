-- ODS 层 CDC 同步作业
-- PostgreSQL CDC -> Fluss ODS 层
CREATE CATALOG fluss_catalog
WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);
USE CATALOG default_catalog;

SET 'pipeline.name' = 'StateGrid CDC: ODS Layer (PostgreSQL -> Fluss)';
SET 'parallelism.default' = '2';

-- ==================== ODS: 用户信息 CDC 同步 ====================

CREATE TABLE IF NOT EXISTS pg_power_user (
    user_id BIGINT,
    user_name STRING,
    usage_type STRING,
    region_id INT,
    region_name STRING,
    address STRING,
    phone STRING,
    create_time TIMESTAMP(3),
    update_time TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'localhost',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'stategrid_db',
    'schema-name' = 'public',
    'table-name' = 'power_user',
    'slot.name' = 'flink_stategrid_user',
    'decoding.plugin.name' = 'wal2json',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.size' = '8096'
);

INSERT INTO fluss_catalog.stategrid_db.ods_power_user
SELECT * FROM pg_power_user;

-- ==================== ODS: 消费记录 CDC 同步 ====================

CREATE TABLE IF NOT EXISTS pg_power_consumption (
    consumption_id BIGINT,
    user_id BIGINT,
    consumption_amount DECIMAL(10, 2),
    consumption_cost DECIMAL(10, 2),
    consumption_date TIMESTAMP(3),
    meter_reading_before DECIMAL(10, 2),
    meter_reading_after DECIMAL(10, 2),
    remark STRING,
    PRIMARY KEY (consumption_id) NOT ENFORCED
) WITH (
    'connector' = 'postgres-cdc',
    'hostname' = 'localhost',
    'port' = '5432',
    'username' = 'postgres',
    'password' = 'postgres',
    'database-name' = 'stategrid_db',
    'schema-name' = 'public',
    'table-name' = 'power_consumption',
    'slot.name' = 'flink_stategrid_consumption',
    'decoding.plugin.name' = 'wal2json',
    'scan.incremental.snapshot.enabled' = 'true',
    'scan.incremental.snapshot.chunk.size' = '8096'
);

INSERT INTO fluss_catalog.stategrid_db.ods_power_consumption
SELECT * FROM pg_power_consumption;
