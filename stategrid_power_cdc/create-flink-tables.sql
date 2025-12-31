-- Flink 表定义脚本
-- 创建 CDC 源表、DWD 层和 DWS 层表

-- 使用默认 catalog
USE CATALOG default_catalog;

-- 设置默认并行度
SET 'parallelism.default' = '2';

-- ==================== ODS 层表（CDC 源表）====================

-- ODS：用户信息 CDC 表
CREATE TABLE ods_power_user (
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

-- ODS：消费记录 CDC 表
CREATE TABLE ods_power_consumption (
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

-- ==================== DWD 层表（中间层）====================

-- DWD：消费明细表（关联用户信息）
CREATE TABLE dwd_power_consumption_detail (
    consumption_id BIGINT,
    user_id BIGINT,
    user_name STRING,
    consumption_amount DECIMAL(10, 2),
    consumption_cost DECIMAL(10, 2),
    consumption_date TIMESTAMP(3),
    region_id INT,
    region_name STRING,
    usage_type STRING,
    etl_time TIMESTAMP(3),
    PRIMARY KEY (consumption_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/stategrid_db',
    'table-name' = 'dwd_power_consumption_detail',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-size' = '10mb',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '5s',
    'sink.max-retries' = '3'
);

-- ==================== DWS 层表（汇总层）====================

-- DWS：地区日汇总表
CREATE TABLE dws_region_daily_stats (
    region_id INT,
    region_name STRING,
    stat_date DATE,
    total_consumption DECIMAL(15, 2),
    total_cost DECIMAL(15, 2),
    user_count INT,
    avg_consumption DECIMAL(10, 2),
    max_consumption DECIMAL(10, 2),
    min_consumption DECIMAL(10, 2),
    PRIMARY KEY (region_id, stat_date) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/stategrid_db',
    'table-name' = 'dws_region_daily_stats',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-size' = '10mb',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '5s',
    'sink.max-retries' = '3'
);

-- DWS：用户用电排名表
CREATE TABLE dws_user_ranking (
    user_id BIGINT,
    user_name STRING,
    region_id INT,
    region_name STRING,
    stat_date DATE,
    total_consumption DECIMAL(10, 2),
    total_cost DECIMAL(10, 2),
    ranking INT,
    PRIMARY KEY (user_id, stat_date) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/stategrid_db',
    'table-name' = 'dws_user_ranking',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-size' = '10mb',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '5s',
    'sink.max-retries' = '3'
);

-- ==================== 配置 Checkpoint ====================

-- 启用精确一次语义
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.timeout' = '5min';
SET 'execution.checkpointing.unaligned' = 'true';
SET 'state.backend' = 'rocksdb';
SET 'state.checkpoints.dir' = 'file:///tmp/flink-checkpoints';
