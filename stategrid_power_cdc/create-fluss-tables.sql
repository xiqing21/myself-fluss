-- Fluss 分层表定义
-- ODS -> DWD -> DWS -> ADS
-- 注意：使用 DataGen 源，非 PostgreSQL CDC

-- 创建 Fluss Catalog
CREATE CATALOG fluss_catalog
WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

USE CATALOG fluss_catalog;

-- 创建数据库
CREATE DATABASE IF NOT EXISTS stategrid_db;

USE stategrid_db;

-- ==================== ODS 层（原始数据层）====================

-- ODS：用户信息 CDC 表（DataGen 源，非 PostgreSQL CDC）
DROP TABLE IF EXISTS ods_power_user;
CREATE TABLE IF NOT EXISTS ods_power_user (
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
    'bucket.key' = 'user_id',
    'table.delete.behavior' = 'IGNORE'
);

-- ODS：消费记录 CDC 表（DataGen 源，非 PostgreSQL CDC）
DROP TABLE IF EXISTS ods_power_consumption;
CREATE TABLE IF NOT EXISTS ods_power_consumption (
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
    'bucket.key' = 'consumption_id',  -- 修改：使用主键字段作为分桶键
    'table.delete.behavior' = 'IGNORE'
);

-- ==================== DWD 层（明细数据层）====================

-- DWD：消费明细表（关联用户维度）
DROP TABLE IF EXISTS dwd_power_consumption_detail;
CREATE TABLE IF NOT EXISTS dwd_power_consumption_detail (
    consumption_id BIGINT,
    user_id BIGINT,
    user_name STRING,
    consumption_amount DECIMAL(10, 2),
    consumption_cost DECIMAL(10, 2),
    consumption_date TIMESTAMP(3),
    WATERMARK FOR consumption_date AS consumption_date - INTERVAL '5' SECOND,
    region_id INT,
    region_name STRING,
    usage_type STRING,
    etl_time TIMESTAMP(3),
    PRIMARY KEY (consumption_id) NOT ENFORCED
) WITH (
    'bucket.key' = 'consumption_id',  -- 修改：使用主键字段作为分桶键
    'table.delete.behavior' = 'IGNORE'
);

-- ==================== DWS 层（汇总数据层）====================

-- DWS：地区日汇总表
DROP TABLE IF EXISTS dws_region_daily_stats;
CREATE TABLE IF NOT EXISTS dws_region_daily_stats (
    region_id INT,
    region_name STRING,
    stat_date DATE NOT NULL,
    total_consumption DECIMAL(38, 2),
    total_cost DECIMAL(38, 2),
    user_count BIGINT,
    avg_consumption DECIMAL(35, 2),
    max_consumption DECIMAL(10, 2),
    min_consumption DECIMAL(10, 2),
    PRIMARY KEY (region_id, stat_date) NOT ENFORCED
) WITH (
    'bucket.key' = 'region_id',
    'table.delete.behavior' = 'IGNORE'
);

-- DWS：用户用电排名表
DROP TABLE IF EXISTS dws_user_ranking;
CREATE TABLE IF NOT EXISTS dws_user_ranking (
    user_id BIGINT,
    user_name STRING,
    region_id INT,
    region_name STRING,
    stat_date DATE NOT NULL,
    total_consumption DECIMAL(38, 2),
    total_cost DECIMAL(38, 2),
    ranking BIGINT,
    PRIMARY KEY (user_id, stat_date) NOT ENFORCED
) WITH (
    'bucket.key' = 'user_id',
    'table.delete.behavior' = 'IGNORE'
);

-- ==================== ADS 层（应用数据层）====================

-- ADS：电力仪表盘数据
DROP TABLE IF EXISTS ads_power_dashboard;
CREATE TABLE IF NOT EXISTS ads_power_dashboard (
    dashboard_id STRING,
    stat_date DATE NOT NULL,
    region_id INT,
    region_name STRING,
    total_consumption DECIMAL(38, 2),
    total_cost DECIMAL(38, 2),
    user_count BIGINT,
    avg_consumption DECIMAL(35, 2),
    peak_hour INT,
    peak_consumption DECIMAL(10, 2),
    top_user_id BIGINT,
    top_user_name STRING,
    top_user_consumption DECIMAL(10, 2),
    update_time TIMESTAMP(3),
    PRIMARY KEY (dashboard_id, stat_date, region_id) NOT ENFORCED
) WITH (
    'bucket.key' = 'dashboard_id',  -- 修改：使用主键第一字段作为分桶键
    'table.delete.behavior' = 'IGNORE'
);

-- ==================== PostgreSQL Sink 表（临时表）====================
-- 注意：Fluss Catalog 不支持非 Fluss 类型的持久表，因此使用 TEMPORARY 表

-- ADS Sink：电力仪表盘（写入 PostgreSQL）
CREATE TEMPORARY TABLE IF NOT EXISTS ads_power_dashboard_sink (
    dashboard_id STRING,
    stat_date DATE NOT NULL,
    region_id INT,
    region_name STRING,
    total_consumption DECIMAL(38, 2),
    total_cost DECIMAL(38, 2),
    user_count BIGINT,
    avg_consumption DECIMAL(35, 2),
    peak_hour INT,
    peak_consumption DECIMAL(10, 2),
    top_user_id BIGINT,
    top_user_name STRING,
    top_user_consumption DECIMAL(10, 2),
    update_time TIMESTAMP(3),
    PRIMARY KEY (dashboard_id, stat_date, region_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/stategrid_db',
    'table-name' = 'ads_power_dashboard',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver',
    'sink.buffer-flush.max-size' = '10mb',
    'sink.buffer-flush.max-rows' = '1000',
    'sink.buffer-flush.interval' = '5s',
    'sink.max-retries' = '3'
);

-- 配置 Checkpoint
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.timeout' = '5min';
SET 'state.backend' = 'rocksdb';
SET 'state.checkpoints.dir' = 'file:///tmp/flink-checkpoints';

-- 配置并行度
SET 'parallelism.default' = '2';
