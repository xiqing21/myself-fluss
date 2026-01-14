-- Fluss ADS -> PostgreSQL Sink 作业
-- 将 ADS 层数据写入 PostgreSQL Sink
CREATE CATALOG fluss_catalog
WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);
USE CATALOG default_catalog;
USE default_database;

-- 创建持久表（在 create-fluss-tables.sql 中执行）
CREATE TABLE IF NOT EXISTS ads_power_dashboard_sink (
    dashboard_id STRING,
    stat_date DATE,
    region_id INT,
    region_name STRING,
    total_consumption DECIMAL(15, 2),
    total_cost DECIMAL(15, 2),
    user_count INT,
    avg_consumption DECIMAL(10, 2),
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

-- 从 Fluss ADS 表读取并写入 PostgreSQL
INSERT INTO ads_power_dashboard_sink
SELECT * FROM fluss_catalog.stategrid_db.ads_power_dashboard;

