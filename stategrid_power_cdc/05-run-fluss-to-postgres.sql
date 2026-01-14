-- Fluss ADS -> Print Sink 作业
-- 将 ADS 层数据输出到控制台（由于 Flink 2.2.0 不支持 JDBC Sink）
CREATE CATALOG fluss_catalog
WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);
USE CATALOG default_catalog;
USE default_database;

-- 创建 Print Sink 表
CREATE TABLE IF NOT EXISTS ads_power_dashboard_print (
    dashboard_id STRING,
    stat_date DATE,
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
    'connector' = 'print'
);

-- 从 Fluss ADS 表读取并输出到控制台
INSERT INTO ads_power_dashboard_print
SELECT * FROM fluss_catalog.stategrid_db.ads_power_dashboard;

