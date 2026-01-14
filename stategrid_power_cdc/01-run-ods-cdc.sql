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
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/stategrid_db',
    'table-name' = 'public.power_user',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver',
    'scan.fetch-size' = '1000',
    'scan.interval.millis' = '5000'
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
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://localhost:5432/stategrid_db',
    'table-name' = 'public.power_consumption',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver',
    'scan.fetch-size' = '1000',
    'scan.interval.millis' = '5000'
);

INSERT INTO fluss_catalog.stategrid_db.ods_power_consumption
SELECT * FROM pg_power_consumption;
