-- ODS 层数据生成作业
-- DataGen -> Fluss ODS 层
CREATE CATALOG fluss_catalog
WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);
USE CATALOG default_catalog;

SET 'pipeline.name' = 'StateGrid DataGen: ODS Layer (DataGen -> Fluss)';
SET 'parallelism.default' = '2';

-- ==================== ODS: 用户信息 DataGen ====================

CREATE TABLE IF NOT EXISTS power_user_source (
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
    'connector' = 'datagen',
    'rows-per-second' = '10',
    'fields.user_id.kind' = 'sequence',
    'fields.user_id.start' = '1',
    'fields.user_id.end' = '10000',
    'fields.user_name.kind' = 'random',
    'fields.user_name.length' = '10',
    'fields.usage_type.kind' = 'random',
    'fields.usage_type.length' = '4',
    'fields.region_id.kind' = 'random',
    'fields.region_id.min' = '1',
    'fields.region_id.max' = '10',
    'fields.region_name.kind' = 'random',
    'fields.region_name.length' = '10',
    'fields.address.kind' = 'random',
    'fields.address.length' = '30',
    'fields.phone.kind' = 'random',
    'fields.phone.length' = '11',
    'fields.create_time.kind' = 'random',
    'fields.create_time.max-past' = '1d',
    'fields.update_time.kind' = 'random',
    'fields.update_time.max-past' = '1d'
);

INSERT INTO fluss_catalog.stategrid_db.ods_power_user
SELECT * FROM power_user_source;

-- ==================== ODS: 消费记录 DataGen ====================

CREATE TABLE IF NOT EXISTS power_consumption_source (
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
    'connector' = 'datagen',
    'rows-per-second' = '20',
    'fields.consumption_id.kind' = 'sequence',
    'fields.consumption_id.start' = '1',
    'fields.consumption_id.end' = '100000',
    'fields.user_id.kind' = 'random',
    'fields.user_id.min' = '1',
    'fields.user_id.max' = '10000',
    'fields.consumption_amount.kind' = 'random',
    'fields.consumption_amount.min' = '10',
    'fields.consumption_amount.max' = '500',
    'fields.consumption_cost.kind' = 'random',
    'fields.consumption_cost.min' = '5',
    'fields.consumption_cost.max' = '300',
    'fields.consumption_date.kind' = 'random',
    'fields.consumption_date.max-past' = '1d',
    'fields.meter_reading_before.kind' = 'random',
    'fields.meter_reading_before.min' = '1000',
    'fields.meter_reading_before.max' = '100000',
    'fields.meter_reading_after.kind' = 'random',
    'fields.meter_reading_after.min' = '1100',
    'fields.meter_reading_after.max' = '100500',
    'fields.remark.kind' = 'random',
    'fields.remark.length' = '20'
);

INSERT INTO fluss_catalog.stategrid_db.ods_power_consumption
SELECT * FROM power_consumption_source;
