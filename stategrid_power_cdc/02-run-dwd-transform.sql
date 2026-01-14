-- DWD 层转换作业
-- Fluss ODS -> Fluss DWD 层
CREATE CATALOG fluss_catalog
WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);
USE CATALOG fluss_catalog;
USE stategrid_db;

SET 'pipeline.name' = 'StateGrid CDC: DWD Layer (Join)';
SET 'parallelism.default' = '2';

-- ==================== DWD: 消费明细（关联用户维度）====================

INSERT INTO dwd_power_consumption_detail
SELECT
    c.consumption_id,
    c.user_id,
    COALESCE(u.user_name, '未知用户') AS user_name,
    c.consumption_amount,
    c.consumption_cost,
    c.consumption_date,
    COALESCE(u.region_id, 0) AS region_id,
    COALESCE(u.region_name, '未知地区') AS region_name,
    COALESCE(u.usage_type, '未知') AS usage_type,
    CURRENT_TIMESTAMP AS etl_time
FROM ods_power_consumption AS c
    LEFT JOIN ods_power_user AS u
        ON c.user_id = u.user_id;
