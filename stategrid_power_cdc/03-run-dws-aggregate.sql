-- DWS 层聚合作业
-- Fluss DWD -> Fluss DWS 层

SET 'pipeline.name' = 'StateGrid CDC: DWS Layer (Aggregate)';
SET 'parallelism.default' = '2';

-- 创建目录并使用
CREATE CATALOG fluss_catalog
WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);

USE CATALOG fluss_catalog;
USE stategrid_db;

-- ==================== DWS: 地区日汇总 ====================

INSERT INTO dws_region_daily_stats
SELECT
    region_id,
    region_name,
    CAST(TUMBLE_START(consumption_date, INTERVAL '1' DAY) AS DATE) AS stat_date,
    SUM(consumption_amount) AS total_consumption,
    SUM(consumption_cost) AS total_cost,
    COUNT(DISTINCT user_id) AS user_count,
    ROUND(AVG(consumption_amount), 2) AS avg_consumption,
    MAX(consumption_amount) AS max_consumption,
    MIN(consumption_amount) AS min_consumption
FROM dwd_power_consumption_detail
GROUP BY
    region_id,
    region_name,
    TUMBLE(consumption_date, INTERVAL '1' DAY);

-- ==================== DWS: 用户用电排名 ====================

INSERT INTO dws_user_ranking
SELECT
    t1.user_id,
    t1.user_name,
    t1.region_id,
    t1.region_name,
    t1.stat_date,
    t1.total_consumption,
    t1.total_cost,
    t2.total_count - t1.row_num_asc + 1 AS ranking
FROM (
    SELECT
        user_id,
        user_name,
        region_id,
        region_name,
        stat_date,
        total_consumption,
        total_cost,
        ROW_NUMBER() OVER (
            PARTITION BY stat_date, region_id
            ORDER BY total_consumption ASC
        ) AS row_num_asc
    FROM (
        SELECT
            user_id,
            user_name,
            region_id,
            region_name,
            CAST(TUMBLE_START(consumption_date, INTERVAL '1' DAY) AS DATE) AS stat_date,
            SUM(consumption_amount) AS total_consumption,
            SUM(consumption_cost) AS total_cost
        FROM dwd_power_consumption_detail
        GROUP BY
            user_id,
            user_name,
            region_id,
            region_name,
            TUMBLE(consumption_date, INTERVAL '1' DAY)
    )
) t1
LEFT JOIN (
    SELECT
        stat_date,
        region_id,
        COUNT(*) AS total_count
    FROM (
        SELECT
            CAST(TUMBLE_START(consumption_date, INTERVAL '1' DAY) AS DATE) AS stat_date,
            region_id,
            user_id
        FROM dwd_power_consumption_detail
        GROUP BY
            user_id,
            region_id,
            TUMBLE(consumption_date, INTERVAL '1' DAY)
    )
    GROUP BY stat_date, region_id
) t2
ON t1.stat_date = t2.stat_date AND t1.region_id = t2.region_id;
