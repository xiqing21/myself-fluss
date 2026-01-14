-- DWS 层聚合作业
-- Fluss DWD -> Fluss DWS 层

USE CATALOG fluss_catalog;
USE stategrid_db;

SET 'pipeline.name' = 'StateGrid CDC: DWS Layer (Aggregate)';
SET 'parallelism.default' = '2';

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
    TUMBLE(consumption_date, INTERVAL '1' DAY');

-- ==================== DWS: 用户用电排名 ====================

INSERT INTO dws_user_ranking
SELECT
    user_id,
    user_name,
    region_id,
    region_name,
    CAST(TUMBLE_START(consumption_date, INTERVAL '1' DAY) AS DATE) AS stat_date,
    SUM(consumption_amount) AS total_consumption,
    SUM(consumption_cost) AS total_cost,
    ROW_NUMBER() OVER (
        PARTITION BY TUMBLE(consumption_date, INTERVAL '1' DAY), region_id
        ORDER BY SUM(consumption_amount) DESC
    ) AS ranking
FROM dwd_power_consumption_detail
GROUP BY
    user_id,
    user_name,
    region_id,
    region_name,
    TUMBLE(consumption_date, INTERVAL '1' DAY);
