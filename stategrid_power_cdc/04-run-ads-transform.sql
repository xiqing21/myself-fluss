-- ADS 层转换作业
-- Fluss DWS -> Fluss ADS 层
CREATE CATALOG fluss_catalog
WITH (
    'type' = 'fluss',
    'bootstrap.servers' = 'localhost:9123'
);
USE CATALOG fluss_catalog;
USE stategrid_db;

SET 'pipeline.name' = 'StateGrid CDC: ADS Layer (Dashboard)';
SET 'parallelism.default' = '2';

-- ==================== ADS: 仪表盘数据 ====================

INSERT INTO ads_power_dashboard
SELECT
    'DASHBOARD_' || region_id || '_' || CAST(stat_date AS STRING) AS dashboard_id,
    stat_date,
    region_id,
    region_name,
    total_consumption,
    total_cost,
    user_count,
    avg_consumption,
    EXTRACT(HOUR FROM MAX(update_time)) AS peak_hour,
    MAX_consumption AS peak_consumption,
    top_user_id,
    top_user_name,
    top_user_consumption,
    CURRENT_TIMESTAMP AS update_time
FROM (
    SELECT
        region_id,
        region_name,
        stat_date,
        total_consumption,
        total_cost,
        user_count,
        avg_consumption,
        max_consumption AS MAX_consumption,
        FIRST_VALUE(user_id) OVER (
            PARTITION BY stat_date, region_id
            ORDER BY total_consumption DESC
        ) AS top_user_id,
        FIRST_VALUE(user_name) OVER (
            PARTITION BY stat_date, region_id
            ORDER BY total_consumption DESC
        ) AS top_user_name,
        FIRST_VALUE(total_consumption) OVER (
            PARTITION BY stat_date, region_id
            ORDER BY total_consumption DESC
        ) AS top_user_consumption,
        CURRENT_TIMESTAMP AS update_time
    FROM dws_region_daily_stats
) t
GROUP BY
    dashboard_id,
    stat_date,
    region_id,
    region_name,
    total_consumption,
    total_cost,
    user_count,
    avg_consumption,
    peak_hour,
    peak_consumption,
    top_user_id,
    top_user_name,
    top_user_consumption,
    update_time;
