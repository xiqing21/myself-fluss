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
    'DASHBOARD_' || CAST(r.region_id AS STRING) || '_' || CAST(r.stat_date AS STRING) AS dashboard_id,
    r.stat_date,
    r.region_id,
    r.region_name,
    r.total_consumption,
    r.total_cost,
    r.user_count,
    r.avg_consumption,
    p.peak_hour,
    r.max_consumption AS peak_consumption,
    u.user_id AS top_user_id,
    u.user_name AS top_user_name,
    u.total_consumption AS top_user_consumption,
    CURRENT_TIMESTAMP AS update_time
FROM dws_region_daily_stats r
LEFT JOIN (
    SELECT
        user_id,
        user_name,
        region_id,
        stat_date,
        total_consumption
    FROM dws_user_ranking
    WHERE ranking = 1
) u ON r.region_id = u.region_id AND r.stat_date = u.stat_date
LEFT JOIN (
    SELECT
        region_id,
        stat_date,
        CAST(peak_hour AS INT) AS peak_hour
    FROM (
        SELECT
            region_id,
            CAST(TUMBLE_START(consumption_date, INTERVAL '5' SECOND) AS TIMESTAMP(3)) AS stat_date,
            EXTRACT(HOUR FROM consumption_date) AS peak_hour,
            SUM(consumption_amount) AS hourly_consumption,
            ROW_NUMBER() OVER (
                PARTITION BY region_id, CAST(TUMBLE_START(consumption_date, INTERVAL '5' SECOND) AS TIMESTAMP(3))
                ORDER BY SUM(consumption_amount) DESC
            ) AS rn
        FROM dwd_power_consumption_detail
        GROUP BY
            region_id,
            EXTRACT(HOUR FROM consumption_date),
            TUMBLE(consumption_date, INTERVAL '5' SECOND)
    )
    WHERE rn = 1
) p ON r.region_id = p.region_id AND r.stat_date = p.stat_date;
