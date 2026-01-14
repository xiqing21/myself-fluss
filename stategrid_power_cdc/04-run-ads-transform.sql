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
        t1.region_id,
        t1.stat_date,
        t1.peak_hour
    FROM (
        SELECT
            region_id,
            CAST(TUMBLE_START(consumption_date, INTERVAL '1' DAY) AS DATE) AS stat_date,
            EXTRACT(HOUR FROM consumption_date) AS peak_hour,
            SUM(consumption_amount) AS hourly_consumption,
            ROW_NUMBER() OVER (
                PARTITION BY region_id, CAST(TUMBLE_START(consumption_date, INTERVAL '1' DAY) AS DATE)
                ORDER BY SUM(consumption_amount) ASC
            ) AS row_num_asc
        FROM dwd_power_consumption_detail
        GROUP BY
            region_id,
            consumption_date,
            TUMBLE(consumption_date, INTERVAL '1' DAY')
    ) t1
    LEFT JOIN (
        SELECT
            region_id,
            CAST(TUMBLE_START(consumption_date, INTERVAL '1' DAY) AS DATE) AS stat_date,
            COUNT(*) AS total_count
        FROM (
            SELECT
                region_id,
                consumption_date,
                TUMBLE(consumption_date, INTERVAL '1' DAY')
            FROM dwd_power_consumption_detail
            GROUP BY
                region_id,
                consumption_date,
                TUMBLE(consumption_date, INTERVAL '1' DAY')
        ) t
        GROUP BY region_id, stat_date
    ) t2 ON t1.region_id = t2.region_id AND t1.stat_date = t2.stat_date
    WHERE t1.row_num_asc = t2.total_count
) p ON r.region_id = p.region_id AND r.stat_date = p.stat_date;
