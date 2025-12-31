-- DWD → DWS 层实时聚合作业 SQL
-- 对 DWD 层明细数据进行聚合统计，生成 DWS 层汇总数据

USE CATALOG default_catalog;

-- 设置作业名称
SET 'pipeline.name' = 'StateGrid CDC: DWD to DWS Layer';

-- 设置并行度
SET 'parallelism.default' = '2';

-- 启用 Checkpoint
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '10s';

-- ==================== DWD → DWS：地区日汇总 ====================

-- 按地区和日期聚合，统计各地区每日用电情况
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

-- 聚合说明：
-- 1. 使用 1 天的滚动窗口（TUMBLE）
-- 2. 按地区 ID 和地区名称分组
-- 3. 计算总用电量、总金额、用户数、平均值、最大值、最小值
-- 4. 实时更新到 dws_region_daily_stats 表

-- ==================== DWD → DWS：用户用电排名 ====================

-- 按日期和地区计算用户用电排名
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

-- 排名说明：
-- 1. 使用 1 天的滚动窗口
-- 2. 按日期和地区分区
-- 3. 按总用电量降序排列，计算排名
-- 4. ROW_NUMBER() 函数生成排名
-- 5. 同一天同一地区内的用户进行排名
