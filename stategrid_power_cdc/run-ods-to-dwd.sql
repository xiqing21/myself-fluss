-- ODS → DWD 层 CDC 同步作业 SQL
-- 将 CDC 捕获的源数据关联后写入 DWD 层

USE CATALOG default_catalog;

-- 设置作业名称
SET 'pipeline.name' = 'StateGrid CDC: ODS to DWD Layer';

-- 设置并行度
SET 'parallelism.default' = '2';

-- 启用 Checkpoint
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.interval' = '10s';

-- ==================== ODS → DWD：消费明细同步 ====================

-- 将消费记录与用户信息关联，生成完整的消费明细
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

-- 作业说明：
-- 1. 通过 CDC 实时捕获 power_consumption 表的变更
-- 2. 关联 power_user 表获取用户维度信息
-- 3. 将完整的数据写入 dwd_power_consumption_detail 表
-- 4. 使用 LEFT JOIN 确保即使用户信息缺失也能保留消费记录
-- 5. 使用 COALESCE 处理 NULL 值
