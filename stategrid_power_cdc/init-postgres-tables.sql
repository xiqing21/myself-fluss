-- PostgreSQL 初始化脚本
-- 创建源表和 Sink 表

-- ==================== 源表（ODS层源数据）====================

-- 电力用户表
DROP TABLE IF EXISTS power_user CASCADE;
CREATE TABLE power_user (
    user_id BIGINT PRIMARY KEY,
    user_name VARCHAR(100),
    usage_type VARCHAR(50),  -- 用电类型：居民、商业、工业
    region_id INT,
    region_name VARCHAR(100),
    address VARCHAR(200),
    phone VARCHAR(20),
    create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 电力消费记录表
DROP TABLE IF EXISTS power_consumption CASCADE;
CREATE TABLE power_consumption (
    consumption_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT,
    consumption_amount DECIMAL(10, 2),  -- 用电量（度）
    consumption_cost DECIMAL(10, 2),     -- 消费金额（元）
    consumption_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    meter_reading_before DECIMAL(10, 2),
    meter_reading_after DECIMAL(10, 2),
    remark VARCHAR(200),
    FOREIGN KEY (user_id) REFERENCES power_user(user_id)
);

-- 创建索引
CREATE INDEX idx_consumption_user_id ON power_consumption(user_id);
CREATE INDEX idx_consumption_date ON power_consumption(consumption_date);
CREATE INDEX idx_user_region_id ON power_user(region_id);

-- ==================== Sink 表（目标表）====================

-- DWD层：消费明细表（关联用户信息）
DROP TABLE IF EXISTS dwd_power_consumption_detail CASCADE;
CREATE TABLE dwd_power_consumption_detail (
    consumption_id BIGINT PRIMARY KEY,
    user_id BIGINT,
    user_name VARCHAR(100),
    consumption_amount DECIMAL(10, 2),
    consumption_cost DECIMAL(10, 2),
    consumption_date TIMESTAMP,
    region_id INT,
    region_name VARCHAR(100),
    usage_type VARCHAR(50),
    etl_time TIMESTAMP
);

-- 创建索引
CREATE INDEX idx_dwd_consumption_date ON dwd_power_consumption_detail(consumption_date);
CREATE INDEX idx_dwd_region_id ON dwd_power_consumption_detail(region_id);
CREATE INDEX idx_dwd_user_id ON dwd_power_consumption_detail(user_id);

-- DWS层：地区日汇总表
DROP TABLE IF EXISTS dws_region_daily_stats CASCADE;
CREATE TABLE dws_region_daily_stats (
    stats_id BIGSERIAL PRIMARY KEY,
    region_id INT,
    region_name VARCHAR(100),
    stat_date DATE,
    total_consumption DECIMAL(15, 2),  -- 总用电量
    total_cost DECIMAL(15, 2),          -- 总金额
    user_count INT,                     -- 用户数
    avg_consumption DECIMAL(10, 2),     -- 人均用电量
    max_consumption DECIMAL(10, 2),     -- 最大用电量
    min_consumption DECIMAL(10, 2),     -- 最小用电量
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(region_id, stat_date)
);

-- DWS层：用户用电排名表
DROP TABLE IF EXISTS dws_user_ranking CASCADE;
CREATE TABLE dws_user_ranking (
    ranking_id BIGSERIAL PRIMARY KEY,
    user_id BIGINT,
    user_name VARCHAR(100),
    region_id INT,
    region_name VARCHAR(100),
    stat_date DATE,
    total_consumption DECIMAL(10, 2),
    total_cost DECIMAL(10, 2),
    ranking INT,                         -- 排名
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id, stat_date)
);

-- 创建索引
CREATE INDEX idx_dws_ranking_date ON dws_user_ranking(stat_date);
CREATE INDEX idx_dws_ranking_region ON dws_user_ranking(region_id);
CREATE INDEX idx_dws_stats_date ON dws_region_daily_stats(stat_date);

-- 配置 CDC
-- 设置 WAL 级别（需要在 postgresql.conf 中配置）
-- wal_level = logical
-- max_replication_slots = 4
-- max_wal_senders = 4
