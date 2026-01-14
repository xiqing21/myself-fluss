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

-- ADS层：电力仪表盘数据（最终 Sink）
DROP TABLE IF EXISTS ads_power_dashboard CASCADE;
CREATE TABLE ads_power_dashboard (
    dashboard_id VARCHAR(100) NOT NULL,
    stat_date DATE NOT NULL,
    region_id INT,
    region_name VARCHAR(100),
    total_consumption DECIMAL(38, 2),
    total_cost DECIMAL(38, 2),
    user_count BIGINT,
    avg_consumption DECIMAL(35, 2),
    peak_hour INT,
    peak_consumption DECIMAL(10, 2),
    top_user_id BIGINT,
    top_user_name VARCHAR(100),
    top_user_consumption DECIMAL(10, 2),
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dashboard_id, stat_date, region_id)
);

-- 创建索引
CREATE INDEX idx_ads_stat_date ON ads_power_dashboard(stat_date);
CREATE INDEX idx_ads_region_id ON ads_power_dashboard(region_id);

-- 注意：DWD和DWS层的数据存储在 Fluss 中，这里不再创建 PostgreSQL 表

-- 配置 CDC
-- 设置 WAL 级别（需要在 postgresql.conf 中配置）
-- wal_level = logical
-- max_replication_slots = 4
-- max_wal_senders = 4
