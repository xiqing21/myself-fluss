-- Fluss ADS -> PostgreSQL Sink 作业
-- 将 ADS 层数据写入 PostgreSQL Sink

USE CATALOG fluss_catalog;
USE stategrid_db;

SET 'pipeline.name' = 'StateGrid CDC: Sink Layer (Fluss -> PostgreSQL)';
SET 'parallelism.default' = '2';

-- ==================== Sink: 写入 PostgreSQL ====================

INSERT INTO ads_power_dashboard_sink
SELECT * FROM ads_power_dashboard;
