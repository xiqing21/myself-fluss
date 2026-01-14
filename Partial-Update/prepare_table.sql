CREATE CATALOG fluss_demo
WITH (
    'type' = 'fluss'
    ,'bootstrap.servers' = 'localhost:9123'
)
;

USE CATALOG fluss_demo
;

CREATE DATABASE IF NOT EXISTS fluss
;

USE fluss
;
-- Recommendations – model scores
CREATE TABLE IF NOT EXISTS `fluss_demo`.fluss.recommendations (
    user_id  STRING,
    item_id  STRING,
    rec_score DOUBLE,
    rec_ts   TIMESTAMP(3),
    PRIMARY KEY (user_id, item_id) NOT ENFORCED
) WITH ('bucket.num' = '3');


-- Impressions – how often we showed something
CREATE TABLE IF NOT EXISTS `fluss_demo`.fluss.impressions (
    user_id STRING,
    item_id STRING,
    imp_cnt INT,
    imp_ts  TIMESTAMP(3),
    PRIMARY KEY (user_id, item_id) NOT ENFORCED
) WITH ('bucket.num' = '3');

-- Clicks – user engagement
CREATE TABLE IF NOT EXISTS `fluss_demo`.fluss.clicks (
    user_id  STRING,
    item_id  STRING,
    click_cnt INT,
    clk_ts    TIMESTAMP(3),
    PRIMARY KEY (user_id, item_id) NOT ENFORCED
) WITH ('bucket.num' = '3');

-- Result wide table
CREATE TABLE IF NOT EXISTS `fluss_demo`.fluss.user_rec_wide (
    user_id   STRING,
    item_id   STRING,
    rec_score DOUBLE,   -- updated by recs stream
    imp_cnt   INT,      -- updated by impressions stream
    click_cnt INT,      -- updated by clicks stream
    PRIMARY KEY (user_id, item_id) NOT ENFORCED
) WITH ('bucket.num' = '3');