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
BEGIN STATEMENT SET
;

INSERT INTO `fluss_demo`.fluss.user_rec_wide (user_id, item_id, rec_score)
SELECT
    user_id
    ,item_id
    ,rec_score
FROM `fluss_demo`.fluss.recommendations
;

INSERT INTO `fluss_demo`.fluss.user_rec_wide (user_id, item_id, imp_cnt)
SELECT
    user_id
    ,item_id
    ,imp_cnt
FROM `fluss_demo`.fluss.impressions
;

-- Apply click counts

INSERT INTO `fluss_demo`.fluss.user_rec_wide (user_id, item_id, click_cnt)
SELECT
    user_id
    ,item_id
    ,click_cnt
FROM `fluss_demo`.fluss.clicks
;


END
;