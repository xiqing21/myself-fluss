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
SET sql-client.execution.result-mode=TABLEAU;

-- Recommendations – model scores
INSERT INTO `fluss_demo`.fluss.recommendations VALUES
    ('user_101','prod_501',0.92 , TIMESTAMP '2025-05-16 09:15:02'),
    ('user_101','prod_502',0.78 , TIMESTAMP '2025-05-16 09:15:05'),
    ('user_102','prod_503',0.83 , TIMESTAMP '2025-05-16 09:16:00'),
    ('user_103','prod_501',0.67 , TIMESTAMP '2025-05-16 09:16:20'),
    ('user_104','prod_504',0.88 , TIMESTAMP '2025-05-16 09:16:45');
-- Impressions – how often each (user,item) was shown
INSERT INTO `fluss_demo`.fluss.impressions VALUES
    ('user_101','prod_501', 3, TIMESTAMP '2025-05-16 09:17:10'),
    ('user_101','prod_502', 1, TIMESTAMP '2025-05-16 09:17:15'),
    ('user_102','prod_503', 7, TIMESTAMP '2025-05-16 09:18:22'),
    ('user_103','prod_501', 4, TIMESTAMP '2025-05-16 09:18:30'),
    ('user_104','prod_504', 2, TIMESTAMP '2025-05-16 09:18:55');
-- Clicks – user engagement
INSERT INTO `fluss_demo`.fluss.clicks VALUES
    ('user_101','prod_501', 520, TIMESTAMP '2025-05-16 09:19:00'),
    ('user_101','prod_502', 2, TIMESTAMP '2025-05-16 09:19:07'),
    ('user_102','prod_503', 1, TIMESTAMP '2025-05-16 09:19:12'),
    ('user_103','prod_501', 1, TIMESTAMP '2025-05-16 09:19:20'),
    ('user_104','prod_504', 1, TIMESTAMP '2025-05-16 09:19:25');


-- #默认table，还可以设置为tableau、changelog
SET sql-client.execution.result-mode=tableau;
-- SELECT * FROM `fluss_demo`.fluss.user_rec_wide;