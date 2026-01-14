CREATE CATALOG fluss_catalog
WITH (
    'type' = 'fluss'
    ,'bootstrap.servers' = 'localhost:9123'
)
;

USE CATALOG fluss_catalog
;

CREATE DATABASE IF NOT EXISTS my_db
;

USE my_db
;

INSERT INTO delta_join_sink
SELECT
    auction
    ,bidder
    ,price
    ,channel
    ,url
    ,B.`dateTime`
    ,B.extra
    ,itemName
    ,description
    ,initialBid
    ,reserve
    ,A.`dateTime`
    ,expires
    ,seller
    ,category
    ,A.extra
FROM bid AS B
    INNER JOIN auction AS A
        on B.auction = A.id
;
