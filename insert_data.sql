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

CREATE TEMPORARY TABLE datagen
(
    event_type  int
    ,person     ROW<
        id BIGINT
        ,name VARCHAR
        ,emailAddress VARCHAR
        ,creditCard VARCHAR
        ,city VARCHAR
        ,state VARCHAR
        ,`dateTime` TIMESTAMP(3)
        ,extra VARCHAR
    >
    ,auction    ROW<
        id BIGINT
        ,itemName VARCHAR
        ,description VARCHAR
        ,initialBid BIGINT
        ,reserve BIGINT
        ,`dateTime` TIMESTAMP(3)
        ,expires TIMESTAMP(3)
        ,seller BIGINT
        ,category BIGINT
        ,extra VARCHAR
    >
    ,bid        ROW<
        auction BIGINT
        ,bidder BIGINT
        ,price BIGINT
        ,channel VARCHAR
        ,url VARCHAR
        ,`dateTime` TIMESTAMP(3)
        ,extra VARCHAR
    >
    ,`dateTime` AS CASE
        WHEN event_type = 0 THEN person.`dateTime`
        WHEN event_type = 1 THEN auction.`dateTime`
        ELSE bid.`dateTime`
    END
    ,WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND
)
WITH (
    'connector' = 'nexmark'
    ,'first-event.rate' = '5000'
    ,'next-event.rate' = '5000'
    ,'events.num' = '100000'
    ,'person.proportion' = '2'
    ,'auction.proportion' = '24'
    ,'bid.proportion' = '24'
)
;

CREATE TEMPORARY VIEW auction_view
AS SELECT
    auction.id
    ,auction.itemName
    ,auction.description
    ,auction.initialBid
    ,auction.reserve
    ,`dateTime`
    ,auction.expires
    ,auction.seller
    ,auction.category
    ,auction.extra
FROM datagen
WHERE event_type = 1
;

CREATE TEMPORARY VIEW bid_view
AS SELECT
    bid.auction
    ,bid.bidder
    ,bid.price
    ,bid.channel
    ,bid.url
    ,`dateTime`
    ,bid.extra
FROM datagen
WHERE event_type = 2
;

INSERT INTO bid
SELECT
    *
FROM bid_view
;

INSERT INTO auction
SELECT
    *
FROM auction_view
;