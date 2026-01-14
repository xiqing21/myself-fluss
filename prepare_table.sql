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

CREATE TABLE fluss_catalog.my_db.delta_join_sink
(
    auction           BIGINT
    ,bidder           BIGINT
    ,price            BIGINT
    ,channel          VARCHAR
    ,url              VARCHAR
    ,bid_dateTime     TIMESTAMP(3)
    ,bid_extra        VARCHAR
    ,itemName         VARCHAR
    ,description      VARCHAR
    ,initialBid       BIGINT
    ,reserve          BIGINT
    ,auction_dateTime TIMESTAMP(3)
    ,expires          TIMESTAMP(3)
    ,seller           BIGINT
    ,category         BIGINT
    ,auction_extra    VARCHAR
    ,PRIMARY KEY (auction, bidder) NOT ENFORCED
)
;

CREATE TABLE IF NOT EXISTS fluss_catalog.my_db.bid
(
    auction     BIGINT
    ,bidder     BIGINT
    ,price      BIGINT
    ,channel    VARCHAR
    ,url        VARCHAR
    ,`dateTime` TIMESTAMP(3)
    ,extra      VARCHAR
    ,PRIMARY KEY (auction, bidder) NOT ENFORCED
)
WITH (
    'bucket.key' = 'auction'
    ,'table.delete.behavior' = 'IGNORE'
)
;

CREATE TABLE IF NOT EXISTS fluss_catalog.my_db.auction
(
    id           BIGINT
    ,itemName    VARCHAR
    ,description VARCHAR
    ,initialBid  BIGINT
    ,reserve     BIGINT
    ,`dateTime`  TIMESTAMP(3)
    ,expires     TIMESTAMP(3)
    ,seller      BIGINT
    ,category    BIGINT
    ,extra       VARCHAR
    ,PRIMARY KEY (id) NOT ENFORCED
)
WITH ('table.delete.behavior' = 'IGNORE')
;

CREATE TABLE IF NOT EXISTS fluss_catalog.my_db.regular_join_sink
(
    auction           BIGINT
    ,bidder           BIGINT
    ,price            BIGINT
    ,channel          VARCHAR
    ,url              VARCHAR
    ,bid_dateTime     TIMESTAMP(3)
    ,bid_extra        VARCHAR
    ,itemName         VARCHAR
    ,description      VARCHAR
    ,initialBid       BIGINT
    ,reserve          BIGINT
    ,auction_dateTime TIMESTAMP(3)
    ,expires          TIMESTAMP(3)
    ,seller           BIGINT
    ,category         BIGINT
    ,auction_extra    VARCHAR
    ,PRIMARY KEY (auction, bidder) NOT ENFORCED
)
;