CREATE DATABASE crypto;

USE crypto;

CREATE TABLE coins(coin_id INT, name TEXT,
    symbol TEXT, slug TEXT, `rank` INT, price_btc FLOAT,
    price_usd FLOAT, price_eth FLOAT, total_supply FLOAT,
    circulating_supply FLOAT, max_supply FLOAT,
    change_1h FLOAT, change_24h FLOAT, change_7d FLOAT,
    market_cap FLOAT, market_cap_percent FLOAT,
    volume_24h FLOAT, volume_24h_percent FLOAT,
    PRIMARY KEY (coin_id));

CREATE TABLE biz_counts(coin_id INT,
    name_count INT, symbol_count INT, total INT,
    PRIMARY KEY(coin_id),
    FOREIGN KEY(coin_id) REFERENCES coins (coin_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE);

CREATE TABLE biz_counts_24h(coin_id INT,
    name_count INT, symbol_count INT, total INT,
    name_count_prev INT, symbol_count_prev INT, total_prev INT,
    PRIMARY KEY (coin_id),
    FOREIGN KEY(coin_id) REFERENCES coins (coin_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE);

CREATE TABLE biz_posts(time INT, post_id INT, thread_id INT, comment TEXT,
    PRIMARY KEY (post_id));

CREATE TABLE biz_relations(coin_id INT, post_id INT,
    FOREIGN KEY(post_id) REFERENCES biz_posts (post_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
    FOREIGN KEY(coin_id) REFERENCES coins (coin_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE);

CREATE TABLE biz_timeline(coin_id INT, time INT, mentions INT,
    FOREIGN KEY(coin_id) REFERENCES coins (coin_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE);

CREATE TABLE biz_total_posts(time INT, count INT);

CREATE table ath(symbol TEXT, ath FLOAT, time INT);

CREATE TABLE heat_map(`rank` INT, symbol TEXT, time INT,
    instance INT, difference FLOAT);

CREATE TABLE news(title TEXT, source TEXT, url TEXT, image TEXT, published INT);

ALTER TABLE news MODIFY COLUMN title TEXT
    CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL;

ALTER TABLE biz_posts MODIFY COLUMN comment TEXT
    CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL;

CREATE TABLE key_values(input_key TEXT, input_value TEXT);
