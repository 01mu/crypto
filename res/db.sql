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
    name_count INT, symbol_count INT,
    PRIMARY KEY(coin_id),
    FOREIGN KEY(coin_id) REFERENCES coins (coin_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE);

CREATE TABLE biz_counts_24h(coin_id INT,
    name_count INT, symbol_count INT, total INT,
    name_count_prev INT, symbol_count_prev INT,
    PRIMARY KEY (coin_id),
    FOREIGN KEY(coin_id) REFERENCES coins (coin_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE);

CREATE TABLE biz_posts(coin_id INT, time INT, post_id INT, comment TEXT,
    FOREIGN KEY(coin_id) REFERENCES coins (coin_id)
    ON UPDATE CASCADE
    ON DELETE CASCADE);

CREATE TABLE heat_map(`rank` INT, symbol TEXT, time INT,
    instance INT, difference FLOAT);

CREATE TABLE news(title TEXT, source TEXT, url TEXT, image TEXT, published INT);

ALTER TABLE news MODIFY COLUMN title TEXT
    CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL;

ALTER TABLE biz_posts MODIFY COLUMN comment TEXT
    CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL;

CREATE TABLE key_values(input_key TEXT, input_value TEXT);
