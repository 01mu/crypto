#!/usr/bin/env python

import sys
import psycopg2
import json
import urllib

def main():
    conn = psql('psql')
    keys = read_file('keys')

    cmc_key = keys[0]
    cmc_limit = 500

    if sys.argv[1] == 'tables':
        create_tables(conn)

    if sys.argv[1] == 'coins':
        get_coins(conn, cmc_key, cmc_limit)

def cc_exchange():
    url = ('https://min-api.cryptocompare.com/data/' +
        'price?fsym=USD&tsyms=CAD,EUR,GBP,INR,MXN')

    j = read_json(url)

def get_coins(conn, cmc_key, cmc_limit):
    cur = conn.cursor()

    coin_vals = get_btc_eth(cmc_key)
    global_vals = get_global(cmc_key)

    btc_price = coin_vals[0]
    eth_price = coin_vals[1]
    btc_mcap = coin_vals[2]
    eth_mcap = coin_vals[3]

    total_markets = global_vals[0]
    total_coins = global_vals[1]
    total_market_cap = global_vals[2]
    total_volume_24h = global_vals[3]

    d = btc_price / eth_price

    for coin in get_cmc_coins(cmc_key, cmc_limit):
        coin_id = coin['id']
        rank = coin['cmc_rank']
        name = coin['name']
        symbol = coin['symbol']
        slug = coin['slug']

        price_btc = coin['quote']['BTC']['price']
        price_usd = price_btc * btc_price
        price_eth = price_btc * d

        total_supply = coin['total_supply']
        circulating_supply = coin['circulating_supply']
        max_supply = coin['max_supply']

        change_1h = coin['quote']['BTC']['percent_change_1h']
        change_24h = coin['quote']['BTC']['percent_change_24h']
        change_7d = coin['quote']['BTC']['percent_change_7d']

        market_cap = coin['quote']['BTC']['market_cap'] * btc_price;
        market_cap_percent = market_cap / total_market_cap * 100

        volume_24h = coin['quote']['BTC']['volume_24h'] * btc_price
        volume_24h_percent = volume_24h / total_volume_24h * 100

        cur.execute("SELECT id FROM coins WHERE coin_id = '%s'", (coin_id,))

        if cur.fetchone() == None:
            q = 'INSERT INTO coins (coin_id, rank, name, symbol, slug, \
                price_btc, price_usd, price_eth, total_supply, \
                circulating_supply, max_supply, change_1h, change_24h, \
                change_7d, market_cap, market_cap_percent, volume_24h, \
                volume_24h_percent) \
                VALUES (%s, %s, %s, %s, %s, \
                %s, %s, %s, %s, \
                %s, %s, %s, %s, \
                %s, %s, %s, %s, \
                %s)'

            vals = (coin_id, rank, name, symbol, slug,
                price_btc, price_usd, price_eth, total_supply,
                circulating_supply, max_supply, change_1h, change_24h,
                change_7d, market_cap, market_cap_percent, volume_24h,
                volume_24h_percent)

            print 'insert: ' + str(vals)
        else:
            q = "UPDATE coins SET rank = %s, name = %s, symbol = %s, \
                slug = %s, price_btc = %s, price_usd = %s, price_eth = %s, \
                total_supply = %s, circulating_supply = %s, \
                max_supply = %s, change_1h = %s, change_24h = %s, \
                change_7d = %s, market_cap = %s, market_cap_percent = %s, \
                volume_24h = %s, volume_24h_percent = %s \
                WHERE coin_id = '%s'"

            vals = (rank, name, symbol, slug,
                price_btc, price_usd, price_eth, total_supply,
                circulating_supply, max_supply, change_1h, change_24h,
                change_7d, market_cap, market_cap_percent, volume_24h,
                volume_24h_percent, coin_id)

            print 'update: ' + str(vals)

        cur.execute(q, vals)

    conn.commit()

def get_cmc_coins(cmc_key, limit):
    url = ('https://pro-api.coinmarketcap.com/' +
        'v1/cryptocurrency/listings/latest?sort=market_cap' +
        '&start=1&limit=' + str(limit) + '&cryptocurrency_type=all' +
        '&convert=BTC&CMC_PRO_API_KEY=' + cmc_key)

    return read_json(url)['data']

def get_global(cmc_key):
    v = []

    url = ('https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/' +
        'latest?convert=USD&CMC_PRO_API_KEY=' + cmc_key)

    j = read_json(url)

    v.append(j['data']['active_market_pairs'])
    v.append(j['data']['active_cryptocurrencies'])
    v.append(j['data']['quote']['USD']['total_market_cap'])
    v.append(j['data']['quote']['USD']['total_volume_24h'])

    return v

def get_btc_eth(cmc_key):
    v = []

    url = ('https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/' +
        'latest?symbol=BTC,ETH&convert=USD&CMC_PRO_API_KEY=' + cmc_key)

    j = read_json(url)

    v.append(j['data']['BTC']['quote']['USD']['price'])
    v.append(j['data']['ETH']['quote']['USD']['price'])
    v.append(j['data']['BTC']['quote']['USD']['market_cap'])
    v.append(j['data']['ETH']['quote']['USD']['market_cap'])

    return v

def read_file(file_name):
    return map(str.strip, open(file_name, 'r').readlines())

def psql(cred):
    creds = read_file(cred)

    return psycopg2.connect(database = creds[0],
                            user = creds[1],
                            password = creds[2],
                            host = creds[3],
                            port = creds[4])

def read_json(page):
    return json.loads(urllib.urlopen(page).read())

def create_tables(conn):
    cur = conn.cursor()

    cmds = ["CREATE TABLE coins()",
            "ALTER TABLE coins ADD COLUMN id SERIAL PRIMARY KEY",

            "ALTER TABLE coins ADD COLUMN name TEXT",
            "ALTER TABLE coins ADD COLUMN symbol TEXT",

            "ALTER TABLE coins ADD COLUMN coin_id TEXT",
            "ALTER TABLE coins ADD COLUMN slug TEXT",

            "ALTER TABLE coins ADD COLUMN rank INT",

            "ALTER TABLE coins ADD COLUMN price_btc DECIMAL",
            "ALTER TABLE coins ADD COLUMN price_usd DECIMAL",
            "ALTER TABLE coins ADD COLUMN price_eth DECIMAL",

            "ALTER TABLE coins ADD COLUMN total_supply DECIMAL",
            "ALTER TABLE coins ADD COLUMN circulating_supply DECIMAL",
            "ALTER TABLE coins ADD COLUMN max_supply DECIMAL",

            "ALTER TABLE coins ADD COLUMN change_1h DECIMAL",
            "ALTER TABLE coins ADD COLUMN change_24h DECIMAL",
            "ALTER TABLE coins ADD COLUMN change_7d DECIMAL",

            "ALTER TABLE coins ADD COLUMN market_cap DECIMAL",
            "ALTER TABLE coins ADD COLUMN market_cap_percent DECIMAL",

            "ALTER TABLE coins ADD COLUMN volume_24h DECIMAL",
            "ALTER TABLE coins ADD COLUMN volume_24h_percent DECIMAL",]

    for cmd in cmds:
        try:
            print cmd
            cur.execute(cmd)
        except Exception as e:
            conn.rollback()
            print e
            continue

    conn.commit()

if __name__ == '__main__':
    main()
