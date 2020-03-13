#!/usr/bin/env python

#
# crypto
# github.com/01mu
#

import sys
import psycopg2
import json
import urllib
import time
import MySQLdb

def main():
    conn = make_conn('credentials')
    conn.set_character_set('utf8')

    opt = read_file('opt')

    cmc_key = opt[0]
    cmc_limit = opt[1]

    if sys.argv[1] == 'reddit':
        reddit(conn)

    if sys.argv[1] == 'heat-map':
        heat_map(conn)

    if sys.argv[1] == 'tables':
        create_tables(conn)

    if sys.argv[1] == 'coins':
        get_coins(conn, cmc_key, cmc_limit)

    if sys.argv[1] == 'rates':
        cc_exchange(conn)

    if sys.argv[1] == 'biz-delete':
        cur = conn.cursor()

        cur.execute('DELETE FROM biz_posts')
        cur.execute('DELETE FROM biz_counts')

        print 'biz deleted'

        conn.commit()

    if sys.argv[1] == 'biz-24h':
        biz_24h(conn)

    if sys.argv[1] == 'biz-update':
        cur = conn.cursor()

        recent = get_recent_biz_post(cur)
        cutoff = get_cutoff(cur)

        biz_posts(conn)
        biz_counts(conn, recent, cutoff)

        cur.execute('DELETE FROM biz_posts WHERE added <= %s', (cutoff,))
        conn.commit()

    if sys.argv[1] == 'test':
        print int(time.time())

def biz_24h(conn):
    cur = conn.cursor()

    cur.execute('SELECT coin_id, mention_count FROM biz_counts')

    for coin in cur.fetchall():
        q = 'UPDATE biz_counts SET count_24h = %s WHERE coin_id = %s'
        vals = (coin[1], coin[0])

        cur.execute(q, vals)

        print 'update: ' + str(vals)

    insert_value(cur, 'last_update_biz_24h', int(time.time()))

    conn.commit()

def reddit(conn):
    url = ('https://www.reddit.com/r/CryptoCurrency/search.json?' +
        'q=subreddit%3Acryptocurrency+%22daily+discussion%22&' +
        'sort=new&t=all')

    j = read_json(url)

    for i in range(len(j['data']['children'])):
        print j['data']['children'][i]['data']['title']


def heat_map(conn):
    cur = conn.cursor()

    cur.execute('SELECT symbol, rank FROM coins WHERE rank <= 100')

    for coin in cur.fetchall():
        url = ('https://min-api.cryptocompare.com/data/histoday' +
            '?fsym=' + coin[0] + '&tsym=USD&limit=20&aggregate=1&e=CCCAGG')

        j = read_json(url)

        prev = 0

        for i in range(len(j['Data'])):
            timestamp = j['Data'][i]['time']
            price = j['Data'][i]['high']

            diff = get_change(price, prev)

            if price < prev:
                diff = diff * -1

            prev = price

            diff = str(round(diff, 2))

            q = 'INSERT INTO heat_map (rank, symbol, time, instance, \
                difference) VALUES (%s, %s, %s, %s, %s)'

            vals = (coin[1], coin[0], timestamp, 1, diff)
            cur.execute(q, vals)

            print 'insert: ' + str(vals)

    cur.execute('DELETE FROM heat_map WHERE instance = 0')
    cur.execute('UPDATE heat_map SET instance = 0 WHERE instance = 1')

    insert_value(cur, 'last_update_heat_map', int(time.time()))

    conn.commit()

def get_change(current, previous):
    if current == previous:
        return 0
    try:
        return (abs(current - previous) / previous) * 100
    except ZeroDivisionError:
        return 0

def get_cutoff(cur):
    cutoff = int(time.time()) - 86400

    q = 'SELECT post_id FROM biz_posts WHERE added <= %s \
        ORDER BY added DESC LIMIT 1'

    cur.execute(q, (cutoff,))

    try:
        cutoff = cur.fetchone()[0]
    except:
        cutoff = 0

    return cutoff


def get_recent_biz_post(cur):
    q = 'SELECT post_id FROM biz_posts ORDER BY added DESC LIMIT 1'
    cur.execute(q)

    try:
        recent = cur.fetchone()[0]
    except:
        recent = 0

    return recent

def biz_counts(conn, recent, cutoff):
    cur = conn.cursor()

    cur.execute('SELECT coin_id, name, symbol, rank FROM coins')

    for coin in cur.fetchall():
        coin_id = coin[0]
        name = coin[1]
        symbol = coin[2]
        rank = coin[3]

        name_c = '% ' + name + ' %';
        name_l = '% ' + name;
        name_r = name + ' %';

        '''name_cu = '% ' + name.lower() + ' %';
        name_lu = '% ' + name.lower();
        name_ru = name.lower() + ' %';'''

        symbol_c = '% ' + symbol +' %';
        symbol_l = '% ' + symbol;
        symbol_r = symbol + ' %';

        q = 'SELECT COUNT(id) FROM biz_posts WHERE \
            (comment LIKE %s OR comment LIKE %s OR comment LIKE %s \
            OR comment LIKE %s OR comment LIKE %s OR comment LIKE %s) \
            AND added > %s'

        vals = (name_c, name_l, name_r, symbol_c, symbol_l, symbol_r,
            recent)

        cur.execute(q, vals)

        mention_count = cur.fetchone()[0]

        q = 'SELECT id FROM biz_counts WHERE coin_id = %s'
        vals = (coin_id,)
        cur.execute(q, vals)

        if cur.fetchone() == None:
            q = 'INSERT INTO biz_counts (rank, name, symbol, coin_id, \
                mention_count, count_24h, change_24h) \
                VALUES (%s, %s, %s, %s, %s, %s, %s)'

            vals = (rank, name, symbol, coin_id, mention_count, 0, 0)

            print 'insert: ' + str(vals)
        else:
            #
            # Select old mention count and 24h count from table.
            #
            q = 'SELECT mention_count, count_24h FROM biz_counts \
                WHERE coin_id = %s'

            args = (coin_id,)
            cur.execute(q, args)

            res = cur.fetchall()[0]

            old_count = res[0]
            count_24h = res[1]

            #
            # Get mention count from posts older than 24 hours.
            #
            q = 'SELECT COUNT(id) FROM biz_posts WHERE \
                (comment LIKE %s OR comment LIKE %s OR comment LIKE %s \
                OR comment LIKE %s OR comment LIKE %s OR comment LIKE %s) \
                AND post_id <= %s'

            vals = (name_c, name_l, name_r, symbol_c, symbol_l, symbol_r,
                cutoff)

            cur.execute(q, vals)

            older_24h = cur.fetchone()[0]

            #
            # Determine mention count for coin over the last 24 hours and
            # the 24 hour change.
            #
            q = 'UPDATE biz_counts SET rank = %s, mention_count = %s, \
                    change_24h = %s WHERE coin_id = %s'

            new_count = old_count + mention_count - older_24h

            change_24h = abs(count_24h - new_count)

            if new_count < count_24h:
                change_24h = change_24h * -1

            vals = (rank, new_count, change_24h, coin_id)

            print 'update: ' + str(vals)

        cur.execute(q, vals)

    insert_value(cur, 'last_update_biz_counts', int(time.time()))

    conn.commit()

def biz_posts(conn):
    cur = conn.cursor()

    url = 'http://a.4cdn.org/biz/threads.json'
    j = read_json(url)

    for page in j:
        for thread in page['threads']:
            thread_id = str(thread['no'])

            url = 'http://a.4cdn.org/biz/thread/' + thread_id + '.json'
            j = read_json(url)

            for post in j['posts']:
                try:
                    comment = post['com']
                except:
                    continue

                timestamp = post['time']
                post_id = post['no']

                vals = (post_id, comment, timestamp, int(time.time()))

                q = 'SELECT id FROM biz_posts WHERE post_id = %s'

                cur.execute(q, (post_id,))

                if cur.fetchone() == None:
                    q = 'INSERT INTO biz_posts (post_id, comment, timestamp, \
                        added) VALUES (%s, %s, %s, %s)'

                    cur.execute(q, vals)

                    print 'insert: ' + str(vals)
                else:
                    print 'skipped: ' + str(vals)


    conn.commit()

def insert_value(cur, key, value):
    cur.execute('SELECT id FROM key_values WHERE input_key = %s', (key,))

    if cur.fetchone() == None:
        q = 'INSERT INTO key_values (input_key, input_value) VALUES (%s, %s)'
        vals = (key, value)

        print 'insert: ' + str(vals)
    else:
        q = 'UPDATE key_values SET input_value = %s WHERE input_key = %s'
        vals = (value, key)

        print 'update: ' + str(vals)

    cur.execute(q, vals)

def cc_exchange(conn):
    cur = conn.cursor()

    vals = ['CAD', 'EUR', 'GBP', 'INR', 'MXN']

    url = ('https://min-api.cryptocompare.com/data/' +
        'price?fsym=USD&tsyms=CAD,EUR,GBP,INR,MXN')

    j = read_json(url)

    for val in vals:
        insert_value(cur, 'USD_to_' + val, j[val])

    conn.commit()

def remove_diff_coins(cur, coins):
    old_coins = []
    new_coins = []

    cur.execute('SELECT coin_id FROM coins')

    for coin_id in cur.fetchall():
        old_coins.append(int(coin_id[0]))

    for coin in coins:
        new_coins.append(int(coin['id']))

    to_delete = list(set(old_coins).difference(new_coins))

    for coin in to_delete:
        cur.execute("DELETE FROM coins WHERE coin_id = '%s'", (coin,))

    return to_delete

def get_coins(conn, cmc_key, cmc_limit):
    cur = conn.cursor()

    coin_vals = get_btc_eth(cmc_key)
    global_vals = get_global(cmc_key)

    cv = ['btc_price', 'eth_price', 'btc_mcap', 'eth_mcap']
    gv = ['total_markets', 'total_coins', 'total_market_cap',
        'total_volume_24h']

    btc_price = coin_vals[0]
    eth_price = coin_vals[1]
    btc_mcap = coin_vals[2]
    eth_mcap = coin_vals[3]

    total_markets = global_vals[0]
    total_coins = global_vals[1]
    total_market_cap = global_vals[2]
    total_volume_24h = global_vals[3]

    for i in range(len(coin_vals)):
        insert_value(cur, cv[i], coin_vals[i])
        insert_value(cur, gv[i], global_vals[i])

    insert_value(cur, 'btc_dominance', btc_mcap / total_market_cap * 100)
    insert_value(cur, 'eth_dominance', eth_mcap / total_market_cap * 100)

    d = btc_price / eth_price

    coins = get_cmc_coins(cmc_key, cmc_limit)

    to_delete = remove_diff_coins(cur, coins)

    for coin in coins:
        coin_id = coin['id']
        rank = coin['cmc_rank']
        name = coin['name']
        symbol = coin['symbol']
        slug = coin['slug']

        price_btc = float(coin['quote']['BTC']['price'])
        price_usd = float(price_btc * btc_price)
        price_eth = float(price_btc * d)

        total_supply = float(coin['total_supply'])
        circulating_supply = coin['circulating_supply']
        max_supply = coin['max_supply']

        change_1h = coin['quote']['BTC']['percent_change_1h']
        change_24h = coin['quote']['BTC']['percent_change_24h']
        change_7d = coin['quote']['BTC']['percent_change_7d']

        try:
            market_cap = coin['quote']['BTC']['market_cap'] * btc_price
            market_cap_percent = market_cap / total_market_cap * 100
        except:
            market_cap = market_cap_percent = 0

        try:
            volume_24h = coin['quote']['BTC']['volume_24h'] * btc_price
            volume_24h_percent = volume_24h / total_volume_24h * 100
        except:
            volume_24h = volume_24h_percent = 0

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

    print '\ndeleted: ' + str(to_delete)

    insert_value(cur, 'last_update_coins', int(time.time()))

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

def make_conn(cred_file):
    creds = read_file(cred_file)

    if creds[0] == 'mysql':
        if len(creds) == 7:
            return MySQLdb.connect(db = creds[1],
                user = creds[2],
                passwd = creds[3],
                unix_socket = creds[6])
        else:
            return MySQLdb.connect(db = creds[1],
                user = creds[2],
                passwd = creds[3])
    else:
        return psycopg2.connect(database = creds[1],
            user = creds[2],
            password = creds[3],
            host = creds[4],
            port = creds[5])

def read_json(page):
    return json.loads(urllib.urlopen(page).read())

def create_tables(conn):
    cur = conn.cursor()

    cmds = ["CREATE TABLE biz_counts()",
            "ALTER TABLE biz_counts ADD COLUMN id SERIAL PRIMARY KEY",
            "ALTER TABLE biz_counts ADD COLUMN name TEXT",
            "ALTER TABLE biz_counts ADD COLUMN symbol TEXT",
            "ALTER TABLE biz_counts ADD COLUMN coin_id TEXT",
            "ALTER TABLE biz_counts ADD COLUMN rank INT",
            "ALTER TABLE biz_counts ADD COLUMN mention_count INT",
            "ALTER TABLE biz_counts ADD COLUMN count_24h INT",
            "ALTER TABLE biz_counts ADD COLUMN change_24h INT",

            "CREATE TABLE biz_posts()",
            "ALTER TABLE biz_posts ADD COLUMN id SERIAL PRIMARY KEY",
            "ALTER TABLE biz_posts ADD COLUMN comment TEXT",
            "ALTER TABLE biz_posts ADD COLUMN timestamp INT",
            "ALTER TABLE biz_posts ADD COLUMN added INT",

            "CREATE TABLE values()",
            "ALTER TABLE values ADD COLUMN id SERIAL PRIMARY KEY",
            "ALTER TABLE values ADD COLUMN input_key TEXT",
            "ALTER TABLE values ADD COLUMN input_value TEXT",

            "CREATE TABLE coins()",
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
