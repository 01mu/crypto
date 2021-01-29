#
# crypto
# github.com/01mu
#

import sys
import psycopg2
import json
import urllib
import urllib.request as urlre
import time
import pymysql
import arrow
import re
from bs4 import BeautifulSoup

def main():
    conn = make_conn('../res/credentials')

    {'ath': get_ath,'reddit': update_reddit,
        'heat-map': update_heat_map,
        'coins': update_coins, 'biz-delete': biz_delete,
        'biz-24h': update_biz_24h, 'news': news,
        'biz-update': update_biz}.get(sys.argv[1])(conn)

def get_ath(conn):
    cur = conn.cursor()
    cur.execute('SELECT symbol FROM coins')

    for coin in cur.fetchall():
        url = ('https://min-api.cryptocompare.com/data/histoday' +
            '?fsym=' + coin[0] + '&tsym=USD&limit=2000&aggregate=1&e=CCCAGG')

        recent = read_json(url)

        if recent['Response'] == 'Error':
            continue

        url = ('https://min-api.cryptocompare.com/data/histoday' +
            '?fsym=' + coin[0] + '&tsym=USD&limit=2000&aggregate=1&e=CCCAGG' +
            '&toTs=' + str(recent['Data'][0]['time']))

        later = read_json(url)

        data = later['Data']
        data += recent['Data']

        max_price = 0

        for point in data:
            values = (coin[0], point['close'], point['time'])

            if point['close'] > max_price:
                cur.execute('INSERT INTO ath (symbol, ath, time) \
                    VALUES (%s, %s, %s)', values)

                print('Inserted: ' + str(values))

                max_price = point['close']

    conn.commit()

def news(conn):
    cur = conn.cursor()

    api = read_file('../res/news')[0]

    url = ('https://newsapi.org/v2/everything?' +
        'qInTitle=(bitcoin%20OR%20cryptocurrency%20OR%20ethereum)' +
        '&pageSize=100&language=en&sortBy=publishedAt&apiKey=' + api)

    cur.execute('SELECT input_value FROM key_values WHERE input_key = \
        "news_last_update"')

    last_update = cur.fetchone()

    if last_update != None:
        last_update = int(last_update[0])
    else:
        last_update = 0

    response = read_json(url)

    for article in response['articles']:
        source = article['source']['name']
        url = article['url']
        title = article['title']
        image = article['urlToImage']
        published = arrow.get(article['publishedAt']).timestamp

        if published > last_update:
            try:
                q = 'INSERT INTO news (title, source, url, image, \
                published) VALUES (%s, %s, %s, %s, %s)'

                cur.execute(q, (title, source, url, image, published, ))
            except:
                continue

    insert_value(cur, 'news_last_update', int(time.time()))
    conn.commit()

def biz_delete(conn):
    cur = conn.cursor()

    cur.execute('DELETE FROM biz_posts')
    cur.execute('DELETE FROM biz_counts')
    cur.execute('DELETE FROM biz_relations')
    cur.execute('DELETE FROM biz_counts_24h')
    cur.execute('DELETE FROM key_values WHERE input_key = "last_post_no"')

    conn.commit()

def get_biz_last_post(cur):
    cur.execute('SELECT input_value FROM key_values WHERE input_key = \
        "last_post_no"')

    try:
        last_post_no = int(cur.fetchone()[0])
    except:
        last_post_no = 0

    return last_post_no

def update_biz_24h(conn):
    cur = conn.cursor()
    cur.execute('SELECT coin_id, name_count, symbol_count, total FROM biz_counts')

    for count_data in cur.fetchall():
        coin_id = count_data[0]
        cur.execute('SELECT coin_id FROM biz_counts_24h \
            WHERE coin_id = %s', (coin_id, ))

        if cur.fetchone() == None:
            cur.execute('INSERT INTO biz_counts_24h (coin_id, \
                name_count, symbol_count, name_count_prev, \
                symbol_count_prev, total_prev, total) \
                VALUES (%s, %s, %s, 0, 0, 0, %s)',
                (count_data[0], count_data[1], count_data[2],
                    count_data[3]))
        else:
            cur.execute('SELECT name_count, symbol_count, total \
                FROM biz_counts_24h WHERE coin_id = %s',
                (coin_id, ))

            previous = cur.fetchall()[0]

            cur.execute('SELECT name_count, symbol_count, total \
                FROM biz_counts WHERE coin_id = %s', (coin_id, ))

            current = cur.fetchall()[0]

            cur.execute('UPDATE biz_counts_24h SET name_count = %s, \
                symbol_count = %s, total = %s, name_count_prev = %s, \
                symbol_count_prev = %s, total_prev = %s WHERE coin_id = %s',
                (current[0], current[1], current[2],
                    previous[0], previous[1], previous[2], coin_id))

    last_post_no = get_biz_last_post(cur)

    cur.execute('DELETE FROM biz_counts')
    cur.execute('DELETE FROM key_values WHERE input_key = "last_post_no"')

    insert_value(cur, 'last_update_biz', int(time.time()))

    conn.commit()

def get_biz_posts(cur):
    url = 'http://a.4cdn.org/biz/'
    posts = []

    last_post_no = get_biz_last_post(cur)
    max_post_no = 0

    for page in read_json(url + 'threads.json'):
        for thread in page['threads']:
            try:
                v = read_json(url + 'thread/' + str(thread['no']) + '.json')
            except:
                continue

            for post in v['posts']:
                post_no = post['no']

                if post_no > last_post_no:
                    print('Appending post ID: ' + str(post_no))
                    posts.append(post)
                    max_post_no = max(post_no, max_post_no)

            time.sleep(1)

        time.sleep(1)

    insert_value(cur, 'last_post_no', max_post_no)

    return posts

def parse_posts(counts, posts, coins, distinct_posts):
    for post in posts:
        try:
            comment = post['com'].lower()
            comment = re.sub(r'[^A-Za-z0-9 ]+', ' ', comment)
            comment = ' ' + comment + ' '
        except:
            continue

        soup = BeautifulSoup(post['com'])

        for a in soup.findAll('a'):
            a.replaceWithChildren()

        save_post = {'com': soup, 'time': post['time'],
            'thread': post['resto'], 'no': post['no']}

        for coin in coins:
            found = 0
            same_label = coin[0].lower() == coin[1].lower()

            if comment.find(' ' + coin[0] + ' ') != -1:
                counts[coin[4]]['name_count'] += 1
                found = 1

            if comment.find(' ' + coin[1] + ' ') != -1 and not same_label:
                counts[coin[4]]['symbol_count'] += 1
                found = 1

            if found:
                distinct_posts[post['no']] = save_post
                counts[coin[4]]['posts'].append(post['no'])

def init_coin_counts(cur, coins):
    counts = {}

    for coin in coins:
        counts[coin[4]] = {'name_count': 0, 'symbol_count': 0,
            'name': coin[2], 'rank': coin[3], 'symbol': coin[1],
            'posts': []}

    return counts

def insert_mention_counts(cur, counts, distinct_posts):
    for item in distinct_posts.items():
        post = item[1]
        values = (post['time'], post['no'], post['com'], post['thread'])

        cur.execute('INSERT INTO biz_posts (time, post_id, \
            comment, thread_id) VALUES (%s, %s, %s, %s)', values)

        print('Insert post: ' + str(values))

    for item in counts.items():
        coin_id = item[0]
        data = item[1]

        count_total = data['name_count'] + data['symbol_count']

        cur.execute('SELECT coin_id FROM biz_counts WHERE coin_id = %s',
            (coin_id, ))

        if cur.fetchone() == None:
            cur.execute('INSERT INTO biz_counts (name_count, \
                symbol_count, coin_id, total) \
                VALUES (%s, %s, %s, %s)',
                (data['name_count'], data['symbol_count'], coin_id,
                count_total))
        else:
           cur.execute('UPDATE biz_counts SET \
                total = total + ' + str(count_total) + ', \
                name_count = name_count + ' + str(data['name_count']) + ', \
                symbol_count = symbol_count + '
                + str(data['symbol_count']) + ' WHERE coin_id = %s',
                (coin_id, ))

        for post in data['posts']:
            values = (coin_id, post)

            cur.execute('INSERT INTO biz_relations (coin_id, post_id) \
                VALUES (%s, %s)', values)

            print('Insert relation: ' + str(values))

def update_biz(conn):
    cur = conn.cursor()

    cur.execute('SELECT lower(name) as lower_name, lower(symbol), name, \
        `rank`, coin_id FROM coins')

    coins = cur.fetchall()

    distinct_posts = {}

    counts = init_coin_counts(cur, coins)
    posts = get_biz_posts(cur)

    parse_posts(counts, posts, coins, distinct_posts)
    insert_mention_counts(cur, counts, distinct_posts)

    conn.commit()

def update_reddit(conn):
    url = ('https://www.reddit.com/r/CryptoCurrency/search.json?' +
        'q=subreddit%3Acryptocurrency+%22daily+discussion%22&' +
        'sort=new&t=all')

    j = read_json(url)

    for i in range(len(j['data']['children'])):
        print(j['data']['children'][i]['data']['title'])

def update_heat_map(conn):
    cur = conn.cursor()

    cur.execute('SELECT symbol, `rank` FROM coins WHERE `rank` <= 100')

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

            q = 'INSERT INTO heat_map (`rank`, symbol, time, instance, \
                difference) VALUES (%s, %s, %s, %s, %s)'

            vals = (coin[1], coin[0], timestamp, 1, diff)
            cur.execute(q, vals)

            print('Insert: ' + str(vals))

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

def insert_value(cur, key, value):
    cur.execute('SELECT input_key FROM key_values WHERE input_key = %s', (key,))

    if cur.fetchone() == None:
        q = 'INSERT INTO key_values (input_key, input_value) VALUES (%s, %s)'
        vals = (key, value)
        print('Insert: ' + str(vals))
    else:
        q = 'UPDATE key_values SET input_value = %s WHERE input_key = %s'
        vals = (value, key)
        print('Update: ' + str(vals))

    cur.execute(q, vals)

def update_rates(conn):
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

def update_coins(conn):
    cur = conn.cursor()
    opt = read_file('../res/opt')
    cmc_key = opt[0]
    cmc_limit = opt[1]
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

        cur.execute("SELECT coin_id FROM coins WHERE coin_id = '%s'",
            (coin_id,))

        if cur.fetchone() == None:
            q = 'INSERT INTO coins (coin_id, `rank`, name, symbol, slug, \
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

            print('Insert: ' + str(vals))
        else:
            q = "UPDATE coins SET `rank` = %s, name = %s, symbol = %s, \
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

            print('Update: ' + str(vals))

        cur.execute(q, vals)

    print('\nDeleted: ' + str(to_delete))

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
    return list(map(str.strip, open(file_name, 'r').readlines()))

def make_conn(cred_file):
    creds = read_file(cred_file)

    if creds[0] == 'mysql':
        if len(creds) == 7:
            return pymysql.connect(charset='utf8',
            db = creds[1],
                user = creds[2],
                passwd = creds[3],
                unix_socket = creds[6])
        else:
            return pymysql.connect(charset='utf8',
            db = creds[1],
                user = creds[2],
                passwd = creds[3])
    else:
        return psycopg2.connect(database = creds[1],
            user = creds[2],
            password = creds[3],
            host = creds[4],
            port = creds[5])

def read_json(page):
    return json.loads(urlre.urlopen(page).read())

if __name__ == '__main__':
    main()
