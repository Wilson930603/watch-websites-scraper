#!pip install PyMySQL
import json
import math

import pymysql
import pandas
from datetime import datetime, timedelta

host = 'xxxxx'
username = 'xxxxx'
password = 'xxxxx'
history_db_name = 'xxxxx'

sale_date_databases = ['ebay', 'watchcharts']

watchanalytics_data = None
conn = pymysql.connect(host=host,
                       user=username,
                       password=password,
                       charset='utf8mb4',
                       )
cursor = conn.cursor()


def main():
    global watchanalytics_data

    createDataBaseIfNotExist()

    for history_table_days in [7, 30, 90, 180, 365]:
        conn.select_db(history_db_name)
        watchanalytics_data = None
        history_table_name = f'history_{history_table_days}_days'

        # drop current history table if exists
        try:
            sql = f"DROP TABLE IF EXISTS {history_table_name}"
            cursor.execute(sql)
            print(f'DROP TABLE {history_table_name} DONE SUCCESSFULLY')
        except Exception as e:
            print(f'FAILED TO DROP TABLE {history_table_name}, {e}')

        # create history table with new values
        createHistoryTable(history_table_days, history_table_name)

    conn.commit()
    conn.close()


def createHistoryTable(history_table_days, history_table_name):
    global watchanalytics_data

    print(f"START CREATING HISTORY TABLE OF : {history_table_days} DAYS")
    databases_dict = dict()

    createHistoryTableIfNotExist(history_table_name)

    today = datetime.today()
    start_date = today - timedelta(days=history_table_days)
    dates = [d.strftime('%Y-%m-%d') for d in pandas.date_range(start_date, today)][:-1]
    print(f"\tSTART DATE: {dates[0]}, END DATE: {dates[-1]}")

    cursor.execute("show databases")

    databases = cursor.fetchall()

    for database in databases:
        database_name = database[0]
        conn.select_db(database_name)

        cursor.execute("SHOW TABLES")
        tables = [item for sublist in cursor.fetchall() for item in sublist]
        if 'listing' in tables:
            # print(database_name)

            cursor.execute('select * from listing LIMIT 1;')
            column_names = [description[0] for description in cursor.description]

            if all(x in column_names for x in ['scraping_date', 'watches_db_foreign_key', 'price', 'sale_date']):
                sql = 'SELECT scraping_date, watches_db_foreign_key, price, sale_date FROM listing ' \
                      'WHERE watches_db_foreign_key != "" ' \
                      'AND (scraping_date BETWEEN "%s" AND "%s" OR sale_date BETWEEN "%s" AND "%s") ' \
                      'ORDER BY scraping_date, sale_date;' % \
                      (dates[0], dates[-1], dates[0], dates[-1])
            elif all(x in column_names for x in ['scraping_date', 'watches_db_foreign_key', 'price']):
                sql = 'SELECT scraping_date, watches_db_foreign_key, price FROM listing ' \
                      'WHERE watches_db_foreign_key != "" ' \
                      'AND (scraping_date BETWEEN "%s" AND "%s") ' \
                      'ORDER BY scraping_date;' % \
                      (dates[0], dates[-1])
            else:
                sql = ''

            if sql:
                try:
                    cursor.execute(sql)
                    returned_data = cursor.fetchall()
                    databases_dict[database_name] = list(returned_data)
                    print(f'GOT "listing" TABLE DATA OF DATABASE: "{database_name}"')
                except Exception:
                    print(f'FAILED TO GET "listing" TABLE DATA OF DATABASE: "{database_name}"')

    foreign_key_list = list(set([row[1] for database_list in databases_dict.values() for row in database_list if row[1]]))
    print(f'Total number of unique watches_db_foreign_key in last {history_table_days} days = {len(foreign_key_list)}')

    watchanalytics_data = databases_dict.get('watchanalytics', None)

    watches_fk_dict = dict()
    for database_name, database_list in databases_dict.items():
        for row in database_list:
            if row[2]:
                scrape_date = row[0]
                fk = row[1]
                price = get_int(row[2])

                if not price:
                    continue

                try:
                    scrape_date_index = dates.index(scrape_date)
                except ValueError:
                    scrape_date_index = None

                sale_date_index = None
                if len(row) > 3:
                    sale_date = row[3]
                    try:
                        sale_date_index = dates.index(sale_date)
                    except ValueError:
                        pass

                # initialize watches_fk_dict[fk] dict
                if fk not in watches_fk_dict.keys():
                    watches_fk_dict[fk] = dict()
                    watches_fk_dict[fk]['listings'] = [list() for i in range(history_table_days)]
                    watches_fk_dict[fk]['sales'] = [list() for i in range(history_table_days)]
                    watches_fk_dict[fk]['total'] = [list() for i in range(history_table_days)]
                    watches_fk_dict[fk]['transactions'] = 0

                # append price to listing column list
                if scrape_date_index:
                    watches_fk_dict[fk]['listings'][scrape_date_index].append(price)

                if sale_date_index:
                    watches_fk_dict[fk]['sales'][sale_date_index].append(price)
                    watches_fk_dict[fk]['transactions'] = watches_fk_dict[fk].get('transactions', 0) + 1

                if database_name in sale_date_databases and sale_date_index:
                    watches_fk_dict[fk]['total'][sale_date_index].append(price)
                elif scrape_date_index:
                    watches_fk_dict[fk]['total'][scrape_date_index].append(price)

    # calculate liquid ranking
    ranking_list = sorted(watches_fk_dict.keys(), key=lambda d: watches_fk_dict[d]['transactions'], reverse=True)
    for i, fk in enumerate(ranking_list):
        watches_fk_dict[fk]['rank'] = i+1

    conn.select_db(history_db_name)
    for fk in watches_fk_dict.keys():
        reference_price = get_reference_price(fk)
        history_prices_list = watches_fk_dict[fk]['total']

        daily_price_aggregated = [int(0.8 * ((sum(x)/len(x)) if len(x) else 0) + 0.2 * ((sum(y)/len(y)) if len(y) else 0))
                                  for x, y in zip(watches_fk_dict[fk]['sales'], watches_fk_dict[fk]['listings'])]

        try:
            flatten_prices_list = [item for sublist in history_prices_list for item in sublist if item]
            if flatten_prices_list:
                open_price = flatten_prices_list[0]
                max_price = max(flatten_prices_list)
                min_price = min(flatten_prices_list)
                close_price = flatten_prices_list[-1]
                avg_price = average_10(flatten_prices_list)
                avg_selling_price = avg_80percent(flatten_prices_list)

                if reference_price:
                    stock_price = int(0.85 * reference_price)
                    sell_price = int(0.925 * reference_price)
                    if abs(avg_selling_price - stock_price) < abs(avg_selling_price - sell_price):
                        bear_bull = 'Fear(Bear)'
                    else:
                        bear_bull = 'Greed(Bull)'
                else:
                    stock_price, sell_price, bear_bull = '', '', ''

                if close_price < (0.75 * open_price):
                    buy_sell_hold = 'Buy'
                elif close_price > (1.25 * open_price):
                    buy_sell_hold = 'Sell'
                else:
                    buy_sell_hold = 'Hold'

            else:
                open_price, max_price, min_price, close_price, avg_price, avg_selling_price = '', '', '', '', '', ''
                stock_price, sell_price, bear_bull, buy_sell_hold = '', '', '', ''

            sql = f"INSERT INTO {history_table_name} (watches_db_foreign_key, daily_price_listing, daily_price_sales, " \
                  f"daily_price_aggregated, highest_price, lowest_price, recent_price, interval_transactions, " \
                  f"avg_price, avg_selling_price, popular_price, listing_count, transactions_count, " \
                  f"lowest_listed, highest_listed, " \
                  f"liquid_ranking, " \
                  f"suggested_stock_price_pre_tax, suggested_selling_price_pre_tax, bear_bull, buy_sell_hold" \
                  f") " \
                  f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
            val = (fk,
                   json.dumps(watches_fk_dict[fk]['listings']),
                   json.dumps(watches_fk_dict[fk]['sales']),
                   json.dumps(daily_price_aggregated),
                   max_price,
                   min_price,
                   close_price,
                   json.dumps([open_price, max_price, min_price, close_price]),
                   avg_price,
                   avg_selling_price,
                   most_popular(flatten_prices_list) if flatten_prices_list else '',
                   len(flatten_prices_list),
                   watches_fk_dict[fk].get('transactions', 0),
                   min_price,
                   max_price,
                   watches_fk_dict[fk].get('rank', 0),
                   stock_price, sell_price, bear_bull, buy_sell_hold
                   )
            cursor.execute(sql, val)
        except Exception as e:
            print(f'FAILED TO PARSE Foreign Key: {fk}, prices list {history_prices_list}, {e}')


def most_popular(lst):
    return max(set(lst), key=lst.count)


def get_int(item):
    try:
        return int(float(item.replace(',', '').strip()))
    except Exception as error:
        print(f'FAILED TO (int) {item}, {error}')


def average_10(price_list):
    if price_list:
        try:
            average_list = sorted(list(set(price_list)))
            middle = int(len(average_list) / 2)
            start = middle - 5
            if start < 0:
                start = 0

            new_list = average_list[start:start + 10]
            return int(sum(new_list) / len(new_list))
        except Exception as error:
            print(f'FAILED TO (avg) {price_list}, {error}')
    return 0


def avg_80percent(price_list):
    if price_list:
        try:
            average_list = sorted(list(set(price_list)))
            if len(average_list) > 10:
                trim = int(math.floor(len(average_list)/10))
                average_list = average_list[trim:-trim]
            else:
                average_list = price_list
            return int(sum(average_list) / len(average_list))
        except Exception as error:
            print(f'FAILED TO (avg_80) {price_list}, {error}')
    return 0


def createDataBaseIfNotExist():
    try:
        cursor.execute(f"CREATE DATABASE {history_db_name}")
        print(f'HISTORY DB "{history_db_name}" CREATED')
    except pymysql.err.ProgrammingError:
        print(f'HISTORY DB "{history_db_name}" ALREADY EXISTS')


def createHistoryTableIfNotExist(history_table_name):
    try:
        cursor.execute(f"CREATE TABLE {history_table_name} (`watches_db_foreign_key` VARCHAR(255) NOT NULL PRIMARY KEY,"
                       f" `daily_price_listing` text,"
                       f" `daily_price_sales` text,"
                       f" `daily_price_aggregated` text,"
                       f" `lowest_price` INTEGER,"
                       f" `highest_price` INTEGER,"
                       f" `avg_price` INTEGER,"
                       f" `recent_price` INTEGER,"
                       f" `popular_price` INTEGER,"
                       f" `avg_selling_price` INTEGER,"
                       f" `lowest_listed` INTEGER,"
                       f" `highest_listed` INTEGER,"
                       f" `listing_count` INTEGER,"
                       f" `transactions_count` INTEGER,"
                       f" `liquid_ranking` INTEGER,"
                       f" `interval_transactions` text,"
                       f" `suggested_stock_price_pre_tax` INTEGER,"
                       f" `suggested_selling_price_pre_tax` INTEGER,"
                       f" `bear_bull` text,"
                       f" `buy_sell_hold` text"
                       f");"
                       )
        print(f'HISTORY TABLE "{history_table_name}" CREATED')
    except pymysql.err.OperationalError:
        print(f'HISTORY TABLE "{history_table_name}" ALREADY EXISTS')


def get_reference_price(fk):
    if watchanalytics_data:
        for row in reversed(watchanalytics_data):
            if fk == row[1] and row[2]:
                return row[2]
    return 0


if __name__ == '__main__':
    main()
