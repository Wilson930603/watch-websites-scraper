#!pip install PyMySQL
import json

import pymysql
import pandas
from datetime import datetime, timedelta

host = 'xxxxx'
username = 'xxxxx'
password = 'xxxxx'
brands_db_name = 'xxxxx'

brands_list = ['Rolex', 'Omega', 'Patek Philippe', 'Hublot', 'Bulgari']

sale_date_databases = ['ebay', 'watchcharts']

conn = pymysql.connect(host=host,
                       user=username,
                       password=password,
                       charset='utf8mb4',
                       )
cursor = conn.cursor()


def main():
    createDataBaseIfNotExist()
    conn.select_db(brands_db_name)

    for brand in brands_list:
        brand_table_name = f'{brand.title().replace(" ", "_").strip()}'
        createBrandTableIfNotExist(brand_table_name)

    for table_days in [7, 30, 90, 180, 365]:
        upsertBrandsTables(table_days)

    conn.commit()
    conn.close()


def upsertBrandsTables(table_days):
    database_list = list()
    today = datetime.today()
    start_date = today - timedelta(days=table_days)
    dates = [d.strftime('%Y-%m-%d') for d in pandas.date_range(start_date, today)][:-1]

    print(f"START UPDATING Brand TABLE OF DAYS: {table_days} DAYS")
    print(f"\tSTART DATE: {dates[0]}, END DATE: {dates[-1]}")

    cursor.execute("show databases")

    databases = cursor.fetchall()

    for database in databases:
        database_name = database[0]
        conn.select_db(database_name)

        if database_name not in sale_date_databases:
            continue

        cursor.execute("SHOW TABLES")
        tables = [item for sublist in cursor.fetchall() for item in sublist]
        if 'listing' in tables:
            # print(database_name)

            try:
                sql = 'SELECT sale_date, watches_db_foreign_key, price, brand FROM listing where ' \
                      'watches_db_foreign_key != "" AND brand != "" ' \
                      'AND sale_date BETWEEN "%s" AND "%s" ORDER BY sale_date;' % (dates[0], dates[-1])
                cursor.execute(sql)
                returned_data = cursor.fetchall()
                database_list += list(returned_data)
                print(f'GOT "listing" TABLE DATA OF DATABASE: "{database_name}"')
            except Exception:
                print(f'FAILED TO GET "listing" TABLE DATA OF DATABASE: "{database_name}"')

    foreign_key_list = list(set([row[1] for row in database_list]))
    print(f'Total number of unique watches_db_foreign_key in last {table_days} days = {len(foreign_key_list)}')

    brands_fk_dict = dict()
    for row in database_list:
        if row[2]:
            fk = row[1]
            price = row[2]
            brand = row[3]

            if brand in brands_list:
                if brand not in brands_fk_dict:
                    brands_fk_dict[brand] = dict()

                if fk not in brands_fk_dict[brand]:
                    brands_fk_dict[brand][fk] = dict()
                    brands_fk_dict[brand][fk]['prices'] = list()
                    brands_fk_dict[brand][fk]['transactions'] = 0

                int_price = get_int(price)
                if int_price:
                    brands_fk_dict[brand][fk]['prices'].append(int_price)
                    brands_fk_dict[brand][fk]['transactions'] += 1

    # print(brands_fk_dict)
    # print('-'*80)

    conn.select_db(brands_db_name)
    for brand in brands_fk_dict.keys():
        brand_table_name = f'{brand.title().replace(" ", "_").strip()}'

        # for fk in brands_fk_dict[brand].keys():
        #     print(fk)
        #     print(average_sale_price(brands_fk_dict[brand][fk]['prices']))

        # calculate ranking
        transactions_ranking_list = sorted(brands_fk_dict[brand].keys(), key=lambda d: brands_fk_dict[brand][d]['transactions'], reverse=True)
        prices_ranking_list = sorted(brands_fk_dict[brand].keys(), key=lambda d: average_sale_price(brands_fk_dict[brand][d]['prices']), reverse=True)

        try:
            sql = f"INSERT INTO {brand_table_name} (history_days, " \
                  f"most_popular_rank, " \
                  f"least_popular_rank, " \
                  f"highest_price_models, " \
                  f"lowest_price_models" \
                  f") " \
                  f"VALUES (%s, %s, %s, %s, %s) " \
                  """ ON DUPLICATE KEY UPDATE 
                  history_days  = history_days, 
                  most_popular_rank  = VALUES(most_popular_rank), 
                  least_popular_rank  = VALUES(least_popular_rank), 
                  highest_price_models  = VALUES(highest_price_models), 
                  lowest_price_models  = VALUES(lowest_price_models);
                                  """
            val = (table_days,
                   transactions_ranking_list[0],
                   transactions_ranking_list[-1],
                   json.dumps(prices_ranking_list[:5]),
                   json.dumps(prices_ranking_list[-5:]),
                   )
            cursor.execute(sql, val)
        except Exception as e:
            print(f'FAILED TO PARSE BRAND {brand} OF {table_days} DAYS, {e}')


def most_popular(lst):
    return max(set(lst), key=lst.count)


def get_int(item):
    try:
        return int(float(item.replace(',', '').strip()))
    except Exception as error:
        print(f'FAILED TO (int) {item}, {error}')


def average_sale_price(price_list):
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


def createDataBaseIfNotExist():
    try:
        cursor.execute(f"CREATE DATABASE {brands_db_name}")
        print(f'HISTORY DB "{brands_db_name}" CREATED')
    except pymysql.err.ProgrammingError:
        print(f'HISTORY DB "{brands_db_name}" ALREADY EXISTS')


def createBrandTableIfNotExist(brand_table_name):
    try:
        cursor.execute(f"CREATE TABLE {brand_table_name} (`history_days` INTEGER NOT NULL PRIMARY KEY,"
                       f" `most_popular_rank` VARCHAR(255),"
                       f" `least_popular_rank` VARCHAR(255),"
                       f" `highest_price_models` text,"
                       f" `lowest_price_models` text"
                       f");"
                       )
        print(f'BRAND TABLE "{brand_table_name}" CREATED')
    except pymysql.err.OperationalError:
        print(f'BRAND TABLE "{brand_table_name}" ALREADY EXISTS')


if __name__ == '__main__':
    main()
