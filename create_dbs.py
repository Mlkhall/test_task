import psycopg2
import requests
from random import randint, uniform
from gen_func import random_date, get_all_records_from_table
from config import host, user, password, db_name

ADD_COUNTRIES, ADD_USERS, ADD_ORDERS = False, False, False

connection = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    database=db_name
)

try:
    with connection.cursor() as cursor:
        cursor.execute(
            """SELECT version();"""
        )

        print(f"Server version: {cursor.fetchone()}")

    if ADD_COUNTRIES:
        with connection.cursor() as cursor:
            answer = requests.get('https://namaztimes.kz/ru/api/country?type=json').json()
            countries = tuple(map(lambda el: (int(el[0]), el[1], el[1][:2].upper()), tuple(answer.items())))
            sql_insert_query = \
                """
                INSERT INTO countries (id, full_name, iso) VALUES
                (%s, %s, %s);
                """
            cursor.executemany(sql_insert_query, countries)
            connection.commit()

    if ADD_USERS:
        number_of_records = 10000
        date_start, date_end = '2019-1-1 12:12:00', '2021-1-2 12:12:00'
        word_site = f"https://www.mit.edu/~ecprice/wordlist.{number_of_records}"

        words = requests.get(word_site).content.splitlines()
        dates = [random_date(date_start, date_end) for _ in range(number_of_records)]
        id_countries = tuple(requests.get('https://namaztimes.kz/ru/api/country?type=json').json().keys())

        users = [(i,
                  dates[i],
                  id_countries[randint(1, len(id_countries) - 1)],
                  words[randint(0, number_of_records - 1)].decode())
                 for i in range(number_of_records)]

        with connection.cursor() as cursor:
            sql_insert_query = \
                """
                INSERT INTO users (id, registered_at, country_id, campaig) VALUES
                (%s, %s, %s, %s);
                """
            cursor.executemany(sql_insert_query, users)
            connection.commit()

    get_all_records_from_table(connection=connection, table_name='users')

    if ADD_ORDERS:
        number_of_records = 100000
        date_start, date_end = '2017-9-10 12:12:00', '2021-1-2 12:02:00'

        finished_at = [random_date(date_start, date_end) for _ in range(number_of_records)]
        amount_usd = [uniform(-100, 100) for _ in range(number_of_records)]

        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(id) FROM users;
                """
            )

            length_users = cursor.fetchone()[0]
            print(f"Count users of recoders {length_users}")

        id_users = tuple(range(length_users))
        orders = [(i, id_users[randint(0, len(id_users) - 1)], amount_usd[i], finished_at[i])
                  for i in range(number_of_records)]

        with connection.cursor() as cursor:
            sql_insert_query = \
                """
                INSERT INTO orders (id, user_id, amount_usd, finished_at) VALUES
                (%s, %s, %s, %s);
                """
            cursor.executemany(sql_insert_query, orders)
            connection.commit()

    get_all_records_from_table(connection=connection, table_name='orders')

except Exception as _ex:
    print("[INFO] Error while working with PostgresSQL", _ex)
finally:
    if connection:
        connection.close()
        print("[INFO] PostgreSQL connection closed")
