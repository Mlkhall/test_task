import psycopg2
from config import host, user, password, db_name

connection = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    database=db_name
)

try:
    with connection.cursor() as cursor:
        condition = \
            """
a.	Вывод количества всех транзакций, сгруппированных по месяцам, совершенных пользователями,
    которые зарегистрировались в тот же месяц, что и осуществили транзакцию. (Т.е. За июль 2019 - это пользователи,
    зарегистрированные в июле 2019, за август 2019 - в августе 2019 и т.д.)
            """

        sql_request = \
            """
            SELECT 
                TO_CHAR(r.registered_at, 'YYYY-MM') AS ym_reg,
                COUNT(l.id) AS number_of_tran
    
            FROM users AS r FULL JOIN orders AS l 
                ON l.user_id = r.id
            WHERE 
                (TO_CHAR(l.finished_at, 'YYYY-MM') = TO_CHAR(r.registered_at, 'YYYY-MM')) AND (r.registered_at <= l.finished_at)
            GROUP BY ym_reg
            ORDER BY ym_reg DESC
            """

        cursor.execute(sql_request)
        print(condition)
        print('Answer:', sql_request)
        print("\nResult:")
        [print('\t\t', row) for row in cursor]
        print('\n')

    with connection.cursor() as cursor:
        condition = \
            """
b.	Вывод количества пользователей не из России, зарегистрировавшихся в 2019 году,
 доход (ввод минус вывод) с каждого из которых за все время составил больше 1000$
            """
        sql_request = \
            """
            SELECT 
                COUNT(res.us) AS number_of_users
            FROM
            (
                SELECT
                    l.user_id AS us,
                    COUNT(l.user_id) AS count_trans,
                    SUM(l.amount_usd) AS total_income
                FROM users AS r FULL JOIN orders AS l 
                    ON l.user_id = r.id
                    FULL JOIN countries AS d
                    ON r.country_id = d.id
            
                WHERE d.full_name != 'Российская Федерация' AND TO_CHAR(r.registered_at, 'YYYY') = '2019'
                GROUP BY us
                HAVING sum(l.amount_usd) > 1000
            ) AS res
            """

        cursor.execute(sql_request)
        print(condition)
        print('Answer:', sql_request)
        print("\nResult:")
        [print('\t\t', row) for row in cursor]
        print('\n')

    with connection.cursor() as cursor:
        condition = \
            """
c.	Вывод, в котором бы каждому месяцу из orders соответствовала бы каждая страна из countries
    (month x - iso A; month x - iso B; month y = iso A; month y - iso B …)
            """
        sql_request = \
            """
            SELECT 
                TO_CHAR(l.finished_at, 'Month YYYY') AS month_of_trans,
                d.iso
            FROM orders AS l FULL JOIN users AS r
                ON l.user_id = r.id
            FULL JOIN countries AS d
                ON r.country_id = d.id
            """

        cursor.execute(sql_request)
        print(condition)
        print('Answer:', sql_request)
        print("\nResult:")
        [print('\t\t', row) for row in cursor.fetchmany(10)]
        print('\n')

    with connection.cursor() as cursor:
        condition = \
            """
d.	Вывод id пяти пользователей, имеющих наибольшую сумму депозитов (положительных транзакций) за все время
            """
        sql_request = \
            """
            SELECT 
                user_id
            FROM orders
            WHERE amount_usd > 0 
            GROUP BY user_id
            ORDER BY SUM(amount_usd) DESC
            LIMIT 5
            """

        cursor.execute(sql_request)
        print(condition)
        print('Answer:', sql_request)
        print("\nResult:")
        [print('\t\t', row) for row in cursor]
        print('\n')

    with connection.cursor() as cursor:
        condition = \
            """
e.	Вывод id пользователей, доход (пополнения минус выводы) с которых за май 2021 составил более 5% общего дохода со страны
            """
        # Должен быть 2021 год, но по нему я не генерировал данные, так что взял именной 2020. Но чтобы задание
        # было выполненым, я сделал его опциональным.
        year = 2020
        sql_request = \
            f"""
            SELECT 
                user_id
            FROM
                (SELECT
                    *,
                    (SUM(income_of_user) OVER (PARTITION BY name_country)) * 0.05 AS income_of_country_5_per
                FROM
                    (SELECT
                        d.full_name AS name_country,
                        l.user_id,
                        SUM(l.amount_usd) AS income_of_user
                    FROM orders AS l FULL JOIN users AS r
                        ON l.user_id = r.id
                    FULL JOIN countries AS d
                        ON r.country_id = d.id
                    WHERE TO_CHAR(l.finished_at, 'YYYY-MM') = '{year}-05'
            
                    GROUP BY name_country, l.user_id
                    ) AS tab) AS final_tab
            WHERE income_of_user > income_of_country_5_per
            LIMIT 10
            """

        cursor.execute(sql_request)
        print(condition)
        print('Answer:', sql_request)
        print("\nResult:")
        [print('\t\t', row) for row in cursor]
        print('\n')

    with connection.cursor() as cursor:
        condition = \
            """
f.	Вывод id пользователей, у которых каждое следующее пополнение счета было выше предыдущего
            """
        sql_request = \
            """
            SELECT 
                user_id
            FROM
                (SELECT
                *,
                amount_usd > lag_row rising
                FROM
                    (SELECT
                    *
                    FROM
                        (SELECT
                            *,
                            LAG(amount_usd, 1) OVER w lag_row
                        FROM
                            (
                            SELECT 
                                user_id,
                                finished_at,
                                amount_usd
                            FROM orders
                            WHERE amount_usd > 0
                            ORDER BY user_id, finished_at
                            ) AS sq1
                        WINDOW w AS (PARTITION BY user_id)) AS sq2
                        WHERE lag_row IS NOT NULL) AS sq3) AS sq4
            GROUP BY user_id
            HAVING bool_and(rising) = true
            LIMIT 10
            """
        cursor.execute(sql_request)
        print(condition)
        print('Answer:', sql_request)
        print("\nResult:")
        [print('\t\t', row) for row in cursor]
        print('\n')

except Exception as _ex:
    print("[INFO] Error while working with PostgresSQL", _ex)
finally:
    if connection:
        connection.close()
        print("[INFO] PostgreSQL connection closed")
