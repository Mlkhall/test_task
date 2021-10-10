import random
import time


def str_time_prop(start, end, time_format, prop):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formatted in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, time_format))
    etime = time.mktime(time.strptime(end, time_format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(time_format, time.localtime(ptime))


def random_date(start, end):
    return str_time_prop(start, end, '%Y-%m-%d %H:%M:%S', random.random())


def get_all_records_from_table(connection, table_name, limit=100):
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT * FROM {table_name}
            LIMIT {limit};
            """
        )
        print('%' * 100)
        [print(row) for row in cursor]
        print('%' * 100)