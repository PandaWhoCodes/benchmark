import sqlite3
import time
import random
import string
import timeit
from functools import wraps
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import threading
import os

database_file = os.path.realpath('database.db')

create_statement = 'CREATE TABLE IF NOT EXISTS database_threading_test (symbol TEXT, ts INTEGER, o REAL, h REAL, l REAL, c REAL, vf REAL, vt REAL, PRIMARY KEY(symbol, ts))'
insert_statement = 'INSERT INTO database_threading_test VALUES(?,?,?,?,?,?,?,?)'
select_statement = 'SELECT * from database_threading_test'

create_statement2 = 'CREATE TABLE IF NOT EXISTS database_threading_test2 (symbol TEXT, ts INTEGER, o REAL, h REAL, l REAL, c REAL, vf REAL, vt REAL, PRIMARY KEY(symbol, ts))'
insert_statement2 = 'INSERT INTO database_threading_test2 VALUES(?,?,?,?,?,?,?,?)'
select_statement2 = 'SELECT * from database_threading_test2'


def time_stuff(some_function):
    def wrapper(*args, **kwargs):
        t0 = timeit.default_timer()
        value = some_function(*args, **kwargs)
        print(timeit.default_timer() - t0, 'seconds')
        return value

    return wrapper


def generate_values(count=100):
    end = int(time.time()) - int(time.time()) % 900
    symbol = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
    ts = list(range(end - count * 900, end, 900))
    for i in range(count):
        yield (
            symbol, ts[i], random.random() * 1000, random.random() * 1000, random.random() * 1000,
            random.random() * 1000,
            random.random() * 1e9, random.random() * 1e5)


def generate_values_list(symbols=1000, count=100):
    values = []
    for _ in range(symbols):
        values.extend(generate_values(count))
    return values


@time_stuff
def sequential_read():
    """
    Read rows one after the other from a single thread
    100k records in the database, 1000 symbols, 100 rows
    """
    conn = sqlite3.connect(os.path.realpath('database.db'))
    try:
        with conn:
            conn.execute(create_statement)
            results = conn.execute(select_statement).fetchall()
            print(len(results))
    except sqlite3.OperationalError as e:
        print(e)


@time_stuff
def sequential_write():
    """
    Insert rows one after the other from a single thread
    1000 symbols, 100 rows
    """
    l = generate_values_list()
    conn = sqlite3.connect(os.path.realpath('database.db'))
    try:
        with conn:
            conn.execute(create_statement)
            conn.executemany(insert_statement, l)

    except sqlite3.OperationalError as e:
        print(e)


def read_task(symbol):
    """
    Task to read all rows of a given symbol from different threads
    """
    results = []
    conn = sqlite3.connect(os.path.realpath('database.db'))
    try:
        with conn:
            results = conn.execute("SELECT * FROM database_threading_test WHERE symbol=?", symbol).fetchall()
    except sqlite3.OperationalError as e:
        print(e)
    finally:
        return results


@time_stuff
def threaded_read():
    """
    Get all the symbols from the database
    Assign chunks of 50 symbols to each thread worker and let them read all rows for the given symbol
    1000 symbols, 100 rows per symbol
    """
    conn = sqlite3.connect(os.path.realpath('database.db'))
    symbols = conn.execute("SELECT DISTINCT SYMBOL from database_threading_test").fetchall()
    with ThreadPoolExecutor(max_workers=8) as e:
        results = e.map(read_task, symbols, chunksize=50)
        for result in results:
            pass


@time_stuff
def multiprocessed_read():
    """
    Get all the symbols from the database
    Assign chunks of 50 symbols to each process worker and let them read all rows for the given symbol
    1000 symbols, 100 rows
    """
    conn = sqlite3.connect(os.path.realpath('database.db'))
    symbols = conn.execute("SELECT DISTINCT SYMBOL from database_threading_test").fetchall()
    with ProcessPoolExecutor(max_workers=8) as e:
        results = e.map(read_task, symbols, chunksize=50)
        for result in results:
            pass


def write_task(n):
    """
    Insert rows for a given symbol in the database from multiple threads
    We ignore the database locked errors here. Ideal case would be to retry but there is no point writing code for that if it takes longer than a sequential write even without database locke errors
    """
    conn = sqlite3.connect('database.db')
    data = list(generate_values())
    try:
        with conn:
            conn.executemany(insert_statement, data)
    except sqlite3.OperationalError as e:
        pass
    finally:
        conn.close()
        return len(data)


@time_stuff
def threaded_write():
    """
    Insert 100 rows per symbol in parallel using multiple threads
    Prone to database locked errors so all rows may not be written
    Takes 20x the amount of time as a normal write
    1000 symbols, 100 rows
    """
    symbols = [i for i in range(1000)]
    with ThreadPoolExecutor(max_workers=8) as e:
        results = e.map(write_task, symbols, chunksize=50)
        for result in results:
            pass


@time_stuff
def multiprocessed_write():
    """
    Insert 100 rows per symbol in parallel using multiple processes
    1000 symbols, 100 rows
    """
    symbols = [i for i in range(1000)]
    with ProcessPoolExecutor(max_workers=8) as e:
        results = e.map(write_task, symbols, chunksize=50)
        for result in results:
            pass


@time_stuff
def sequential_multidatabase_read():
    """
    Read 100 rows per symbol, 1000 symbols from 2 tables one after the other
    2 tables
    1000 symbols 100 rows
    1000 symbols 100 rows
    Read them one after the other
    """
    conn = sqlite3.connect(os.path.realpath('database.db'))
    try:
        with conn:
            conn.execute(create_statement)
            conn.execute(create_statement2)
            results = conn.execute(select_statement).fetchall()
            results2 = conn.execute(select_statement2).fetchall()
    except sqlite3.OperationalError as e:
        # print(e)
        pass


@time_stuff
def sequential_multidatabase_write():
    """
    Insert 100 rows per symbol, 1000 symbols into 2 tables one after the other
    2 tables
    1000 symbols 100 rows
    1000 symbols 100 rows
    Write them one after the other
    """
    l = generate_values_list()
    l2 = generate_values_list()
    conn = sqlite3.connect(os.path.realpath('database.db'))
    try:
        with conn:
            conn.execute(create_statement)
            conn.execute(create_statement2)
            conn.executemany(insert_statement, l)
            conn.executemany(insert_statement2, l2)

    except sqlite3.OperationalError as e:
        print(e)


def multidatabase_read_task(table_name):
    conn = sqlite3.connect(os.path.realpath('database.db'))
    results = conn.execute('SELECT * from ' + table_name).fetchall()
    print(table_name, len(results))


@time_stuff
def threaded_multidatabase_read():
    """
    Instead of dividing on the basis of symbols which was done in threaded_read and threaded_write methods above and avail no benefits, lets try to read tables in parallel
    This method has 2 databases from which we try to read in parallel using threads
    """
    conn = sqlite3.connect(os.path.realpath('database.db'))
    table_names = ['database_threading_test', 'database_threading_test2']
    with ThreadPoolExecutor(max_workers=8) as e:
        results = e.map(multidatabase_read_task, table_names)
        for result in results:
            pass


@time_stuff
def multiprocessed_multidatabase_read():
    """
    """
    conn = sqlite3.connect(os.path.realpath('database.db'))
    table_names = ['database_threading_test', 'database_threading_test2']
    with ProcessPoolExecutor(max_workers=8) as e:
        results = e.map(multidatabase_read_task, table_names)
        for result in results:
            pass


def multidatabase_write_task(table_name):
    conn = sqlite3.connect(os.path.realpath('database.db'))
    l = generate_values_list()
    with conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS " + table_name + " (symbol TEXT, ts INTEGER, o REAL, h REAL, l REAL, c REAL, vf REAL, vt REAL, PRIMARY KEY(symbol, ts))")
        results = conn.executemany('INSERT INTO ' + table_name + ' VALUES(?,?,?,?,?,?,?,?)', l)
        print(table_name)


@time_stuff
def threaded_multidatabase_write():
    """
    """
    conn = sqlite3.connect(os.path.realpath('database.db'))
    table_names = ['database_threading_test', 'database_threading_test2']
    with ThreadPoolExecutor(max_workers=8) as e:
        results = e.map(multidatabase_write_task, table_names)
        for result in results:
            pass


@time_stuff
def multiprocessed_multidatabase_write():
    """
    """
    conn = sqlite3.connect(os.path.realpath('database.db'))
    table_names = ['database_threading_test', 'database_threading_test2']
    with ProcessPoolExecutor(max_workers=8) as e:
        results = e.map(multidatabase_write_task, table_names)
        for result in results:
            pass


if __name__ == '__main__':
    print("sequential read")
    # sequential read
    sequential_read()
    print("sequential write")
    # sequential write
    sequential_write()
    print("threaded read")
    # threaded read
    threaded_read()
    print("multiprocessed read")
    # multiprocessed read
    multiprocessed_read()
    print("threaded write")
    # threaded write
    threaded_write()
    # multiprocessed write
    print("multiprocessed write")
    multiprocessed_write()
    # sequential multidatabase read
    print("sequential multidatabase read")
    sequential_multidatabase_read()
    # sequential_multidatabase_write
    print("sequential_multidatabase_write")
    sequential_multidatabase_write()
    # threaded multidatabase read
    print("threaded multidatabase read")
    threaded_multidatabase_read()
    # threaded multidatabase write
    print("threaded multidatabase write")
    threaded_multidatabase_write()
    # multiprocessed multidatabase write
    print("multiprocessed multidatabase write")
    multiprocessed_multidatabase_write()
