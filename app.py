import psycopg2
import time
import random
from psycopg2 import sql

# Параметры подключения к БД
host = "pg-32b22183-py-api-test-db.c.aivencloud.com"
port = 12811
dbname = "defaultdb"
user = "avnadmin"
password = "AVNS_h1CF9J3jlqE_axHiUEf"

def get_db_connection():
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password
    )
    return conn

def create_table():
    conn = get_db_connection()
    cursor = conn.cursor()

    # Створення таблиці
    create_table_query = '''
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        age INT
    );
    '''
    cursor.execute(create_table_query)

    # Додавання індексів
    create_index_name_query = 'CREATE INDEX IF NOT EXISTS idx_users_name ON users (name);'
    create_index_age_query = 'CREATE INDEX IF NOT EXISTS idx_users_age ON users (age);'
    cursor.execute(create_index_name_query)
    cursor.execute(create_index_age_query)

    conn.commit()
    cursor.close()
    conn.close()

# Функции для замеров времени выполнения запросов
def measure_insert(n):
    conn = get_db_connection()
    cursor = conn.cursor()

    start_time = time.time()
    for i in range(n):
        name = f'User{i}'
        age = random.randint(18, 80)
        cursor.execute('INSERT INTO users (name, age) VALUES (%s, %s)', (name, age))

    conn.commit()
    end_time = time.time()

    cursor.close()
    conn.close()

    return end_time - start_time

def measure_select(n):
    conn = get_db_connection()
    cursor = conn.cursor()

    start_time = time.time()
    cursor.execute('SELECT * FROM users LIMIT %s', (n,))
    cursor.fetchall()
    end_time = time.time()

    cursor.close()
    conn.close()

    return end_time - start_time

def measure_update(n):
    conn = get_db_connection()
    cursor = conn.cursor()

    start_time = time.time()
    for i in range(n):
        name = f'UpdatedUser{i}'
        age = random.randint(18, 80)
        cursor.execute('UPDATE users SET name = %s, age = %s WHERE id = %s', (name, age, i + 1))

    conn.commit()
    end_time = time.time()

    cursor.close()
    conn.close()

    return end_time - start_time

def measure_delete(n):
    conn = get_db_connection()
    cursor = conn.cursor()

    start_time = time.time()
    for i in range(n):
        cursor.execute('DELETE FROM users WHERE id = %s', (i + 1,))

    conn.commit()
    end_time = time.time()

    cursor.close()
    conn.close()

    return end_time - start_time

def perform_tests():
    test_sizes = [10,100,1000]

    results = []

    for size in test_sizes:
        print(f"Running tests for {size} records...")

        insert_time = measure_insert(size)
        select_time = measure_select(size)
        update_time = measure_update(size)
        delete_time = measure_delete(size)

        results.append({
            "size": size,
            "insert_time": insert_time,
            "select_time": select_time,
            "update_time": update_time,
            "delete_time": delete_time
        })

    return results

# Вывод результатов в виде таблицы
def print_results(results):
    print(f"{'Size':<10}{'Insert Time':<20}{'Select Time':<20}{'Update Time':<20}{'Delete Time':<20}")
    print("="*90)

    for result in results:
        print(f"{result['size']:<10}{result['insert_time']:<20.4f}{result['select_time']:<20.4f}{result['update_time']:<20.4f}{result['delete_time']:<20.4f}")

if __name__ == "__main__":
    create_table()
    results = perform_tests()
    print_results(results)

    Кількість записів	INSERT (середній час, с)	SELECT (середній час, с)	UPDATE (середній час, с)	DELETE (середній час, с)
    1,000	            2.0483	                    2.1694	                    4.5000	                    4.4610
    10,000	            20.413	                    21.681	                    45.008	                    44.556
    100,000	            204.430	                    216.810	                    449.980	                    446.050
    1,000,000	        2015.0436	                2030.1646	                4040.5016	                4000.4528