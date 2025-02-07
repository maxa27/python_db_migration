#!/usr/bin/env python3
import argparse
from sqlalchemy import create_engine, text
try:
    from tabulate import tabulate
except ImportError:
    tabulate = None

def get_mysql_table_counts(mysql_conn_str):
    """Подключается к MySQL и возвращает словарь {table_name: row_count} для всех таблиц."""
    engine = create_engine(mysql_conn_str)
    counts = {}
    with engine.connect() as conn:
        # Получаем список таблиц
        result = conn.execute(text("SHOW TABLES"))
        tables = [row[0] for row in result]
        for table in tables:
            try:
                count = conn.execute(text(f"SELECT COUNT(*) FROM `{table}`")).scalar()
                counts[table] = count
            except Exception as e:
                counts[table] = f"Error: {e}"
    return counts

def get_postgres_table_counts(pg_conn_str):
    """Подключается к PostgreSQL и возвращает словарь {table_name: row_count} для таблиц в схеме public."""
    engine = create_engine(pg_conn_str)
    counts = {}
    with engine.connect() as conn:
        # Получаем список таблиц в схеме public
        result = conn.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))
        tables = [row[0] for row in result]
        for table in tables:
            try:
                count = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar()
                counts[table] = count
            except Exception as e:
                counts[table] = f"Error: {e}"
    return counts

def main():
    parser = argparse.ArgumentParser(
        description="Сравнивает количество строк в таблицах базы данных MySQL и PostgreSQL."
    )
    parser.add_argument(
        "--mysql",
        default="mysql+pymysql://root:root@127.0.0.1:3306/source_db",
        help="Строка подключения к MySQL (по умолчанию: mysql+pymysql://root:root@127.0.0.1:3306/source_db)"
    )
    parser.add_argument(
        "--postgres",
        default="postgresql+psycopg2://postgres@localhost/hexly_proj",
        help="Строка подключения к PostgreSQL (по умолчанию: postgresql+psycopg2://postgres@localhost/hexly_proj)"
    )
    args = parser.parse_args()

    print("Получаем данные из MySQL...")
    mysql_counts = get_mysql_table_counts(args.mysql)
    print("Получаем данные из PostgreSQL...")
    pg_counts = get_postgres_table_counts(args.postgres)

    # Объединяем наборы таблиц (если таблица есть в одной БД, а в другой — нет, выводим N/A)
    all_tables = set(mysql_counts.keys()) | set(pg_counts.keys())
    output_data = []
    for table in sorted(all_tables):
        mysql_val = mysql_counts.get(table, "N/A")
        pg_val = pg_counts.get(table, "N/A")
        output_data.append([table, mysql_val, pg_val])

    headers = ["Таблица", "MySQL rows", "PostgreSQL rows"]
    if tabulate:
        print(tabulate(output_data, headers=headers, tablefmt="psql"))
    else:
        # Если tabulate не установлен, выводим простой текстовый вывод
        print("{:<20} {:<15} {:<15}".format(*headers))
        for row in output_data:
            print("{:<20} {:<15} {:<15}".format(*row))

if __name__ == "__main__":
    main()
