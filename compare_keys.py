#!/usr/bin/env python3
import argparse
from sqlalchemy import create_engine, text

def get_row_count(engine, table):
    with engine.connect() as conn:
        query = text(f"SELECT COUNT(*) AS count FROM {table}")
        result = conn.execute(query).mappings().one()
        return result["count"]

def main():
    parser = argparse.ArgumentParser(
        description="Сравнение общего количества строк между таблицами в MySQL и PostgreSQL."
    )
    parser.add_argument(
        "--table",
        type=str,
        default="webform_submission_data",
        help="Название таблицы для сравнения (по умолчанию: webform_submission_data)"
    )
    parser.add_argument(
        "--mysql",
        type=str,
        default="mysql+pymysql://root:root@127.0.0.1:3306/source_db",
        help="Строка подключения к MySQL (по умолчанию: mysql+pymysql://root:root@127.0.0.1:3306/source_db)"
    )
    parser.add_argument(
        "--postgres",
        type=str,
        default="postgresql+psycopg2://postgres@localhost/hexly_proj",
        help="Строка подключения к PostgreSQL (по умолчанию: postgresql+psycopg2://postgres@localhost/hexly_proj)"
    )
    args = parser.parse_args()

    mysql_engine = create_engine(args.mysql)
    pg_engine = create_engine(args.postgres)

    print(f"Получаем общее количество строк в таблице '{args.table}' в MySQL...")
    mysql_count = get_row_count(mysql_engine, args.table)
    print(f"Общее количество строк в MySQL: {mysql_count}")

    print(f"\nПолучаем общее количество строк в таблице '{args.table}' в PostgreSQL...")
    pg_count = get_row_count(pg_engine, args.table)
    print(f"Общее количество строк в PostgreSQL: {pg_count}")

    if mysql_count == pg_count:
        print("\nОбщее количество строк совпадает.")
    else:
        print(f"\nОбщее количество строк различается: в MySQL на {mysql_count - pg_count} строк(и) больше.")

if __name__ == "__main__":
    main()
