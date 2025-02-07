#!/usr/bin/env python3
# Команда для запуска с удалением существующей таблицы:
# python sync_webform_submission_data.py --table webform_submission_data --drop

#!/usr/bin/env python3
import argparse
import sys
from sqlalchemy import create_engine, text

def get_primary_key_mysql(engine, table):
    """
    Определяет имя первичного ключа в MySQL для указанной таблицы.
    """
    with engine.connect() as conn:
        result = conn.execute(text(f"SHOW KEYS FROM {table} WHERE Key_name = 'PRIMARY'")).mappings().all()
        if result:
            return result[0]["Column_name"]
        else:
            return None

def get_primary_key_pg(engine, table):
    """
    Определяет имя первичного ключа в PostgreSQL для указанной таблицы,
    используя information_schema. Если не найден стандартным способом, проверяет наличие столбца 'sid'.
    """
    with engine.connect() as conn:
        query = text("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = :table AND tc.constraint_type = 'PRIMARY KEY'
            LIMIT 1
        """)
        result = conn.execute(query, {"table": table}).mappings().all()
        if result:
            return result[0]["column_name"]
        else:
            col_query = text("SELECT column_name FROM information_schema.columns WHERE table_name = :table")
            cols = {row["column_name"] for row in conn.execute(col_query, {"table": table}).mappings().all()}
            if "sid" in cols:
                return "sid"
            else:
                return None

def get_all_keys(engine, table, key_column, db_label):
    """
    Возвращает множество значений ключевого столбца из таблицы.
    Если возникает ошибка (например, столбец не найден), пытается определить первичный ключ автоматически.
    """
    try:
        with engine.connect() as conn:
            query = text(f"SELECT {key_column} FROM {table}")
            result = conn.execute(query)
            keys = {row[0] for row in result}
        return keys
    except Exception as e:
        err_str = str(e)
        if ("Unknown column" in err_str) or ("does not exist" in err_str):
            print(f"Не найден столбец '{key_column}' в таблице '{table}' в {db_label}.")
            print("Пытаемся определить первичный ключ автоматически...")
            if db_label == "MySQL":
                pk = get_primary_key_mysql(engine, table)
            else:
                pk = get_primary_key_pg(engine, table)
            if pk is None:
                print(f"Не удалось обнаружить первичный ключ для таблицы '{table}' в {db_label}.")
                sys.exit(1)
            print(f"Обнаружен первичный ключ: '{pk}'. Повторяем запрос с этим столбцом...")
            with engine.connect() as conn:
                query = text(f"SELECT {pk} FROM {table}")
                result = conn.execute(query)
                keys = {row[0] for row in result}
            return keys
        else:
            print(f"Ошибка при получении ключей из таблицы '{table}' в {db_label} с использованием столбца '{key_column}':\n{e}")
            sys.exit(1)

def fetch_rows_by_key(engine, table, key_column, key_value):
    """
    Получает из таблицы все строки, удовлетворяющие условию key_column = key_value.
    Возвращает список строк (каждая строка — словарь).
    """
    with engine.connect() as conn:
        query = text(f"SELECT * FROM {table} WHERE {key_column} = :key_val")
        results = conn.execute(query, {"key_val": key_value}).mappings().all()
        return [dict(row) for row in results]

def sanitize_row(row):
    """
    Проходит по всем полям строки (словаря) и, если значение является строкой,
    удаляет все NUL (0x00) символы.
    """
    sanitized = {}
    for key, value in row.items():
        if isinstance(value, str):
            sanitized[key] = value.replace('\0', '')
        else:
            sanitized[key] = value
    return sanitized

def insert_row_pg(engine, table, row_dict):
    """
    Вставляет строку (словарь с данными) в таблицу PostgreSQL.
    Перед вставкой очищает строковые поля от NUL-символов.
    Формирует запрос INSERT, используя имена столбцов из row_dict.
    """
    if not row_dict:
        return
    sanitized_row = sanitize_row(row_dict)
    columns = sanitized_row.keys()
    columns_str = ", ".join(columns)
    placeholders = ", ".join(f":{col}" for col in columns)
    insert_stmt = text(f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})")
    with engine.begin() as conn:
        conn.execute(insert_stmt, sanitized_row)

def main():
    parser = argparse.ArgumentParser(
        description="Синхронизирует данные таблицы между MySQL и PostgreSQL, добавляя недостающие записи."
    )
    parser.add_argument(
        "--table",
        type=str,
        default="webform_submission_data",
        help="Название таблицы для синхронизации (по умолчанию: webform_submission_data)"
    )
    parser.add_argument(
        "--key",
        type=str,
        default="id",
        help="Имя ключевого столбца (по умолчанию: id). Если его нет, скрипт определит первичный ключ автоматически."
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

    # Создаём движки подключения
    mysql_engine = create_engine(args.mysql)
    pg_engine = create_engine(args.postgres)

    # Получаем ключи из обеих баз
    mysql_keys = get_all_keys(mysql_engine, args.table, args.key, "MySQL")
    pg_keys = get_all_keys(pg_engine, args.table, args.key, "PostgreSQL")

    print(f"Общее количество строк в MySQL (по ключам): {len(mysql_keys)}")
    print(f"Общее количество строк в PostgreSQL (по ключам): {len(pg_keys)}")

    missing_in_pg = mysql_keys - pg_keys
    print(f"\nНайдено {len(missing_in_pg)} недостающих записей в PostgreSQL.")

    if missing_in_pg:
        # Определяем имя первичного ключа для MySQL
        pk_mysql = args.key
        if not any(str(k) == pk_mysql for k in mysql_keys):
            pk_mysql = get_primary_key_mysql(mysql_engine, args.table)
            if pk_mysql is None:
                pk_mysql = "sid"
        print(f"Используем в MySQL первичный ключ: '{pk_mysql}'")
        count_inserted = 0
        for missing_key in sorted(missing_in_pg):
            rows = fetch_rows_by_key(mysql_engine, args.table, pk_mysql, missing_key)
            if not rows:
                print(f"Не удалось получить данные для ключа {missing_key} из MySQL.")
                continue
            for row in rows:
                try:
                    insert_row_pg(pg_engine, args.table, row)
                    count_inserted += 1
                except Exception as e:
                    print(f"Ошибка при вставке записи с ключом {missing_key} в PostgreSQL: {e}")
        print(f"\nВставлено {count_inserted} недостающих записей в PostgreSQL.")
    else:
        print("Нет недостающих записей для синхронизации.")

if __name__ == "__main__":
    main()
