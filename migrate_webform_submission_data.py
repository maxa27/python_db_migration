#!/usr/bin/env python3
# Команда для запуска с удалением существующей таблицы:
# python migrate_webform_submission_data.py --table webform_submission_data --drop

#!/usr/bin/env python3
import argparse
import sys
from sqlalchemy import create_engine, MetaData, Table, select, text, Text
from sqlalchemy.dialects.mysql import MEDIUMTEXT

def get_primary_key_mysql(engine, table):
    """Определяет имя первичного ключа в MySQL для указанной таблицы."""
    with engine.connect() as conn:
        result = conn.execute(
            text(f"SHOW KEYS FROM {table} WHERE Key_name = 'PRIMARY'")
        ).mappings().all()
        if result:
            return result[0]["Column_name"]
        else:
            return None

def get_primary_key_pg(engine, table):
    """Определяет имя первичного ключа в PostgreSQL для указанной таблицы,
    используя information_schema. Если не найден стандартным способом, проверяет наличие столбца 'sid'."""
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
    """Возвращает множество значений ключевого столбца из таблицы.
    Если возникает ошибка (например, столбец не найден), пытается определить первичный ключ автоматически."""
    try:
        with engine.connect() as conn:
            query = text(f"SELECT {key_column} FROM {table}")
            result = conn.execute(query)
            keys = {row[0] for row in result.mappings().all()}
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
                keys = {row[0] for row in result.mappings().all()}
            return keys
        else:
            print(f"Ошибка при получении ключей из таблицы '{table}' в {db_label} с использованием столбца '{key_column}':\n{e}")
            sys.exit(1)

def fetch_rows_by_key(engine, table, key_column, key_value):
    """Получает из таблицы все строки, удовлетворяющие условию key_column = key_value.
    Возвращает список строк (каждая строка — словарь)."""
    with engine.connect() as conn:
        query = text(f"SELECT * FROM {table} WHERE {key_column} = :key_val")
        results = conn.execute(query, {"key_val": key_value}).mappings().all()
        return [dict(row) for row in results]

def sanitize_value(value):
    """Очищает значение от NUL-символов.
    Если значение является строкой, удаляет символы '\0'. Если это байты, пытается декодировать в UTF-8."""
    if value is None:
        return value
    elif isinstance(value, str):
        return value.replace('\0', '')
    elif isinstance(value, bytes):
        try:
            decoded = value.decode('utf-8', errors='ignore')
            return decoded.replace('\0', '')
        except Exception:
            return value
    else:
        return value

def sanitize_row(row):
    """Применяет sanitize_value ко всем полям строки (словаря)."""
    return {key: sanitize_value(value) for key, value in row.items()}

def insert_row_pg(engine, table, row_dict):
    """Вставляет строку (словарь с данными) в таблицу PostgreSQL.
    Перед вставкой очищает строковые поля от NUL-символов.
    Формирует запрос INSERT, используя имена столбцов из row_dict."""
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
        description="Мигрирует таблицу из MySQL в PostgreSQL: если таблица не существует, создаёт её, а затем копирует все данные."
    )
    parser.add_argument("--mysql", type=str,
                        default="mysql+pymysql://root:root@127.0.0.1:3306/source_db",
                        help="Строка подключения к MySQL (по умолчанию: mysql+pymysql://root:root@127.0.0.1:3306/source_db)")
    parser.add_argument("--postgres", type=str,
                        default="postgresql+psycopg2://postgres@localhost/hexly_proj",
                        help="Строка подключения к PostgreSQL (по умолчанию: postgresql+psycopg2://postgres@localhost/hexly_proj)")
    parser.add_argument("--table", type=str,
                        default="webform_submission_data",
                        help="Название таблицы для миграции (по умолчанию: webform_submission_data)")
    parser.add_argument("--drop", action="store_true",
                        help="Если указано, удаляет таблицу в PostgreSQL (если существует) перед созданием новой.")
    args = parser.parse_args()

    # Создаем движки подключения
    mysql_engine = create_engine(args.mysql)
    pg_engine = create_engine(args.postgres)

    # Создаем объект MetaData для отражения схемы из MySQL
    metadata = MetaData()

    print(f"Отражаем схему таблицы '{args.table}' из MySQL...")
    try:
        mysql_table = Table(args.table, metadata, autoload_with=mysql_engine)
    except Exception as e:
        print(f"Ошибка при отражении таблицы '{args.table}' из MySQL: {e}")
        return

    # Преобразуем неподдерживаемые типы, например, MEDIUMTEXT → Text
    for col in mysql_table.columns:
        if isinstance(col.type, MEDIUMTEXT):
            print(f"Преобразуем тип столбца '{col.name}' с MEDIUMTEXT на Text().")
            col.type = Text()

    # Если указан флаг --drop, удаляем таблицу в PostgreSQL, если она существует
    if args.drop:
        print(f"Удаляем таблицу '{args.table}' в PostgreSQL, если она существует...")
        try:
            pg_metadata = MetaData()
            table_to_drop = Table(args.table, pg_metadata, autoload_with=pg_engine)
            table_to_drop.drop(pg_engine, checkfirst=True)
        except Exception as e:
            print(f"Ошибка при удалении таблицы в PostgreSQL: {e}")

    print(f"Создаем таблицу '{args.table}' в PostgreSQL...")
    try:
        mysql_table.create(pg_engine)
    except Exception as e:
        print(f"Ошибка при создании таблицы '{args.table}' в PostgreSQL: {e}")
        return

    print("Извлекаем данные из MySQL...")
    with mysql_engine.connect() as conn:
        result = conn.execute(select(mysql_table))
        # Используем mappings() для получения результатов в виде словарей
        rows = result.mappings().all()
    print(f"Извлечено {len(rows)} строк.")

    if not rows:
        print("Данных для миграции не найдено.")
        return

    data = list(rows)  # Уже список словарей

    # Очищаем все строки от NUL-символов
    sanitized_data = [sanitize_row(row) for row in data]

    print("Вставляем данные в PostgreSQL...")
    with pg_engine.begin() as conn:
        try:
            conn.execute(mysql_table.insert(), sanitized_data)
        except Exception as e:
            print(f"Ошибка при вставке данных в PostgreSQL: {e}")
            return

    print("Миграция завершена успешно.")

if __name__ == "__main__":
    main()
