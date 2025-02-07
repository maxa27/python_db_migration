#!/usr/bin/env python3
# Команда для запуска с удалением существующей таблицы:
# python migrate_db.py --table <название_таблицы> --drop

import pandas as pd
from sqlalchemy import create_engine, inspect

mysql_conn_str = "mysql+pymysql://root:root@127.0.0.1:3306/source_db"
postgres_conn_str = "postgresql+psycopg2://postgres@localhost/hexly_proj"

mysql_engine = create_engine(mysql_conn_str)
pg_engine = create_engine(postgres_conn_str)

inspector = inspect(mysql_engine)
tables = inspector.get_table_names()

print("Найденные таблицы в MySQL:", tables)

chunk_size = 10000  # можно настроить под размер таблицы

for table in tables:
    print(f"Перенос таблицы: {table}")
    try:
        # Создаем пустую таблицу в PostgreSQL (заменяя, если существует)
        # Читаем первый чанк, чтобы определить схему и создать таблицу
        first_chunk = pd.read_sql_query(f"SELECT * FROM {table} LIMIT {chunk_size}", mysql_engine)
        first_chunk.to_sql(table, pg_engine, if_exists='replace', index=False)
        print(f"Таблица {table} создана, перенос данных порциями:")

        # Читаем таблицу порциями и добавляем в PostgreSQL
        offset = chunk_size
        while True:
            chunk = pd.read_sql_query(f"SELECT * FROM {table} LIMIT {chunk_size} OFFSET {offset}", mysql_engine)
            if chunk.empty:
                break
            chunk.to_sql(table, pg_engine, if_exists='append', index=False)
            offset += chunk_size
            print(f"Перенесено строк: {offset}")
        print(f"Таблица {table} успешно перенесена.")
    except Exception as e:
        print(f"Ошибка при переносе таблицы {table}: {e}")
