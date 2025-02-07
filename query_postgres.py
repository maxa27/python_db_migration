#!/usr/bin/env python3
import argparse
import psycopg2
from psycopg2.extras import RealDictCursor

# Если библиотека tabulate установлена, будем её использовать для форматированного вывода.
try:
    from tabulate import tabulate
except ImportError:
    tabulate = None

def main():
    parser = argparse.ArgumentParser(
        description="Выполняет SQL-запрос к PostgreSQL и выводит результат в отформатированном виде."
    )
    parser.add_argument(
        '--query',
        type=str,
        help="SQL-запрос для выполнения (например: \"SELECT COUNT(*) FROM users\")"
    )
    parser.add_argument(
        '--host',
        type=str,
        default='localhost',
        help="Хост PostgreSQL (по умолчанию: localhost)"
    )
    parser.add_argument(
        '--port',
        type=int,
        default=5432,
        help="Порт PostgreSQL (по умолчанию: 5432)"
    )
    parser.add_argument(
        '--dbname',
        type=str,
        default='hexly_proj',
        help="Имя базы данных (по умолчанию: hexly_proj)"
    )
    parser.add_argument(
        '--user',
        type=str,
        default='postgres',
        help="Имя пользователя (по умолчанию: postgres)"
    )
    parser.add_argument(
        '--password',
        type=str,
        default='',
        help="Пароль (по умолчанию пустой)"
    )

    args = parser.parse_args()

    # Если параметр --query не передан, запрашиваем у пользователя название таблицы
    if not args.query:
        table = input("Введите название таблицы для подсчета строк: ").strip()
        if not table:
            print("Название таблицы не введено. Завершаем работу.")
            return
        # Формируем запрос, который возвращает количество строк
        args.query = f"SELECT COUNT(*) AS count FROM {table}"

    try:
        # Подключаемся к PostgreSQL
        conn = psycopg2.connect(
            host=args.host,
            port=args.port,
            dbname=args.dbname,
            user=args.user,
            password=args.password
        )
    except Exception as e:
        print("Ошибка подключения к базе данных:", e)
        return

    try:
        # Используем курсор, возвращающий словари (ключи – имена колонок)
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(args.query)
        rows = cur.fetchall()

        if not rows:
            print("Запрос выполнен, но результатов не найдено.")
        else:
            if tabulate:
                # Выводим результат с помощью tabulate
                print(tabulate(rows, headers="keys", tablefmt="psql"))
            else:
                # Если tabulate не установлен, выводим простой текстовый вывод
                print("Результат запроса:")
                for row in rows:
                    print(row)
        cur.close()
    except Exception as e:
        print("Ошибка при выполнении запроса:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
