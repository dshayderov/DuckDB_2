#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import duckdb
import typing as t
from pathlib import Path


def display_planes(staff: t.List[t.Dict[str, t.Any]]) -> None:
    """
    Отобразить список самолетов.
    """
    # Проверить, что список самолетов не пуст.
    if staff:
        # Заголовок таблицы.
        line = '+-{}-+-{}-+-{}-+-{}-+'.format(
            '-' * 4,
            '-' * 30,
            '-' * 20,
            '-' * 15
        )
        print(line)
        print(
            '| {:^4} | {:^30} | {:^20} | {:^15} |'.format(
                "No",
                "Пункт назначения",
                "Номер рейса",
                "Тип самолета"
            )
        )
        print(line)

        # Вывести данные о всех самолетах.
        for idx, plane in enumerate(staff, 1):
            print(
                '| {:>4} | {:<30} | {:<20} | {:>15} |'.format(
                    idx,
                    plane.get('destination', ''),
                    plane.get('num', 0),
                    plane.get('typ', '')
                )
            )
            print(line)

    else:
        print("Список самолетов пуст.")


def create_db(database_path: Path) -> None:
    """
    Создать базу данных.
    """
    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()

    # Создать таблицу с информацией о типах.
    cursor.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS type_st START 1
        """
    )
    cursor.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS plane_st START 1
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS types (
            type_id INTEGER PRIMARY KEY,
            type_title TEXT NOT NULL
        )
        """
    )

    # Создать таблицу с информацией о самолетах.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS planes (
            plane_id INTEGER PRIMARY KEY,
            plane_destination TEXT NOT NULL,
            type_id INTEGER NOT NULL,
            plane_num INTEGER NOT NULL,
            FOREIGN KEY(type_id) REFERENCES types(type_id)
        )
        """
    )

    conn.close()


def add_plane(
    database_path: Path,
    destination: str,
    typ: str,
    num: int
) -> None:
    """
    Добавить самолет в базу данных.
    """
    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()
    # Получить идентификатор должности в базе данных.
    # Если такой записи нет, то добавить информацию о новом типе.
    cursor.execute(
        """
        SELECT type_id FROM types WHERE type_title = ?
        """,
        (typ,)
    )
    row = cursor.fetchone()
    if row is None:
        cursor.execute(
            """
            INSERT INTO types VALUES (nextval('type_st'), ?)
            """,
            (typ,)
        )
        cursor.execute(
            """
            SELECT currval('type_st')
            """
        )
        sel = cursor.fetchone()
        type_id = sel[0]

    else:
        type_id = row[0]

    # Добавить информацию о новом самолете.
    cursor.execute(
        """
        INSERT INTO planes (plane_id, plane_destination, type_id, plane_num)
        VALUES (nextval('plane_st'), ?, ?, ?)
        """,
        (destination, type_id, num)
    )

    conn.commit()
    conn.close()


def select_all(database_path: Path) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать все самолеты.
    """
    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT planes.plane_destination, types.type_title, planes.plane_num
        FROM planes
        INNER JOIN types ON types.type_id = planes.type_id
        """
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {
        "destination": row[0],
        "typ": row[1],
        "num": row[2],
        }
        for row in rows
    ]


def select_by_type(
    database_path: Path, jet: str
) -> t.List[t.Dict[str, t.Any]]:
    """
    Выбрать самолеты с заданным типом.
    """

    conn = duckdb.connect(str(database_path))
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT planes.plane_destination, types.type_title, planes.plane_num
        FROM planes
        INNER JOIN types ON types.type_id = planes.type_id
        WHERE types.type_title = ?
        """,
        (jet,)
    )
    rows = cursor.fetchall()

    conn.close()
    return [
        {
            "destination": row[0],
            "typ": row[1],
            "num": row[2],
        }
        for row in rows
    ]


def main(command_line=None):
    # Создать родительский парсер для определения имени файла.
    file_parser = argparse.ArgumentParser(add_help=False)
    file_parser.add_argument(
        "--db",
        action="store",
        required=False,
        default=str(Path.cwd() / "planes.db"),
        help="The database file name"
    )

    # Создать основной парсер командной строки.
    parser = argparse.ArgumentParser("planes")
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 0.1.0"
    )
    subparsers = parser.add_subparsers(dest="command")

    # Создать субпарсер для добавления самолета.
    add = subparsers.add_parser(
        "add",
        parents=[file_parser],
        help="Add a new plane"
    )
    add.add_argument(
        "-d",
        "--destination",
        action="store",
        required=True,
        help="The plane's destination"
    )
    add.add_argument(
        "-n",
        "--num",
        action="store",
        type=int,
        required=True,
        help="The plane's numer"
    )
    add.add_argument(
        "-t",
        "--typ",
        action="store",
        required=True,
        help="The plane's type"
    )

    # Создать субпарсер для отображения всех самолетов.
    _ = subparsers.add_parser(
        "display",
        parents=[file_parser],
        help="Display all planes"
    )

    # Создать субпарсер для выбора самолетов.
    select = subparsers.add_parser(
        "select",
        parents=[file_parser],
        help="Select the planes"
    )
    select.add_argument(
        "-T",
        "--type",
        action="store",
        required=True,
        help="The required type"
    )

    # Выполнить разбор аргументов командной строки.
    args = parser.parse_args(command_line)

    # Получить путь к файлу базы данных.
    db_path = Path(args.db)
    create_db(db_path)

    # Добавить самолет.
    if args.command == "add":
        add_plane(db_path, args.destination, args.typ, args.num)

    # Отобразить все самолеты.
    elif args.command == "display":
        display_planes(select_all(db_path))

    # Выбрать требуемые самолеты.
    elif args.command == "select":
        display_planes(select_by_type(db_path, args.type))
    pass


if __name__ == "__main__":
    main()