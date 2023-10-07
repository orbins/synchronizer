import logging
import os
from pathlib import Path
import requests
import sqlite3
import time

logger = logging.getLogger(__name__)


def main():
    dir_path = 'C:/Users/Danil/Desktop/Foundation'
    current_modified = time.ctime(
        max(os.path.getmtime(root) for root, _, _ in os.walk(dir_path))
    )
    db_file = Path(__file__).cwd() / 'db.sqlite'

    if not db_file.exists():
        connection = sqlite3.connect('db.sqlite')
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS modifies 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, dir_path TEXT, updated_date TEXT)
            """
        )
        connection.commit()
        cursor.execute(
            """INSERT INTO modifies VALUES (dir_path = ?, updated_date = ?)""",
            (dir_path, current_modified)
        ).fetchone()
        connection.commit()
        connection.close()

    connection = sqlite3.connect('db.sqlite')
    cursor = connection.cursor()
    last_modified = cursor.execute(
        """SELECT updated_date FROM modifies WHERE dir_path = ?""",
        (dir_path, )
    ).fetchone()
    connection.close()

    if last_modified != current_modified:
        print(f"current updated date: {current_modified}")
        print(f"date from db {last_modified}")
        connection = sqlite3.connect('db.sqlite')
        cursor = connection.cursor()
        cursor.execute(
            f"""UPDATE modifies SET updated_date = ? WHERE dir_path = ?""",
            (dir_path, current_modified)
        )
        connection.commit()
        connection.close()
        params = {
            "client_id": "d5598843-162e-4ce7-adad-e228224194cb",
            "scope": "files.readwrite",
            "response_type": "token",
        }
        response = requests.get(
            f'https://login.microsoftonline.com/common/oauth2/v2.0/authorize',
            params=params
        )
        ...


main()


