import logging
import os
from pathlib import Path
import sqlite3
import time

from dotenv import load_dotenv
import requests

logger = logging.getLogger(__name__)

load_dotenv()

access_token = os.getenv('ACCESS_TOKEN')


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
            """INSERT INTO modifies (dir_path)  VALUES (?)""",
            (dir_path, )
        )
        connection.commit()
        connection.close()

    connection = sqlite3.connect('db.sqlite')
    cursor = connection.cursor()
    last_modified = cursor.execute(
        """SELECT updated_date FROM modifies WHERE dir_path = ?""",
        (dir_path, )
    ).fetchone()[0]
    connection.close()

    if last_modified != current_modified:
        print(f"current updated date: {current_modified}")
        print(f"date from db {last_modified}")
        connection = sqlite3.connect('db.sqlite')
        cursor = connection.cursor()
        cursor.execute(
            f"""UPDATE modifies SET updated_date = ? WHERE dir_path = ?""",
            (current_modified, dir_path)
        )
        connection.commit()
        connection.close()

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'OAuth {access_token}'
    }

    # проверка наличия папки на диске
    params = {
        'path': 'Foundation'
    }

    response = requests.get(
        'https://cloud-api.yandex.net/v1/disk/resources',
        params=params,
        headers=headers
    )
    print(response.status_code)
    print(response.text)

    params = {
        'path': 'Foundation/',
        'overwrite': True
    }

    # Запрос урла для загрузки
    response = requests.get(
        'https://cloud-api.yandex.net/v1/disk/resources/upload',
        params=params,
        headers=headers
    ).json()
    upload_url = response['href']
    # открытие файла и его загрузка на сервер
    with open('C:/Users/Danil/Desktop/test.txt', 'rb') as file:
        try:
            response = requests.put(
                upload_url,
                files={'file': file},
                headers=headers
            )
        except KeyError:
            print(response.text)


main()
