import logging
import os
import zipfile
from pathlib import Path
import sqlite3
import time

import pyzipper
from dotenv import load_dotenv
import requests

logger = logging.getLogger(__name__)

load_dotenv()

ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
PASSWORD = os.getenv('PASSWORD')


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

    content = os.walk(dir_path)
    try:
        zip_file = pyzipper.AESZipFile('Foundation.zip', 'w', encryption=pyzipper.WZ_AES)
        zip_file.pwd = bytes(PASSWORD, encoding='UTF-8')
        for root, folders, files in content:
            for folder_name in folders:
                absolute_path = os.path.join(root, folder_name)
                relative_path = absolute_path.replace(f'{dir_path}', '')
                print(f'Add {absolute_path} to archive')
                zip_file.write(absolute_path, relative_path)

            for file_name in files:
                absolute_path = os.path.join(root, file_name)
                relative_path = absolute_path.replace(f'{dir_path}', '')
                print(f'Add {absolute_path} to archive')
                zip_file.write(absolute_path, relative_path)

        print('zip file created successfully')
    except IOError as error:
        print(error)
    except OSError as error:
        print(error)
    except zipfile.BadZipfile as error:
        print(error)
    finally:
        zip_file.close()

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'OAuth {ACCESS_TOKEN}'
    }

    params = {
        'path': 'Foundation.zip',
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

    zip_file = Path(__file__).cwd() / 'Foundation.zip'
    with open(zip_file, 'rb') as file:
        try:
            response = requests.put(
                upload_url,
                files={'file': file},
                headers=headers
            )
        except KeyError:
            print(response.text)
    with pyzipper.AESZipFile('Foundation.zip') as zip_file:
        zip_file.setpassword(bytes(PASSWORD, encoding='UTF-8'))
        zip_file.extractall('Foundation')


if __name__ == '__main__':
    main()
