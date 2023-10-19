import logging
import os
from pathlib import Path
import sqlite3
import time
import zipfile

import pyzipper
from dotenv import load_dotenv
import requests

# SETTINGS
logger = logging.getLogger(__name__)
logger.setLevel('INFO')

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
        logger.info('Файл базы данных в текущей директории не найден.')
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
        logger.info('Файл базы данных создан!')

    connection = sqlite3.connect('db.sqlite')
    cursor = connection.cursor()
    last_modified = cursor.execute(
        """SELECT updated_date FROM modifies WHERE dir_path = ?""",
        (dir_path, )
    ).fetchone()[0]
    connection.close()
    print(f"current updated date: {current_modified}")
    print(f"date from db {last_modified}")

    if last_modified != current_modified:
        logger.info('Указанная директория обновлялась. Начинается процесс синхронизации')
        content = os.walk(dir_path)
        try:
            zip_file = pyzipper.AESZipFile('Foundation.zip', 'w', encryption=pyzipper.WZ_AES)
            zip_file.pwd = bytes(PASSWORD, encoding='UTF-8')
            for root, folders, files in content:
                for folder_name in folders:
                    absolute_path = os.path.join(root, folder_name)
                    relative_path = absolute_path.replace(f'{dir_path}', '')
                    logger.info(f'Добавление папки {absolute_path} в zip-архив')
                    zip_file.write(absolute_path, relative_path)
                    # absolute - путь к файлу для внесения в архив, relative - путь и имя внутри архива

                for file_name in files:
                    absolute_path = os.path.join(root, file_name)
                    relative_path = absolute_path.replace(f'{dir_path}', '')
                    logger.info(f'Добавление {absolute_path} в zip-архив')
                    zip_file.write(absolute_path, relative_path)
            logger.info('Zip-архив успешно создан!')
        except IOError as error:
            logger.error(f'Ошибка ввода при заполнении архива:\n{error}')
            return
        except OSError as error:
            logger.error(f'Ошибка системы при заполнении архива:\n{error}')
            return
        except zipfile.BadZipfile as error:
            logger.error(f'Ошибка записи в архив:\n{error}')
            return
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

        logger.info('Получение url\'a для загрузки zip-архива')
        response = requests.get(
            'https://cloud-api.yandex.net/v1/disk/resources/upload',
            params=params,
            headers=headers
        ).json()

        upload_url = response['href']
        zip_file = Path(__file__).cwd() / 'Foundation.zip'

        logger.info('Загрузка архива на сервер')
        with open(zip_file, 'rb') as file:
            try:
                response = requests.put(
                    upload_url,
                    files={'file': file},
                    headers=headers
                )
            except KeyError as error:
                logger.error(f'Ошибка загрузки файла на сервер:\n {error}')

        # Декодирование архива с целью проверки работы софта
        with pyzipper.AESZipFile('Foundation.zip') as zip_file:
            zip_file.setpassword(bytes(PASSWORD, encoding='UTF-8'))
            zip_file.extractall('Foundation')

        logger.info('Изменение даты обновления в базе данных')
        connection = sqlite3.connect('db.sqlite')
        cursor = connection.cursor()
        cursor.execute(
            f"""UPDATE modifies SET updated_date = ? WHERE dir_path = ?""",
            (current_modified, dir_path)
        )
        connection.commit()
        connection.close()
        logger.info('Синхронизация успешно завершена!')


if __name__ == '__main__':
    main()
