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
DATE_PLACEHOLDER = '01.01.2000'


class Synchronizer:

    def __init__(self, dir_path):
        self.dir_path = dir_path
        self.last_modified = None
        self.current_modified = None
        self.db_file = None
        self.zip_file = None

    def create_db(self):
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
            """INSERT INTO modifies (dir_path)  VALUES (?, ?)""",
            (self.dir_path, DATE_PLACEHOLDER)
        )
        connection.commit()
        connection.close()
        logger.info('Файл базы данных создан!')

    def get_date_from_db(self):
        logger.info('Получения даты последнего изменения из базы данных')
        connection = sqlite3.connect('db.sqlite')
        cursor = connection.cursor()
        try:
            self.last_modified = cursor.execute(
                """SELECT updated_date FROM modifies WHERE dir_path = ?""",
                (self.dir_path, )
            ).fetchone()[0]
        except Exception as error:
            logging.error(f'Ошибка получения даты последнего изменения из базы данных:\n{error}')
        connection.close()

    def make_zip(self):
        logger.info('Начинается процесс создания и наполнения zip-архива')
        content = os.walk(self.dir_path)
        try:
            zip_file = pyzipper.AESZipFile('Foundation.zip', 'w', encryption=pyzipper.WZ_AES)
            zip_file.pwd = bytes(PASSWORD, encoding='UTF-8')
            for root, folders, files in content:
                for folder_name in folders:
                    absolute_path = os.path.join(root, folder_name)
                    relative_path = absolute_path.replace(f'{self.dir_path}', '')
                    logger.info(f'Добавление папки {absolute_path} в zip-архив')
                    zip_file.write(absolute_path, relative_path)
                    # absolute - путь к файлу для внесения в архив, relative - путь и имя внутри архива

                for file_name in files:
                    absolute_path = os.path.join(root, file_name)
                    relative_path = absolute_path.replace(f'{self.dir_path}', '')
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

    @staticmethod
    def get_upload_url(headers):
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
        return response

    def load_zip(self, upload_url, headers):
        logger.info('Загрузка архива на сервер')
        with open(self.zip_file, 'rb') as file:
            try:
                requests.put(
                    upload_url,
                    files={'file': file},
                    headers=headers
                )
            except KeyError as error:
                logger.error(f'Ошибка загрузки файла на сервер:\n {error}')

    def save_date_to_db(self):
        logger.info('Изменение даты обновления в базе данных')
        connection = sqlite3.connect('db.sqlite')
        cursor = connection.cursor()
        cursor.execute(
            f"""UPDATE modifies SET updated_date = ? WHERE dir_path = ?""",
            (self.current_modified, self.dir_path)
        )
        connection.commit()
        connection.close()

    def main(self):
        self.current_modified = time.ctime(
            max(os.path.getmtime(root) for root, _, _ in os.walk(self.dir_path))
        )
        self.db_file = Path(__file__).cwd() / 'db.sqlite'

        if not self.db_file.exists():
            logger.info('Файл базы данных в текущей директории не найден.')
            self.create_db()

        self.get_date_from_db()

        if self.last_modified != self.current_modified:
            logger.info('Указанная директория обновлялась. Начинается процесс синхронизации')
            self.make_zip()
            self.zip_file = Path(__file__).cwd() / 'Foundation.zip'

            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': f'OAuth {ACCESS_TOKEN}'
            }

            response = self.get_upload_url(headers)
            if response['status_code'] != 200:
                return
            upload_url = response['href']

            if self.zip_file:
                self.load_zip(upload_url, headers)

            self.save_date_to_db()
            logger.info('Синхронизация успешно завершена!')


if __name__ == '__main__':
    instance = Synchronizer('C:/Users/Danil/Desktop/Foundation')
    instance.main()

# Декодирование архива с целью проверки работы софта
# with pyzipper.AESZipFile('Foundation.zip') as zip_file:
#     zip_file.setpassword(bytes(PASSWORD, encoding='UTF-8'))
#     zip_file.extractall('Foundation')
