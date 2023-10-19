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

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

file_handler = logging.FileHandler('sync.log', encoding='UTF-8')
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

load_dotenv()

ACCESS_TOKEN = os.getenv('ACCESS_TOKEN')
PASSWORD = os.getenv('PASSWORD')
DATE_PLACEHOLDER = '01.01.2000'


class Synchronizer:

    def __init__(self, dir_path):
        self.dir_path = dir_path
        self.last_modified = None
        self.current_modified = time.ctime(
            max(os.path.getmtime(root) for root, _, _ in os.walk(self.dir_path))
        )
        self.db_file = Path(__file__).cwd() / 'db.sqlite'
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
            self.zip_file = Path(__file__).cwd() / 'Foundation.zip'
        except IOError as error:
            logger.error(f'Не удалось создать zip-архив. Ошибка ввода при заполнении архива:\n{error}')
        except OSError as error:
            logger.error(f'Не удалось создать zip-архив. Ошибка системы при заполнении архива:\n{error}')
        except zipfile.BadZipfile as error:
            logger.error(f'Не удалось создать zip-архив. Ошибка записи в архив:\n{error}')
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
                return True
            except KeyError as error:
                logger.error(f'Ошибка загрузки файла на сервер:\n {error}')
                return False

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
        logger.info('Синхронизация успешно завершена!')

    def main(self):
        if not self.db_file.exists():
            logger.info('Файл базы данных в текущей директории не найден.')
            self.create_db()
        self.get_date_from_db()
        if not self.last_modified:
            return
        logger.info(f"Date from db: {self.last_modified}")
        logger.info(f"current date: {self.current_modified}")
        if self.last_modified != self.current_modified:
            logger.info('Указанная директория обновлялась. Начинается процесс синхронизации')
            self.make_zip()
            if not self.zip_file.exists():
                return
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': f'OAuth {ACCESS_TOKEN}'
            }
            response = self.get_upload_url(headers)
            upload_url = response.get('href', None)
            if not upload_url:
                logging.error(f'Ошибка получения ссылки url\'a для загрузки:\n{response}')
                return
            is_loaded = self.load_zip(upload_url, headers)
            if not is_loaded:
                return
            self.save_date_to_db()
        else:
            logger.info('Указанная директория не обновлялась с последней проверки, синхронизация не требуется.')


if __name__ == '__main__':
    instance = Synchronizer('C:/Users/Danil/Desktop/Foundation')
    instance.main()

# Декодирование архива с целью проверки работы софта
# with pyzipper.AESZipFile('Foundation.zip') as zip_file:
#     zip_file.setpassword(bytes(PASSWORD, encoding='UTF-8'))
#     zip_file.extractall('Foundation')
