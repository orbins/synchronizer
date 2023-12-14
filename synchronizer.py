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


class BaseClass:
    ACCESS_TOKEN = os.getenv('ACCESS_TOKEN', None)
    PASSWORD = os.getenv('PASSWORD', None)
    DATE_PLACEHOLDER = '01.01.2000'
    DIR_NAME = os.getenv('DIR_NAME', None)
    HEADERS = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': f'OAuth {ACCESS_TOKEN}'
    }

    def get_upload_url(self, url, params):
        """
        Получение url'a для загрузки/скачивания zip-архива
        :param url - url, который вернет ссылку на скачивание, загрузку
        :param params - параметры запроса
        :return: объект ответа от api яндекс диска
        """
        logger.info('Получение url\'a для скачивания/загрузки zip-архива')
        response = requests.get(
            url,
            params=params,
            headers=self.HEADERS
        ).json()
        return response


class Loader(BaseClass):
    """
    Класс для создания архива
    на основе передаваемого пути
    и отправки его в облако
    """
    PARAMS = {
        'path': f'{BaseClass.DIR_NAME}.zip',
        'overwrite': True
    }
    ACTION_URL = 'https://cloud-api.yandex.net/v1/disk/resources/upload'

    def __init__(self, dir_path):
        """
        :param dir_path: путь к папке, которую нужно синхронизировать
        """
        self.dir_path = dir_path
        self.last_modified = None
        self.current_modified = time.ctime(
            max(os.path.getmtime(root) for root, _, _ in os.walk(self.dir_path))
        )
        self.db_file = Path(__file__).cwd() / 'sync.sqlite'
        self.zip_file = None

    def create_db(self):
        """
        Создание базы данных, в случае отсутствия файла базы
        в текущей директории
        """
        connection = sqlite3.connect('sync.sqlite')
        cursor = connection.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS modifies 
            (id INTEGER PRIMARY KEY AUTOINCREMENT, dir_path TEXT, updated_date TEXT)
            """
        )
        connection.commit()
        cursor.execute(
            """INSERT INTO modifies (dir_path, updated_date)  VALUES (?, ?)""",
            (self.dir_path, self.DATE_PLACEHOLDER)
        )
        connection.commit()
        connection.close()
        logger.info('Файл базы данных создан!')

    def get_date_from_db(self):
        """
        Получение даты последней синхронизации из базы данных
        """
        logger.info('Получения даты последнего изменения из базы данных')
        connection = sqlite3.connect('sync.sqlite')
        cursor = connection.cursor()
        try:
            self.last_modified = cursor.execute(
                """SELECT updated_date FROM modifies WHERE dir_path = ?""",
                (self.dir_path, )
            ).fetchone()[0]
        except TypeError as error:
            logging.error(f'Ошибка получения даты последнего изменения из базы данных:\n{error}')
        connection.close()

    def make_zip(self):
        """
        Создание зашифрованного zip-архива на основе передаваемой директории
        """
        logger.info('Начинается процесс создания и наполнения zip-архива')
        content = os.walk(self.dir_path)
        try:
            with pyzipper.AESZipFile(f'{self.DIR_NAME}.zip', 'w', encryption=pyzipper.WZ_AES) as zip_file:
                zip_file.pwd = bytes(self.PASSWORD, encoding='UTF-8')
                for root, folders, files in content:

                    for folder_name in folders:
                        absolute_path = os.path.join(root, folder_name)
                        relative_path = absolute_path.replace(f'{self.dir_path}', '')
                        logger.info(f'Добавление папки {absolute_path} в zip-архив')
                        zip_file.write(absolute_path, relative_path)
                        # absolute - путь к файлу в файловой системе, который нужно добавить в архив (откуда брать)
                        # relative - Относительный путь внутри архива, куда будет добавлен файл. (куда добавить)

                    for file_name in files:
                        absolute_path = os.path.join(root, file_name)
                        relative_path = absolute_path.replace(f'{self.dir_path}', '')
                        logger.info(f'Добавление {absolute_path} в zip-архив')
                        zip_file.write(absolute_path, relative_path)
                logger.info('Zip-архив успешно создан!')
                self.zip_file = Path(__file__).cwd() / f'{self.DIR_NAME}.zip'
        except IOError as error:
            logger.error(f'Не удалось создать zip-архив. Ошибка ввода при заполнении архива:\n{error}')
        except OSError as error:
            logger.error(f'Не удалось создать zip-архив. Ошибка системы при заполнении архива:\n{error}')
        except zipfile.BadZipfile as error:
            logger.error(f'Не удалось создать zip-архив. Ошибка записи в архив:\n{error}')

    def load_zip(self, upload_url):
        """Загрузка архива в облако"""
        logger.info('Загрузка архива на сервер')
        with open(self.zip_file, 'rb') as file:
            try:
                requests.put(
                    upload_url,
                    files={'file': file},
                    headers=self.HEADERS
                )
                return True
            except KeyError as error:
                logger.error(f'Ошибка загрузки файла на сервер:\n {error}')
                return False

    def save_date_to_db(self):
        """Изменение даты последней синхронизации"""
        logger.info('Изменение даты обновления в базе данных')
        connection = sqlite3.connect('sync.sqlite')
        cursor = connection.cursor()
        try:
            cursor.execute(
                f"""UPDATE modifies SET updated_date = ? WHERE dir_path = ?""",
                (self.current_modified, self.dir_path)
            )
        except TypeError as error:
            logger.error(f"Ошибка, дата последней синхронизации не обновлена в БД:\n{error}")
        connection.commit()
        connection.close()
        logger.info('Синхронизация успешно завершена!')

    def main(self):
        if not self.db_file.exists():
            logger.info('Файл базы данных в текущей директории не найден.')
            self.create_db()
        self.get_date_from_db()
        if self.last_modified:
            logger.info(f"Дата последнего изменения (фактическая): {self.current_modified}")
            logger.info(f"Дата последнего изменения из базы данных: {self.last_modified}")
            if self.last_modified != self.current_modified:
                logger.info('Указанная директория обновлялась. Начинается процесс синхронизации')
                self.make_zip()
                if self.zip_file.exists():
                    response = self.get_upload_url(self.ACTION_URL, self.PARAMS)
                    upload_url = response.get('href', None)
                    if upload_url:
                        is_loaded = self.load_zip(upload_url)
                        if is_loaded:
                            self.save_date_to_db()
                            os.remove(Path(__file__).cwd() / f'{self.DIR_NAME}.zip')
                            logger.info('Zip-архив удалён!')
                    else:
                        logging.error(f'Ошибка получения ссылки url\'a для загрузки:\n{response}')
                else:
                    logging.error('Zip-архив не был создан')
            else:
                logger.info('Указанная директория не обновлялась с последней проверки, синхронизация не требуется.')
        else:
            logger.error('Не удалось получить последнюю дату модификации файла!')


class Importer(BaseClass):
    """Класс для скачивания и декодирования архива из облака"""
    PARAMS = {
        'path': f'{BaseClass.DIR_NAME}.zip',
    }
    ACTION_URL = 'https://cloud-api.yandex.net/v1/disk/resources/download'

    def main(self):
        response = self.get_upload_url(self.ACTION_URL, self.PARAMS)
        download_url = response.get('href', None)
        if download_url:
            logger.info('URL для скачивания архива получен')
            response = requests.get(download_url, self.HEADERS)
            if response.status_code == 200:
                with open(f'{self.DIR_NAME}.zip', 'wb') as file:
                    file.write(response.content)
                logger.info('Архив сохранен')
                with pyzipper.AESZipFile(f'{self.DIR_NAME}.zip') as zip_file:
                    zip_file.setpassword(bytes(self.PASSWORD, encoding='UTF-8'))
                    zip_file.extractall(f'{self.DIR_NAME}')
                logger.info('Архив декодирован и распакован')
                os.remove(Path(__file__).cwd() / f'{self.DIR_NAME}.zip')
                logger.info('Zip-архив удалён')
            else:
                logger.error(f'Ошибка при скачивании файла с сервера: {response.text}')
        else:
            logger.error(f'Ошибка при полуении url\'а для скачивания:\n{response.text}')


if __name__ == '__main__':
    def select_action():
        try:
            action = input("Введите '1' для загруки архива в облако и '2' для скачивания из облака: ")
            if action == '1':
                dir_path = os.getenv('HOST_PATH', None)
                if Path(dir_path).exists() and Path(dir_path).is_dir():
                    instance = Loader(dir_path)
                    instance.main()
                else:
                    logger.error('Путь к директории не найден! Проверьте env файл!')
                    select_action()
            elif action == '2':
                instance = Importer()
                instance.main()
            else:
                print('Некорректный ввод, попробуйте ещё раз!')
                select_action()
        except Exception as error:
            logger.error(f'Непредвиденная ошибка:\n {error}')

    select_action()