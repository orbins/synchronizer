import os
import sqlite3
import time


def main():
    dir_path = 'C:/Users/Danil/Desktop/Foundation'
    current_modified = time.ctime(
        max(os.path.getmtime(root) for root, _, _ in os.walk(dir_path))
    )
    # try-except, преобразование в формат

    connection = sqlite3.connect('db.sqlite')
    cursor = connection.cursor()
    cursor.execute(
        """
        CREATE TABLE modifies IF NOT EXIST 
        (id INTEGER, PRIMARY KEY AUTOINCREMENT, dir_path TEXT, date TEXT)
        """
    )
    connection.commit()
    last_modified = cursor.execute(
        """SELECT date FROM modifies WHERE dir_path is ?""", dir_path
    ).fetchone()
    connection.close()
    if last_modified:
        if last_modified != current_modified:
            connection = sqlite3.connect('db.sqlite')
            cursor = connection.cursor()
            cursor.execute(
                f"""INSERT ? INTO modifies WHERE dir_path is ?""", (current_modified, dir_path)
            )
            connection.commit()
            connection.close()
            ...
            # сохранение в onedrive


main()
