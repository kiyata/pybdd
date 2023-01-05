
import sqlite3
from sqlite3 import Error


class SqliteUtil:
    dbName = "plics.db"

    def __init__(self):
        pass

    def run_query(self, query):
        try:
            conn = sqlite3.connect(self.dbName, timeout=0)
            cursor = conn.cursor()
            cursor.execute(query)
            conn.commit()
        except Exception as e:
            print(e)

    def select_data(self, query):
        try:
            conn = sqlite3.connect(self.dbName, timeout=0)
            conn.row_factory = sqlite3.Row # includes column headers in results
            cursor = conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            print(e)
