import csv
import sqlite3
from sqlite3 import Error

class CsvUtil:
    feed_type = None

    # constructor
    def __init__(self):
        pass

    def import_csv(self, feed_type, file_name, data_type):
        with open(file_name, 'r') as f:
            reader = csv.reader(f)
            csv_headers = next(reader)
            csv_data = reader

            # create cursor
            connection = sqlite3.connect("plics.db", timeout=0)
            cursor = connection.cursor()

            feedType = feed_type.replace('"', '')
            table_name = f"csv_{feedType}_{data_type}"
            if data_type is None:
                table_name = f"csv_{feedType}"
            columns = ','.join(csv_headers)
            #print(columns)
            # insert data
            query = f"INSERT INTO {table_name} ({columns}) VALUES "

            for row in csv_data:
                query += "('" + "','".join(row) + "'),"

            # remove last comma
            q = query.rstrip(query[-1])
            #print(q)
            try:
                cursor.execute(q)
                connection.commit()
            except:
                print(q)
                print("error inserting data")

            connection.close()
