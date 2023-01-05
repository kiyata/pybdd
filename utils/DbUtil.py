import csv

import pyodbc
import os
from dotenv import load_dotenv
import pandas as pd
import sqlite3
from sqlite3 import Error


class DbUtil:
    connectionString = ""
    memoryDbServer = "plicsQA"
    load_dotenv()
    sqlDbServer = os.getenv('sqlDbServer')

    # PLICS table names for each feed type
    plicsDbTables = {
        "AMB": "PLIC_AMB.ic.PLIC_AMB_",
        "IAPT": "PLIC_INTEGRATED.ic.PLIC_IAPT_",
        "MHPS": "PLIC_INTEGRATED.ic.PLIC_MH_PS_",
        "MHCC": "PLIC_INTEGRATED.ic.PLIC_MH_CC_",
        "CSCC": "PLIC_INTEGRATED.ic.PLIC_CSCC_",
        "INTREC": "PLIC_INTEGRATED.ic.PLIC_INTGD_",
        "APC": "PLIC_INTEGRATED.ic.PLIC_HES_APC_",
        "EC": "PLIC_INTEGRATED.ic.PLIC_HES_EC_",
        "OP": "PLIC_INTEGRATED.ic.PLIC_HES_OP_",
        "SI": "PLIC_INTEGRATED.ic.PLIC_HES_SI_",
        "SWC": "PLIC_INTEGRATED.ic.PLIC_HES_SWC_"
    }

    def __init__(self):
        self.connectionString = f"Driver=SQL Server;Server={self.sqlDbServer};Database=master;Trusted_Connection=yes;"
        
     
    def set_db_table_name(self, collection, feedType, dataType):
        collectionName = str(collection).replace('"', '')
        feedTypeName = str(feedType).replace('"', '')
        return f"{self.plicsDbTables[feedTypeName]}{dataType}_SQL"
    
    def run_select_query(self, context, single_row=False):
        OrgSubmittingID = context['OrgSubmissionID']
        Month = context['Month']
        Collection = ""
        FeedType = context['feed_type']
       
        table = self.set_db_table_name(Collection, FeedType, "Act")

        query = f"SELECT * FROM {table} WHERE OrgSubmittingID='{OrgSubmittingID}' AND Mnth='{Month}'"
        connection = pyodbc.connect(self.connectionString)
        cursor = connection.cursor()
        cursor.execute(query)
        if single_row:
            return dict(zip(zip(*cursor.description)[0], cursor.fetchone()))
        else:
            columns = [column[0] for column in cursor.description]
            result = [{columns[index][0]:column for index, column in enumerate(value)} for value in cursor.fetchall()]
            return result
    
    def select_db_activity_data_all(self, context):
        OrgSubmittingID = context['OrgSubmittingID']
        Month = context['Mnth']
        Collection = ""
        FeedType = context['feed_type'].replace('"','')
       
        table = self.set_db_table_name(Collection, FeedType, "Act")

        query = f"SELECT * FROM {table} WHERE OrgSubmittingID='{OrgSubmittingID}' AND Mnth='{Month}'"
                
        query = query.replace('\'"', "'")
        query = query.replace('"\'', "'")
        cnxn = pyodbc.connect(self.connectionString)
        return pd.read_sql(query, cnxn)
    
    def select_db_cost_data_all(self, context):
        OrgSubmittingID = context['OrgSubmittingID']
        Month = context['Mnth']
        Collection = ""
        FeedType = context['feed_type']

        table = self.set_db_table_name(Collection, FeedType, "Cost")
        query = f"SELECT * FROM {table} WHERE OrgSubmittingID='{OrgSubmittingID}' AND Mnth='{Month}' ORDER BY OrgSubmittingID ASC, [OrgProviderID] ASC, OrgSubmissionID ASC, Activity_ORDINAL ASC"

        query = query.replace('\'"', "'")
        query = query.replace('"\'', "'")
        cnxn = pyodbc.connect(self.connectionString)
        return pd.read_sql(query, cnxn)


    # def select_xml_data_from_db(self, collection, header_data, data_type):
    #     OrgSubmittingID = header_data['OrgSubmittingID']
    #     CollYear = header_data['CollYear']
    #     FeedType = header_data['FeedType']
    #     Mnth = header_data['Mnth']

    #     table = self.set_db_table_name(collection, FeedType, data_type)

    #     query = f"SELECT * FROM {table}"
    #     query += f" WHERE OrgSubmittingID='{OrgSubmittingID}'"
    #     query += f" AND CollYear='{CollYear}'"
    #     query += f" AND FeedType='{FeedType}'"
    #     query += f" AND Mnth='{Mnth}'"
    #     query = query.replace('\'"', "'")
    #     query = query.replace('"\'', "'")
    #     cnxn = pyodbc.connect(self.connectionString)
    #     cursor = cnxn.cursor()
    #     cursor.execute(query)
    #     return cursor.fetchall()

    # def select_db_cost_data(self, feed_data):
    #     OrgSubmittingID = feed_data.OrgSubmittingID
    #     Month = feed_data.Month
    #     Activity_ORDINAL = feed_data.Activity_ORDINAL
    #     FinYr = feed_data.FinYr
    #     Collection = feed_data.Collection
    #     FeedType = feed_data.FeedType
    #     OrgId = feed_data.OrgId
    #     CstActivity_ORDINAL = feed_data.CstActivity_ORDINAL
    #     Resource_ORDINAL = feed_data.Resource_ORDINAL

    #     table = self.set_db_table_name(Collection, FeedType, "Cost")
    #     query = f"SELECT ActCstID, ActCnt, ResCstID, TotCst AS DECIMAL FROM {table}"
    #     query += f" WHERE OrgSubmittingID='{OrgSubmittingID}'"
    #     query += f" AND OrgProviderID='{OrgId}'"
    #     query += f" AND CollYear='{FinYr}'"
    #     query += f" AND Mnth='{Month}'"
    #     query += f" AND Activity_ORDINAL={Activity_ORDINAL}"
    #     query += f" AND CstActivity_ORDINAL={CstActivity_ORDINAL}"
    #     query += f" AND Resource_ORDINAL={Resource_ORDINAL}"

    #     query = query.replace('\'"', "'")
    #     query = query.replace('"\'', "'")

    #     cnxn = pyodbc.connect(self.connectionString)
    #     cursor = cnxn.cursor()
    #     cursor.execute(query)
    #     result = cursor.fetchall()
    #     return result[0]

    def select_db_rec_data(self, feed_data, ReconciliationLevel):
        OrgSubmittingID = feed_data.OrgSubmittingID
        FinYr = feed_data.FinYr
        Collection = feed_data.Collection
        FeedType = feed_data.FeedType

        table = self.set_db_table_name(Collection, FeedType, "")

        query = f"SELECT FinAccID , SerID ,AgrAdj,TotCst ,TotCstIncome FROM {table}"
        query += f" WHERE OrgSubmittingID='{OrgSubmittingID}'"
        query += f" AND OrgProviderID='{feed_data.OrgId}'"
        query += f" AND CollYear='{FinYr}'"
        query += f" AND ReconciliationLevel='{ReconciliationLevel}'"

        query = query.replace('\'"', "'")
        query = query.replace('"\'', "'")
        cnxn = pyodbc.connect(self.connectionString)
        cursor = cnxn.cursor()
        cursor.execute(query)
        result = list(cursor.fetchall())
        return result

    # def select_summary_data(self, fields, query_data, table_name):
    #     # construct where clause dynamically
    #     whereClause = ""
    #     index = 0
    #     for key in query_data:
    #         whereClause += f"{key} = '{query_data[key]}'"
    #         if index < len(query_data) - 1:
    #             whereClause += f" AND "
    #         index += 1

    #     query = f"SELECT {','.join(fields)} FROM {table_name}"
    #     query += f" WHERE {whereClause} LIMIT 1"

    #     query = query.replace('\'"', "'")
    #     query = query.replace('"\'', "'")

    #     conn = self.create_sqlite_connection()
    #     cursor = conn.cursor()
    #     cursor.execute(query)

    #     record = cursor.fetchone()
    #     if record == None:
    #         print(query)
    #     conn.close()
    #     return record

    # def create_sqlite_connection(self):
    #     conn = None
    #     try:
    #         conn = sqlite3.connect('plics.db')
    #     except Error as e:
    #         print(e)
    #     return conn

    def insert_data_db_table(self, table, data):
        query = "INSERT INTO " + table + "("
        for key in data:
            query += key + ","
        query = query[:-1] + ") VALUES ("
        for key in data:
            query += "'" + str(data[key]) + "',"
        query = query[:-1] + ");"

        conn = self.create_sqlite_connection()
        cursor = conn.cursor()
        #print(query)
        try:
            cursor.execute(query)
            conn.commit()
        except Exception as e:
            print(e)

    # def run_query_memory_db(self, query):
    #     conn = self.create_sqlite_connection()
    #     cursor = conn.cursor()
    #     #print(query)
    #     try:
    #         cursor.execute(query)
    #         conn.commit()
    #     except Exception as e:
    #         print(e)

    # def create_query_string(self, table, data):
    #     OrgSubmittingID = data.OrgSubmittingID
    #     Month = data.Month
    #     FinYr = data.FinYr

    #     query = f"SELECT * FROM {table}"
    #     query += f" WHERE OrgSubmittingID='{OrgSubmittingID}'"
    #     query += f" AND OrgProviderID='{data.OrgId}'"
    #     query += f" AND CollYear='{FinYr}'"
    #     query += f" AND Mnth='{Month}'"
    #     query = query.replace('\'"', "'")
    #     return query.replace('"\'', "'")

    # def select_activity_data(self, db, query):
    #     connectionString = sqlite3.connect('plics.db')
    #     if db == "sql":
    #         connectionString = f"Driver=SQL Server;Server={self.sqlDbServer};Database=master;Trusted_Connection=yes;"
    #     cnxn = pyodbc.connect(connectionString)
    #     cursor = cnxn.cursor()
    #     cursor.execute(query)
    #     return cursor.fetchall()

    def create_sqlite_connection(self):
        conn = None
        try:
            conn = sqlite3.connect('plics.db', timeout=0)
        except csv.Error as e:
            print(e)
        return conn

    # # creates database table based on header of a csv file
    # # all fields are TEXT
    # def create_table_from_csv_header(self, connection, table_name, headers):
    #     cursor = connection.cursor()
    #     try:
    #         cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    #         connection.commit()
    #     except sqlite3.Error as err:
    #         print(err)

    #     query = f"CREATE TABLE IF NOT EXISTS {table_name} ( id INTEGER PRIMARY KEY,"
    #     count = 0
    #     for column in headers:
    #         query += f"{column} TEXT"
    #         if int(count) < len(headers) - 1:
    #             query += ","
    #             count += 1
    #     query += ")"
    #     cursor.execute(query)
    #     connection.commit()

    # def importCsvIntoMemoryDb(self, file_name, table_name):
    #     connection = self.create_sqlite_connection()
    #     with open(file_name, 'r') as f:
    #         reader = csv.reader(file_name, delimiter=',')

    #         # create table
    #         headers = next(reader, None)
    #         self.create_table_from_csv_header(table_name, headers)

    #         query = f"insert into {table_name} ({0}) values ({1})"
    #         query = query.format(','.join(headers), ','.join('?' * len(headers)))
    #         cursor = connection.cursor()
    #         for data in reader:
    #             cursor.execute(query, data)
    #         connection.commit()
    #     connection.close()
