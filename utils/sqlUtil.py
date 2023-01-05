import pyodbc
import os
from dotenv import load_dotenv
import sqlite3
from sqlite3 import Error


# noinspection PyMethodMayBeStatic
class sqlUtil:
    connectionString = ""
    load_dotenv()
    sqlDbServer = os.getenv('sqlDbServer')

    # PLICS table names for each feed type
    sqlTables = {
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
        return f"{self.sqlTables[collectionName]}_{feedTypeName}_{dataType}_SQL"

    def select_db_activity_data(self, field, feed_data):
        OrgSubmittingID = feed_data.OrgSubmittingID
        Month = feed_data.Month
        Activity_ORDINAL = feed_data.Activity_ORDINAL
        FinYr = feed_data.FinYr
        Collection = feed_data.Collection
        FeedType = feed_data.FeedType

        table = self.set_db_table_name(Collection, FeedType, "Act")

        query = f"SELECT {field} FROM {table}"
        query += f" WHERE OrgSubmittingID='{OrgSubmittingID}'"
        query += f" AND OrgProviderID='{feed_data.OrgId}'"
        query += f" AND CollYear='{FinYr}'"
        query += f" AND Mnth='{Month}'"
        ORDINAL_FIELD = "Activity_ORDINAL"
        if FeedType.upper().replace('"', "") == "APC":
            ORDINAL_FIELD = f"APCActivity_ORDINAL"

        query += f" AND {ORDINAL_FIELD}='{Activity_ORDINAL}'"
        query = query.replace('\'"', "'")
        query = query.replace('"\'', "'")
        cnxn = pyodbc.connect(self.connectionString)
        cursor = cnxn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0]

    def select_db_cost_data(self, feed_data):
        OrgSubmittingID = feed_data.OrgSubmittingID
        Month = feed_data.Month
        Activity_ORDINAL = feed_data.Activity_ORDINAL
        FinYr = feed_data.FinYr
        Collection = feed_data.Collection
        FeedType = feed_data.FeedType
        OrgId = feed_data.OrgId
        CstActivity_ORDINAL = feed_data.CstActivity_ORDINAL
        Resource_ORDINAL = feed_data.Resource_ORDINAL

        table = self.set_db_table_name(Collection, FeedType, "Cost")
        query = f"SELECT ActCstID, ActCnt, ResCstID, TotCst AS DECIMAL FROM {table}"
        query += f" WHERE OrgSubmittingID='{OrgSubmittingID}'"
        query += f" AND OrgProviderID='{OrgId}'"
        query += f" AND CollYear='{FinYr}'"
        query += f" AND Mnth='{Month}'"
        query += f" AND Activity_ORDINAL={Activity_ORDINAL}"
        query += f" AND CstActivity_ORDINAL={CstActivity_ORDINAL}"
        query += f" AND Resource_ORDINAL={Resource_ORDINAL}"

        query = query.replace('\'"', "'")
        query = query.replace('"\'', "'")

        cnxn = pyodbc.connect(self.connectionString)
        cursor = cnxn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()
        return result[0]

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

    def get_orgSubmission_id(self, file_name):
        table = "PLIC_INTEGRATED.ic.PLIC_SHARED_Audit_ALL"
        query = f"SELECT TOP 1 OrgSubmissionID FROM {table} WHERE fileName='{file_name}' ORDER BY Audit_IN_Key DESC"
        cnxn = pyodbc.connect(self.connectionString)
        cursor = cnxn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        return result[0]
