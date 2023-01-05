import os.path
from bs4 import BeautifulSoup
import os.path
from bs4 import BeautifulSoup
from utils import DataFields, DbUtil, BaseUtil, MappingUtil
from decouple import config


class XmlExtractor:
    feed_type = ""
    xml_data = ""
    xml_activity_fields = []
    xml_costing_fields = []
    xml_resource_fields = []
    xml_header_data = {}
    db_activity_fields = []
    db_costing_fields = []
    config_dir = os.path.dirname(__file__) + "/utils/"
    header_fields = {}
    activity_fields = []
    intrec_fields = []
    costing_fields = []
    resource_fields = {}
    db = ""
    dbUtil = None
    fieldData = []
    mappingUtil = MappingUtil.MappingUtil()
    FinancialYearStartDate = ""

    # constructor
    def __init__(self):
        self.baseUtil = BaseUtil.BaseUtil()
        self.fieldData = DataFields.DataFields()
        self.dbUtil = DbUtil.DbUtil()
        # self.FinancialYearStartDate = f"{config('FinancialYearStartDate')}"
        self.FinancialYearStartDate = "2021-04-01"

    def extract_xml_file(self, xml_file):
        self.read_xml_data(xml_file)
        self.get_xml_header_data()
        self.activity_fields = self.mappingUtil.get_field_mappings(self.xml_header_data['FeedType'], "extract",
                                                                   "activity")
        self.costing_fields = self.mappingUtil.get_field_mappings(self.xml_header_data['FeedType'], "extract", "cost")
        self.intrec_fields = self.mappingUtil.get_rec_mappings(self.xml_header_data['FeedType'], "extract")

    # reads xml
    def read_xml_data(self, xml_file):
        file_dir = os.path.dirname(os.path.realpath(__file__))
        file_name = os.path.join(file_dir, f"..\\resources\\xml\\{xml_file}")
        with open(file_name) as f:
            soup = BeautifulSoup(f, 'xml')
        self.xml_data = soup

    # read xml header data
    def get_xml_header_data(self):
        data = {}
        header_data = str(self.xml_data.find_all('ns:MessageHeader')[0])
        soup_content = BeautifulSoup(header_data, 'lxml-xml')

        data['OrgSubmittingID'] = soup_content.find("OrgSubmittingID").text
        data['FinYr'] = soup_content.find("FinYr").text
        data['FeedType'] = soup_content.find("FeedType").text
        data['Mnth'] = 'M' + (soup_content.find("PeriodStartDate").text).split("-")[1]
        self.xml_header_data = data

    # extracts xml activities and costs & inserts them to DB
    def process_xml_data(self, OrgSubmissionID):
        resourceFields = ["ResCstID", "TotCst"]
        cstActivityFields = ["ActCstID", "ActCnt"]
        act_table_name = f"xml_{self.xml_header_data['FeedType'].lower()}_act"
        cost_table_name = f"xml_{self.xml_header_data['FeedType'].lower()}_cost"
        Activity_ORDINAL = 1
        activities = self.xml_data.find_all('ns:Activity')
        FinancialYearStartDate = "2021-04-01"
        CstActivity_ORDINAL = 1
        Resource_ORDINAL = 1
        for item in activities:
            activity_content = BeautifulSoup(str(item), 'lxml-xml')
            activity_data = {'OrgSubmittingID': self.xml_header_data['OrgSubmittingID'],
                             'Mnth': self.xml_header_data['Mnth'],
                             'FinYr': self.xml_header_data['FinYr'],
                             'FeedType': self.xml_header_data['FeedType']}

            for attr in self.activity_fields:
                if attr not in self.xml_header_data:
                    try:
                        if attr == 'FinancialYearStartDate':
                            activity_data[attr] = FinancialYearStartDate
                        else:
                            activity_data[attr] = activity_content.find(attr).text
                    except:
                        activity_data[attr] = None
                        pass
            # overwrite orgsubmissionid
            activity_data["OrgSubmissionID"] = OrgSubmissionID

            # overwrite activity_ordinal
            if self.xml_header_data['FeedType'] == "APC":
                activity_data["APCActivity_ORDINAL"] = Activity_ORDINAL
            else:
                activity_data["Activity_ORDINAL"] = Activity_ORDINAL

            # insert activity to sqlite
            self.dbUtil.insert_data_db_table(act_table_name, activity_data)

            # loop costings
            
            costs = activity_content.find_all('CstActivity')
            
            for cost in costs:
                cost_data = {'OrgSubmissionID': OrgSubmissionID,
                             'OrgSubmittingID': self.xml_header_data['OrgSubmittingID'],
                             'Mnth': self.xml_header_data['Mnth'],
                             'FeedType': self.xml_header_data['FeedType'], 'OrgId': activity_data['OrgId'],
                             'Activity_ORDINAL': Activity_ORDINAL, 'CstActivity_ORDINAL': CstActivity_ORDINAL,
                             'FinancialYearStartDate': FinancialYearStartDate}
                cost_content = BeautifulSoup(str(cost), 'lxml-xml')
                for cstActivityField in cstActivityFields:
                    try:
                        cost_data[cstActivityField] = cost_content.find(cstActivityField).text
                    except:
                        cost_data[cstActivityField] = None
                        pass
                
                resources = cost_content.find_all('Resource')
                for resource in resources:
                    resource_content = BeautifulSoup(str(resource), 'lxml-xml')
                    cost_data['Resource_ORDINAL'] = Resource_ORDINAL
                    for resourceField in resourceFields:
                        try:
                            cost_data[resourceField] = resource_content.find(resourceField).text
                        except:
                            cost_data[resourceField] = None
                            pass
                    # insert data
                    self.dbUtil.insert_data_db_table(cost_table_name, cost_data)
                    Resource_ORDINAL += 1
                CstActivity_ORDINAL += 1
            Activity_ORDINAL += 1

    # extracts intrec
    def process_intrec(self, ReconciliationID):
        rec_table_name = f"xml_{self.xml_header_data['FeedType'].lower()}"
        FinancialYearStartDate = "2021-04-01"
        account_contents = BeautifulSoup(str(self.xml_data.find('ns:FinalAuditedAccounts')), 'lxml-xml')
        exclusion_contents = BeautifulSoup(str(self.xml_data.find('ns:ServiceCostExclusions')), 'lxml-xml')

        accounts = account_contents.find_all('Accts')
        for account in accounts:
            data = {'FinancialYearStartDate': FinancialYearStartDate, 'ReconciliationID': ReconciliationID,
                    'ReconciliationLevel': 1, 'FinAccID': account.find('FinAccID').text,
                    'CstIncVal': account.find('CstIncVal').text}

            # insert record to sqlite
            self.dbUtil.insert_data_db_table(rec_table_name, data)

        exclusions = exclusion_contents.find_all('Exclusions')
        for exclusion in exclusions:
            data = {'FinancialYearStartDate': FinancialYearStartDate, 'ReconciliationID': ReconciliationID,
                    'ReconciliationLevel': 2, 'TotCst': exclusion.find('TotCst').text,
                    'SerID': exclusion.find('SerID').text, 'AgrAdj': exclusion.find('AgrAdj').text}

            # insert record to sqlite
            self.dbUtil.insert_data_db_table(rec_table_name, data)
