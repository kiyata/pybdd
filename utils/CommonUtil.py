from asyncio.windows_events import NULL
from datetime import datetime
import hashlib
import base64
import calendar
from pydoc import text
from decouple import config


# noinspection PyMethodMayBeStatic
class CommonUtil:

    def __init__(self):
        pass

    def compare_single_row(self, xml_data, csv_data, field_mappings, jobType):

        totalPassed = 0
        totalFailed = 0
        results = []
        skippedFields = self.getConfig("SKIPPED")
        for xml_field in field_mappings:
            entry = {}
            if xml_field not in skippedFields:
                entry["field"] = xml_field
                entry["expected"] = self.getExpectedValue(xml_data, xml_field, csv_data, jobType)
                entry["actual"] = csv_data[field_mappings[xml_field]]

                if entry["actual"] == entry["expected"]:
                    totalPassed += 1
                else:
                    totalFailed += 1
                results.append(entry)
        if xml_data["FeedType"] == "IAPT":
            results.insert(3, {"field": "IAPT_Person_ID", "expected": "", "actual": ""})
        return {"totalPassed": totalPassed, "totalFailed": totalFailed, "tests": results}

    def iteration_result_report(self, tests):
        results = []
        for item in tests:
            entry = {'field': item["field"], 'expected': item["expected"], 'actual': item["actual"],
                     'result': 'OK' if item["actual"] == item["expected"] else 'KO'}
            results.append(entry)

        return results

    def calculate_age(self, data):
        feedType = data['FeedType']
        try:
            xml_age_field = self.getConfig("AGE_FIELDS", feedType)
            hospital_date = data[xml_age_field]
            dob = data['DoB']
        except:
            hospital_date = None
            dob = None
        if feedType == "IAPT":
            return self.iapt_age(dob, hospital_date)
        elif feedType == "CSCC":
            return self.cscc_age(dob, hospital_date)
        else:
            return self.generic_age(dob, hospital_date)

    def generic_age(self, dob, hosp_date):

        if self.validate(dob) is False or self.validate(hosp_date) is False:
            return 999
        if hosp_date is None or len(hosp_date) <= 0 or dob is None or len(dob) <= 0:
            return 999
        start_date = datetime.strptime(dob, "%Y-%m-%d")
        end_date = datetime.strptime(hosp_date, "%Y-%m-%d")
        leap_years = calendar.leapdays(start_date.year, end_date.year)
        number_of_days = (end_date - start_date).days - leap_years
        if number_of_days <= 6:
            return 901
        elif 7 <= number_of_days <= 28:
            return 902
        elif 29 <= number_of_days <= 90:
            return 903
        elif 91 <= number_of_days <= 181:
            return 904
        elif 182 <= number_of_days <= 272:
            return 905
        elif 273 <= number_of_days <= 366:
            return 906
        elif number_of_days > 366:
            return round(number_of_days / 365)
        else:
            return 999

    def iapt_age(self, dob, app_date):

        if app_date is None or len(app_date) <= 0 or dob is None or len(dob) <= 0:
            return 999
        start_date = datetime.strptime(dob, "%Y-%m-%d")
        end_date = datetime.strptime(app_date, "%Y-%m-%d")
        years = end_date.year - start_date.year
        if years <= 120:
            return years
        else:
            return 999

    def cscc_age(self, dob, care_date):
        if care_date is None or len(care_date) <= 0 or dob is None or len(dob) <= 0:
            return "999"
        start_date = datetime.strptime(dob, "%Y-%m-%d")
        end_date = datetime.strptime(care_date, "%Y-%m-%d")
        years = end_date.year - start_date.year
        if years >= 999:
            return "999"
        lower = years - (years % 5)
        return "%s-%s" % (lower, lower + 4)

    def hashing(self, xml_field, xml_data, csv_data):
        feedType = xml_data["FeedType"]
        if feedType in ["MHCC", "MHPS"] and xml_field == "NHSNo":
            return csv_data["NHSNumber"]

        xml_value = xml_data[xml_field]
        org_id = xml_data["OrgId"]
        # we only hash valid xml data
        if xml_value in [" ", None, "None", "NULL", "EMPTY"] or len(xml_value) <= 0:
            return ""

        salt = self.getConfig(feedType, "salts")[xml_field]
        to_be_hashed = self.getStringToBeHashed(feedType, xml_field, xml_value, org_id, salt)
        encoding = self.getEncoding(feedType, xml_field)
        to_be_hashed_as_bytes = to_be_hashed.encode(encoding)

        if config("HASHALGORYGTHM") == 'sha256':
            hash_as_bytes = base64.b64encode(hashlib.sha256(to_be_hashed_as_bytes).digest())
            return base64.b64decode(hash_as_bytes).hex().upper()
        else:
            hash_as_bytes = base64.b64encode(hashlib.sha512(to_be_hashed_as_bytes).digest())
            return base64.b64decode(hash_as_bytes).hex().upper()

    def getStringToBeHashed(self, feet_type, xml_field, xml_value, org_id, salt):
        if xml_field == "PLEMI":
            return salt + xml_value + org_id
        elif xml_field == "NHSNo":
            return salt + xml_value
        elif xml_field in ["SerReqID", "HSpellId", "CareId", "CareConID"]:
            if feet_type in ["CSCC"]:
                return org_id + xml_value + salt
            else:
                return xml_value + salt
        else:
            return org_id + xml_value + salt

    def getEncoding(self, feedType, xml_field):
        encoding = 'ascii'  # default pseudomisation encoding
        if self.getConfig("NVARCHAR") == "true":
            nvarchar_fields = self.getConfig("NVARCHARFIELDS")
            if feedType in ["CSCC", "IAPT", "MHCC", "MHPS"] and xml_field in nvarchar_fields:
                encoding = 'utf_16_le'  # for nvarchar encoding
        return encoding

    def validate(self, date_text):
        try:
            if date_text is not None and date_text != "":
                datetime.strptime(date_text, '%Y-%m-%d')
                return True
            else:
                return False
        except ValueError:
            return False

    def formatTotCst(self, data, jobType):

        if jobType != "weekly":
            return format(float(data), '.8f')

        parts = data.split(".")
        if len(parts) == 1:
            return f"{data}.00000000"
        if len(parts[1]) >= 5:
            return f"{parts[0]}.{parts[1][0:5]}000"
        else:
            return format(float(data), '.8f')

    def formatActCnt(self, value):
        data = value.replace("'", "").replace('"', '')
        # sas issue with dropping 18th digit, round and then replacing with 0
        if len(data) >= 18:
            num_str = str(data)
            last_digit_str = num_str[-1]
            last_digit = int(last_digit_str)

            if last_digit >= 5:
                second_last_digit_str = num_str[-2:-1]
                second_last_digit = int(second_last_digit_str) + 1
                return num_str[0:len(data) - 2] + str(second_last_digit) + "0"
            else:
                return num_str[0:len(data) - 3] + "00"
        else:
            return data

    def getGender(self, xml_data):
        try:
            value = xml_data["Gendr"]
        except:
            value = ""
        if value not in ["1", "2", "9", "x", "X"]:
            value = ""
        return value

    def getExpectedValue(self, xml_data, xml_field, csv_data, jobType):
        hashedFields = self.getConfig("HASHED")
        specialFields = {
            'DoB': self.calculate_age(xml_data),
            'Gendr': self.getGender(xml_data),
            'TotCst': self.formatTotCst(xml_data[xml_field], jobType),
            'ActCnt': self.formatActCnt(xml_data[xml_field]),
            "LptID": ""
        }
        if xml_data["FeedType"] in ["MHCC", "MHPS"] and xml_field == "NHSNo":
            return csv_data["NHSNumber"]
        if xml_field == "UniqServReqID" and xml_data["FeedType"] in ["MHCC", "MHPS"]:
            return csv_data["UniqServReqID"]
        if xml_data[xml_field] == "" or xml_data[xml_field] == "None" or xml_data[xml_field] is None:
            return ""
        elif xml_field in hashedFields:
            return self.hashing(xml_field, xml_data, csv_data)
        elif xml_field in specialFields:
            return specialFields[xml_field]
        else:
            return xml_data[xml_field]

    def getConfig(self, attribute, parent=None, grandChild=None):
        file_name = f"{config('LOCAL_RESOURCE_DIR')}/config.json"
        with open(file_name, 'r') as f:
            configs = json.load(f)
        if grandChild is not None:
            return configs[attribute][parent][grandChild]
        elif parent is not None:
            return configs[attribute][parent]
        else:
            return configs[attribute]

    def compare_single_row_daily(self, xml_data, db_data, field_mappings, jobType):
        totalPassed = 0
        totalFailed = 0
        actualIndex = 0
        results = []
        orgSubmissionId = xml_data['OrgSubmissionID'].replace('"', '')
        skippedFields = self.skippedDailyField()
        for xml_field in field_mappings:
            entry = {}
            if xml_field not in skippedFields:
                entry["field"] = xml_field
                db_field = field_mappings[xml_field]
                if xml_field == 'ORGActivityID':
                    if xml_data["FeedType"] == "APC":
                        entry["expected"] = xml_data['APCActivity_ORDINAL'] + orgSubmissionId + xml_data['Mnth']
                    else:
                        entry["expected"] = xml_data['Activity_ORDINAL'] + orgSubmissionId + xml_data['Mnth']
                elif xml_field == 'CstActivityID':
                    entry["expected"] = xml_data['Resource_ORDINAL'] + orgSubmissionId + xml_data['Mnth']
                elif xml_field == 'TotCst':
                    entry["expected"] = format(float(xml_data[xml_field]), '.8f')
                elif xml_field == 'ActCnt':
                    entry["expected"] = xml_data[xml_field]
                elif xml_field == 'OrgSubmissionID':
                    entry["expected"] = orgSubmissionId
                else:
                    entry["expected"] = xml_data[xml_field]

                if db_field == 'OrgSubmittingID' and jobType == 'activity' and xml_data['FeedType'] in ['APC', 'OP',
                                                                                                        'SWC', 'CSCC']:
                    db_field = 'OrgSubmittingId'

                if db_field == 'FinYr':
                    db_field = 'CollYear'
                    entry["expected"] = entry["expected"].replace('FY', '').replace('-', '')

                if db_field == 'OrgId':
                    db_field = 'OrgProviderID'

                if xml_data['FeedType'] == 'IAPT':
                    if db_field == 'NHSNo':
                        db_field = 'NhsNo'
                    elif db_field == 'Postcd':
                        db_field = 'PostCd'
                    elif db_field == 'DoB':
                        db_field = 'Dob'
                    elif db_field == 'LptID':
                        db_field = 'Lptid'

                if db_field == 'ActCnt' or db_field == 'Resource_ORDINAL' or db_field == 'Activity_ORDINAL' or db_field == 'CstActivity_ORDINAL' or db_field == 'APCActivity_ORDINAL' or db_field == 'Activity_ORDINAL':
                    entry["actual"] = str(int(db_data._get_value(db_data.index.get_loc(db_field), db_field)))
                elif db_field == 'TotCst':
                    entry["actual"] = format(float(db_data._get_value(db_data.index.get_loc(db_field), db_field)),
                                             '.8f')
                elif db_field == 'AttID' and xml_data['FeedType'] == 'EC':
                    entry["actual"] = db_data['AEATTENDNO']
                elif db_field == 'AttID' and xml_data['FeedType'] == 'OP':
                    entry["actual"] = db_data['ATTENDID']
                else:
                    entry["actual"] = db_data._get_value(db_data.index.get_loc(db_field), db_field)

                if entry["expected"] == "" or entry["expected"] is None or entry["expected"] == 'None':
                    entry["expected"] = 'NULL'

                if entry["actual"] == 'NULL' or entry["actual"] is None or entry["actual"] == 'None':
                    entry["actual"] = 'NULL'

                if entry["actual"] == entry["expected"]:
                    totalPassed += 1
                else:
                    totalFailed += 1
                results.append(entry)
        return {"totalPassed": totalPassed, "totalFailed": totalFailed, "tests": results}
