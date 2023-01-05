import pytest
from pytest_bdd import scenario, given, when, then, parsers, scenarios
from utils import XmlExtractor, ReportUtil, FileUtil, CsvUtil, MappingUtil, SqliteUtil, CommonUtil
import csv
import os
from decouple import config

scenarios('../features/matching.feature')

def test_successful_process_of_orgid():
    pass

@pytest.fixture(scope="session")
def name(pytestconfig):
    return pytestconfig

@pytest.fixture(scope='function')
def context():
    return {}

@given(parsers.parse('feed type {feed_type}, batch number {batch_number}, xml file {xml_file_name}, matching data {matching_data_file} and OrgSubmissionID {OrgSubmissionID}'))
def step_imp_feed_type(context, feed_type, batch_number, xml_file_name, matching_data_file, OrgSubmissionID):
    context['feed_type'] = feed_type
    context['batch_number'] = batch_number
    context['OrgSubmissionID'] = OrgSubmissionID

    xmlExtractor = XmlExtractor.XmlExtractor()
    xmlExtractor.extract_xml_file(xml_file_name)
    xmlExtractor.process_xml_data(OrgSubmissionID)

    # todo:import and save to db
    file_path = f"{config('LOCAL_RESOURCE_DIR')}/matching/{matching_data_file}"
    #print(file_path)
    data = csv.DictReader(open(file_path, 'r'))
    # print(data)
    # print(list(data))
    context['matching_data'] = list(data)

@pytest.mark.xfail(strict=True, msg="Incorrect mathing value")
@then('the matching value in CSV should be correct')
def step_impl_validate_matching(context):
    jobType = "output"
    feedType = context['feed_type']
    fileUtil = FileUtil.FileUtil(context['feed_type'])
    csvUtil = CsvUtil.CsvUtil()
    sqliteutil = SqliteUtil.SqliteUtil()
    commonUtil = CommonUtil.CommonUtil()
    feed_type = context['feed_type'].lower()
    mappingUtil = MappingUtil.MappingUtil()

    matchingFields = mappingUtil.get_matching_mappings(feedType.upper())
    # import output CSV
    file_name = fileUtil.get_file_path("Activity", context['batch_number'])
    csvUtil.import_csv(feed_type, file_name, "act")

    left_act_table_name = f"xml_{feedType.lower()}_act"
    right_act_table_name = f"csv_{feedType.lower()}_act"

    hesMatchingFields = {
        "APC": "CDSID",
        "CSCC": "CareId",
        "EC": "CDSID",
        "IAPT": "CareId",
        "MHCC": "CareId",
        "MHPS": "HSpellNo",
        "OP": "CDSID"
    }

    keyField = hesMatchingFields[feedType]

    # verify data
    orderBy = """ORDER BY
    OrgSubmittingID ASC,
    OrgId ASC,
    OrgSubmissionID ASC,
    """
    if feedType == 'APC':
        orderBy += "CAST(APCActivity_ORDINAL AS INTEGER) ASC"
    else:
        orderBy += "CAST(Activity_ORDINAL AS INTEGER) ASC"


    OrgSubmissionID = context["OrgSubmissionID"]
    left_query = f"SELECT * FROM {left_act_table_name} WHERE OrgSubmissionID = '{OrgSubmissionID}' {orderBy} "
    right_query = f"SELECT * FROM {right_act_table_name}"

    #print(left_query)
    expected_activities = sqliteutil.select_data(left_query)
    actual_activities = sqliteutil.select_data(right_query)
    matching_data = context['matching_data']
    # compare activities
    results = []
    totalPassed = 0
    totalFailed = 0

    for index in range(len(expected_activities)):
        left_side_data = expected_activities[index]
        right_side_data = actual_activities[index]

        matching_entry = matching_data[index] # we are making dangerous assumption that activity sequence does not change
        matchCriteria = matching_entry['matchCriteria']
        expectedMatchCode = matching_entry['matchCode']
        actualMatchCode = right_side_data['RecordIdentifierMatched']
        epiKey = matching_entry['epiKey']

        if expectedMatchCode != actualMatchCode:
            print("expected :" + expectedMatchCode)
            print("actual :" + actualMatchCode)

        result = {"ActivityRowID": matching_entry['Activity_ORDINAL'],
                  "matchCriteria": matching_entry['matchCriteria'],
                  "matchCode": right_side_data['RecordIdentifierMatched'],
                  "result": "OK" if expectedMatchCode == actualMatchCode else 'KO'}

        if expectedMatchCode == actualMatchCode:
            totalPassed += 1
        else:
            totalFailed += 1

        plicsIdentifierMatched = matching_entry['epiKey']
        if actualMatchCode in ["30", "40"] and feedType in ["APC", "EC", "OP"]:
            if left_side_data[keyField] is not None and left_side_data[keyField] != '':
                to_be_hashed = left_side_data[keyField]
                saltedValue = to_be_hashed + os.getenv("GENERICSALT")
                plicsIdentifierMatched = commonUtil.hashing(saltedValue, "sha256")
            else:
                plicsIdentifierMatched = epiKey

        result["identifier"] = plicsIdentifierMatched

        matchedValues = ""
        if actualMatchCode in ["10", "40"]:
            if feedType == "MHCC":
                matchedValues = left_side_data["OrgId"] + left_side_data["SerReqID"] + left_side_data["CareId"]
            elif feedType == "MHPS":
                matchedValues = left_side_data["OrgId"] + left_side_data["SerReqID"] + left_side_data["HSpellId"]
            elif feedType == "IAPT":
                matchedValues = left_side_data["OrgId"] + left_side_data["CareConID"]
            else:
                matchedValues = left_side_data[matchingFields[matchCriteria]]
        elif actualMatchCode == "30":
            result["matchCriteria"] = "None"
            matchedValues = "No match"
        else:
            criteria = matchCriteria.split(":")
            print(criteria)
            if len(criteria) > 1:
                for criterion in criteria:
                    # print(criteria)
                    # print(matchingFields[criteria])
                    d = left_side_data[matchingFields[criterion]]
                    if d is not None and d != "":
                        matchedValues += d + ":"
                matchedValues = matchedValues[:-1]
            else:
                matchedValues = criteria[0]
        result["matchedValues"] = matchedValues
        results.append(result)
    # print report
    #print(results)
    reportUtil = ReportUtil.ReportUtil()
    reportUtil.printMatchingReport(results, feedType, totalPassed, totalFailed)
