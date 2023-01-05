import pytest
from pytest_bdd import scenario, given, when, then, parsers, scenarios
from utils import XmlExtractor, ReportUtil, FileUtil, CsvUtil, MappingUtil, SqliteUtil, CommonUtil

scenarios('../features/weekly.feature')

def test_successful_process_of_orgid():
    pass


@pytest.fixture(scope="session")
def name(pytestconfig):
    return pytestconfig


@pytest.fixture(scope='function')
def context():
    return {}


# feed type step
@given(parsers.parse('feed type {feedType}, batch number {batch_number} and OrgSubmissionID {OrgSubmissionID}'))
def step_imp_feed_type(context, feedType, batch_number, OrgSubmissionID):
    context['feed_type'] = feedType
    context['batch_number'] = batch_number
    context['OrgSubmissionID'] = OrgSubmissionID

    # get XML files for feed type from feedData.csv
    fileUtil = FileUtil.FileUtil(context['feed_type'])
    xml_files = fileUtil.get_feed_xml()
    xmlExtractor = XmlExtractor.XmlExtractor()

    data = []
    for file_name in xml_files:
        # todo: get OrgSubmissionID from Audit table
        xmlExtractor.extract_xml_file(file_name)
        # extract data and insert into memory db
        if context['feed_type'] not in ['INTREC', 'AMBREC', 'IAPTREC']:
            xmlExtractor.process_xml_data(OrgSubmissionID)
        else:
            print("Extracting REC file .....")
            xmlExtractor.process_intrec(OrgSubmissionID)
        data.append({"file_name": file_name, "OrgSubmissionID": OrgSubmissionID})

    context["xml_data"] = data

@pytest.mark.xfail(strict=True, msg="Activity data does match expected value")
@then('the output CSV file should be correctly produced')
def step_impl_validate_csv_output(context):
    jobType = "output"
    feedType = context['feed_type']
    fileUtil = FileUtil.FileUtil(context['feed_type'])
    csvUtil = CsvUtil.CsvUtil()
    mappingUtil = MappingUtil.MappingUtil()
    sqliteutil = SqliteUtil.SqliteUtil()
    commonUtil = CommonUtil.CommonUtil()
    feed_type = context['feed_type'].lower()
    suffixes = {"Activity": "act", "Cost": "cost"}
    # import output CSV
    data_types = ["Activity", "Cost"]
    for data_type in data_types:
        file_name = fileUtil.get_file_path(data_type, context['batch_number'])
        csvUtil.import_csv(feed_type, file_name, suffixes[data_type])


    activityFields = mappingUtil.get_field_mappings(feedType.upper(), jobType, "activity")
    costFields = mappingUtil.get_field_mappings(feedType.upper(), jobType, "cost")

    left_act_table_name = f"xml_{feedType.lower()}_act"
    right_act_table_name = f"csv_{feedType.lower()}_act"
    left_cost_table_name = f"xml_{feedType.lower()}_cost"
    right_cost_table_name = f"csv_{feedType.lower()}_cost"

    activity_results = []
    cost_results = []

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

    # verify data for each file
    for item in context["xml_data"]:

        OrgSubmissionID = item["OrgSubmissionID"]
        left_query = f"SELECT * FROM {left_act_table_name} WHERE OrgSubmissionID = '{OrgSubmissionID}' {orderBy} "
        right_query = f"SELECT * FROM {right_act_table_name}"

        expected_activities = sqliteutil.select_data(left_query)
        actual_activities = sqliteutil.select_data(right_query)

        assert int(len(actual_activities)) == int(len(expected_activities)),\
            "Row counts do not match: expected {}, got {}".format(len(expected_activities), len(actual_activities))

        # compare activities
        for index in range(len(expected_activities)):
            left_side_data = expected_activities[index]
            right_side_data = actual_activities[index]
            results = commonUtil.compare_single_row(left_side_data, right_side_data, activityFields, "weekly")

            # save result for report
            activity_results.append(commonUtil.iteration_result_report(results["tests"]))

        # compare costs for none REC feed types
        orderBy = """ORDER BY
        OrgSubmittingID ASC,
        OrgId ASC,
        OrgSubmissionID ASC,
        CAST(Activity_ORDINAL AS INTEGER) ASC
        """
        left_query = f"SELECT * FROM {left_cost_table_name} WHERE OrgSubmissionID = '{OrgSubmissionID}' {orderBy}"
        right_query = f"SELECT * FROM {right_cost_table_name}"
        expected_costs = sqliteutil.select_data(left_query)
        actual_costs = sqliteutil.select_data(right_query)

        for index in range(len(expected_costs)):
            left_side_data = expected_costs[index]
            right_side_data = actual_costs[index]
            # compare costs
            # iapt mixed economy hack
            #month = int(item.file_name.split("_")[2][1:])
            results = commonUtil.compare_single_row(left_side_data, right_side_data, costFields, "weekly")

            # save result for report
            cost_results.append(commonUtil.iteration_result_report(results["tests"]))


    # print report
    reportUtil = ReportUtil.ReportUtil()
    reportUtil.printReport(activity_results, "activity", jobType, feedType)
    reportUtil.printReport(cost_results, "cost", jobType, feedType)


@pytest.mark.xfail(strict=True, msg="INTREC data does match expected value")
@then('the REC output CSV file should be correctly produced')
def step_impl_validate_rec_csv_output(context):
    jobType = "output"
    feedType = context['feed_type']
    fileUtil = FileUtil.FileUtil(context['feed_type'])
    csvUtil = CsvUtil.CsvUtil()
    mappingUtil = MappingUtil.MappingUtil()
    sqliteutil = SqliteUtil.SqliteUtil()
    commonUtil = CommonUtil.CommonUtil()
    feed_type = context['feed_type'].lower()
    file_name = fileUtil.get_file_path(feedType.lower(), context['batch_number'])
    csvUtil.import_csv(feed_type, file_name, None)


    recFields = mappingUtil.get_rec_mappings(feedType.upper(), jobType)

    left_rec_table_name = f"xml_{feedType.lower()}"
    right_rec_table_name = f"csv_{feedType.lower()}"

    rec_results = []

    # verify data for each file
    left_query = f"SELECT * FROM {left_rec_table_name}"
    right_query = f"SELECT * FROM {right_rec_table_name}"

    expected_data = sqliteutil.select_data(left_query)
    actual_data = sqliteutil.select_data(right_query)

    assert int(len(actual_data)) == int(len(expected_data)), \
        "Row counts do not match: expected {}, got {}".format(len(expected_data), len(actual_data))

    # compare rec data
    for index in range(len(expected_data)):
        left_side_data = expected_data[index]
        right_side_data = actual_data[index]
        results = commonUtil.compare_single_row(left_side_data, right_side_data, recFields, "weekly")

        # save result for report
        rec_results.append(commonUtil.iteration_result_report(results["tests"]))

    # print report
    reportUtil = ReportUtil.ReportUtil()
    reportUtil.printReport(rec_results, "", jobType, feedType)


def getMonth(file_name):
    return int(file_name.split("_")[2][1:])