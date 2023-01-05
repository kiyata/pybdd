import pytest
from pytest_bdd import scenario, given, when, then, parsers, scenarios
from utils import XmlExtractor,FileUtil ,ReportUtil, DbUtil, MappingUtil, SqliteUtil, CommonUtil

scenarios('../features/daily.feature')

@pytest.fixture(scope="session")
def name(pytestconfig):
    return pytestconfig


@pytest.fixture(scope='function')
def context():
    return {}

@given(parsers.parse(
    'FeedType {feedType}, OrgSubmittingID {OrgSubmittingID} and Month {Month} and OrgSubmissionID {OrgSubmissionID} is received'))
def feed_data(context, feedType, OrgSubmittingID, Month, OrgSubmissionID):     
    context['feed_type'] = feedType
    context['Mnth'] = Month
    context['OrgSubmittingID'] = OrgSubmittingID
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
@then('the file is successfully processed and stored in Database')
def step_impl_validate_daily_processing(context):
    jobType = "extract"
    feedType = context['feed_type'].replace('"', '')
    dbUtil = DbUtil.DbUtil()
    mappingUtil = MappingUtil.MappingUtil()
    sqliteutil = SqliteUtil.SqliteUtil()
    commonUtil = CommonUtil.CommonUtil()
    feed_type = context['feed_type'].lower()
    suffixes = {"Activity": "act", "Cost": "cost"}
   
    activityFields = mappingUtil.get_field_mappings(feedType.upper(), jobType, "activity")
    costFields = mappingUtil.get_field_mappings(feedType.upper(), jobType, "cost")

    left_act_table_name = f"xml_{feedType.lower()}_act"
    left_cost_table_name = f"xml_{feedType.lower()}_cost"
    
    activity_results = []
    cost_results = []

    # verify data for each file
    for item in context["xml_data"]:
       
        OrgSubmissionID = item["OrgSubmissionID"].replace("'", "")
        left_query = f"SELECT * FROM {left_act_table_name} WHERE OrgSubmissionID = '{OrgSubmissionID}' "
        
        expected_activities = sqliteutil.select_data(left_query)
        actual_activities = dbUtil.select_db_activity_data_all(context)

        assert int(len(actual_activities)) == int(len(expected_activities)),\
            "Row counts do not match: expected {}, got {}".format(len(expected_activities), len(actual_activities))

        # compare activities
        for index in range(len(expected_activities)):
            left_side_data = expected_activities[index]
            right_side_data = actual_activities.iloc[index]        
         
            results = commonUtil.compare_single_row_daily(left_side_data, right_side_data, activityFields, "activity")

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
        #right_query = f"SELECT * FROM {right_cost_table_name}"
        expected_costs = sqliteutil.select_data(left_query)
        actual_costs = dbUtil.select_db_cost_data_all(context)
        for index in range(len(expected_costs)):
            left_side_data = expected_costs[index]
            right_side_data = actual_costs.iloc[index] 
            
            results = commonUtil.compare_single_row_daily(left_side_data, right_side_data, costFields, "cost")

            # save result for report
            cost_results.append(commonUtil.iteration_result_report(results["tests"]))


    # print report
    reportUtil = ReportUtil.ReportUtil()
    reportUtil.printReport(activity_results, "activity", jobType, feedType)
    reportUtil.printReport(cost_results, "cost", jobType, feedType)

