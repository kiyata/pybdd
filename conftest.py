import csv
import os
import sqlite3
from py.xml import html
import pytest
from utils import DataFields, DbUtil, BaseUtil, SqliteUtil, MappingUtil


class Meerkat:
    def __init__(self, val):
        val = val

    def __eq__(self, other):
        return val == other.val


class Data:
    FinYr = None
    OrgId = None
    month = None
    Activity_ORDINAL = None
    feed_type = None
    OrgSubmittingID = None
    FeedType = None
    ActCsv = None
    CostCsv = None
    OrgProviderID = None
    field_name = None
    field_value = None
    result = None
    ActivityRowID = None
    OrgSubmissionID = None
    PLICSRecordIdentifier = None
    RecordIdentifierMatched = None
    OrganisationID = None
    OrgPLEMI = None
    PLICSNHSNo = None
    Age = None
    PersonStatedGender = None
    PLICSPatientPathwayID = None
    PatientPathwayOrganisationID = None
    PointOfDelivery = None
    PLICSHospProvSpell = None
    EpisodeNumber = None
    StartDateEpisode = None
    EndDateEpisode = None
    EpType = None
    TreatmentFunctionCode = None
    CFBand = None
    FCEHRG = None
    SpellHRG = None
    AdjLoS = None
    plicsDb = None
    memoryDb = "plics-qa"
    plicsActivityTable = None


class FeedData:
    Fields = None
    ChsCcy = None
    TeamType = None
    OrgProviderID = None
    Iteration = None
    PreviousField = None
    TotCst = None
    ResCstID = None
    ActCnt = None
    ActCstID = None
    FinYr = None
    Collection = None
    FeedType = None
    OrgSubmittingID = None
    Month = None
    Activity_ORDINAL = None
    OrgId = None
    CstActivity_ORDINAL = None
    Resource_ORDINAL = None


def pytest_addoption(parser):
    parser.addoption("--job", action="store", default="features.daily")
    parser.addoption("--env", action="store", default="local")


def pytest_configure(config):
    config._metadata = None


def pytest_runtest_setup(item):
    # reset in memory tables
    reset_sqlite_data()


def pytest_html_report_title(report):
    """ modifying the title  of html report"""
    report.title = "PLICS Test Report"


def pytest_html_results_table_header(cells):
    """ meta programming to modify header of the result"""

    # removing old table headers
    del cells[1]
    # adding new headers
    cells.insert(0, html.th('Collection'))
    cells.insert(1, html.th('Feature'))
    cells.insert(2, html.th('Scenario'))
    cells.insert(3, html.th('Iteration'))
    cells.insert(4, html.th('Expected'))
    cells.insert(5, html.th('Actual'))
    cells.pop()


def pytest_html_results_table_row(report, cells):
    """ orienting the data gotten from  pytest_runtest_makereport
    and sending it as row to the result """
    del cells[1]
    cells.insert(0, html.td(getattr(report, "collection", '')))
    cells.insert(1, html.td(getattr(report, "feature", '')))
    cells.insert(2, html.td(getattr(report, "scenario", '')))
    cells.insert(3, html.td(getattr(report, "iteration", '')))
    cells.insert(4, html.td(getattr(report, "expected", '')))
    cells.insert(5, html.td(getattr(report, "actual", '')))
    cells.pop()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item):
    """data from the output of pytest gets processed here
     and are passed to pytest_html_results_table_row"""
    outcome = yield
    # this is the output that is seen end of test case
    report = outcome.get_result()
    # taking doc string of the string
    testcase = str(item.function.__doc__)
    d = testcase.split(":")

    # work around. pytest reports more tests than available and the last one
    # will throw error when extracting test title
    collection = str(item.session.config.cache.get('COLLECTION', None))
    report.collection = collection.replace('"', '')
    report.expected = item.session.config.cache.get('EXPECTED', None)
    report.actual = item.session.config.cache.get('ACTUAL', None)
    report.iteration = item.session.config.cache.get('ITERATION', None)

    if len(d) > 1:
        featurePath = d[1].replace('\\', ':')
        p = featurePath.strip().split(':')
        counter = len(p)

        report.feature = p[counter - 1].split('.')[0].upper()
        report.scenario = d[2].strip()

        collection = str(item.session.config.cache.get('COLLECTION', None))
        report.collection = collection.replace('"', '')
        report.testcase = f"[{testcase}]"


def pytest_assertrepr_compare(op, left, right):
    if isinstance(left, Meerkat) and isinstance(right, Meerkat) and op == "==":
        return ['Comparing input with output:',
                '   Result: %s != %s' % (left.val, right.val)]


# clear activity and cost tables from memory databases
def reset_sqlite_data():
    feed_types = ['apc', 'ec', 'iapt', 'op', 'si', 'swc', 'cscc', 'mhcc', 'mhps']
    job_types = ["extract", "output"]
    mappingUtil = MappingUtil.MappingUtil()
    sqliteUtil = SqliteUtil.SqliteUtil()

    prefixes = {
        "extract": "xml",
        "output": "csv"
    }

    sqliteUtil.run_query(f"drop table if exists csv_intrec;")
    sqliteUtil.run_query(f"drop table if exists xml_intrec;")
    intrec_fields = ["ReconciliationID", "ReconciliationLevel", "FinAccID", "CstIncVal",
                     "SerID", "AgrAdj", "TotCst", "FinancialYearStartDate"]
    for feed_type in feed_types:
        #print(feed_type)
        # drop activity and cost tables
        sqliteUtil.run_query(f"drop table if exists xml_{feed_type}_act;")
        sqliteUtil.run_query(f"drop table if exists xml_{feed_type}_cost;")
        sqliteUtil.run_query(f"drop table if exists csv_{feed_type}_act;")
        sqliteUtil.run_query(f"drop table if exists csv_{feed_type}_cost;")
        # create activity and cost tables
        for job_type in job_types:
            activityFields = mappingUtil.get_field_mappings(feed_type.upper(), job_type, "activity")
            activityColumns = create_table_columns_from_fields(activityFields)
            costFields = mappingUtil.get_field_mappings(feed_type.upper(), job_type, "cost")
            costColumns = create_table_columns_from_fields(costFields)
            sqliteUtil.run_query(
                f"create table {prefixes[job_type]}_{feed_type}_act ({activityColumns})")
            sqliteUtil.run_query(
                f"create table {prefixes[job_type]}_{feed_type}_cost ({costColumns})")
            # if feed_type == 'mhps' and job_type == 'output':
            #     print(activityColumns)
    # create rec tables
    intrec_table_columns = ""
    for field in intrec_fields:
        intrec_table_columns += f"{field} text, "

    intrec_table_columns = intrec_table_columns[:-2]
    for key in prefixes:
        sqliteUtil.run_query(f"create table {prefixes[key]}_intrec ({intrec_table_columns})")

def create_table_columns_from_fields(fields):
    result = ""
    for key in fields:
        result += f"{fields[key]} text, "
    return result[:-2]  # remove ', '
