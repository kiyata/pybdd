import json
import pytest
from py.xml import html

metadata_table_fields = ""

def pytest_addoption(parser):
    parser.addoption("--table", action="store")

def pytest_configure(config):
    config._metadata = None


def pytest_runtest_setup(item):
    pass


def pytest_html_report_title(report):
    """ modifying the title  of html report"""
    report.title = "Metadata Spreadsheet Test Report"


def pytest_html_results_table_header(cells):
    """ meta programming to modify header of the result"""

    # removing old table headers
    del cells[1]
    # adding new headers
    cells.insert(0, html.th('Seq'))
    cells.insert(1, html.th('Table'))
    cells.insert(2, html.th('Field'))
    cells.insert(3, html.th('Data type'))
    cells.insert(4, html.th('Result'))
    cells.pop()


def pytest_html_results_table_row(report, cells):
    """ orienting the data gotten from  pytest_runtest_makereport
    and sending it as row to the result """
    del cells[1]
    cells.insert(0, html.td(getattr(report, "seq", '')))
    cells.insert(1, html.td(getattr(report, "table", '')))
    cells.insert(2, html.td(getattr(report, "field", '')))
    cells.insert(3, html.td(getattr(report, "dataType", '')))
    cells.insert(4, html.td(getattr(report, "result", '')))
    cells.pop()


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item):
    """data from the output of pytest gets processed here
     and are passed to pytest_html_results_table_row"""
    outcome = yield
    # this is the output that is seen end of test case
    report = outcome.get_result()
    
    # work around. pytest reports more tests than available and the last one
    # will throw error when extracting test title
    report.seq = item.session.config.cache.get('SEQ', None)
    report.table = item.session.config.cache.get('TABLE', None)
    report.field = item.session.config.cache.get('FIELD', None)
    report.datatype = item.session.config.cache.get('DATATYPE', None)
    report.result = item.session.config.cache.get('RESULT', None)
