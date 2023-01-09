import sys
import pytest
from pytest_bdd import scenario, given, when, then, parsers, scenarios
from utils import BaseUtils
import pandas as pd
import traceback

scenarios('../features/metadata.feature')

@pytest.fixture(scope="session")
def name(pytestconfig):
    return pytestconfig


@pytest.fixture(scope='function')
def context():
    return {}

@given(parsers.parse('Metadata spreadsheet {metadatata_file}'))
def given_spreadsheet(context, metadatata_file):     
    context['metadatata_file'] = metadatata_file


@given(parsers.parse('Table {table}'))
def given_table(context, table):     
    context['table'] = table

@pytest.mark.xfail(strict=True, msg="Metadata has some errors")
@then('The metadata should be correctly created')
def step_impl_validate_metadata(request, context):
    baseUtils = BaseUtils.BaseUtils()
    metadata_file_name = context['file_name']
    table_name = context['table']

    # read meatadata spreadsheet
    df_metadata = pd.read_excel(f"resources/{metadata_file_name}", sheet_name='metadata')

    # read test data 
    df_test_data = pd.read_csv(f"resources/test_data.csv")
    # select row for the table
    table_config = df_test_data.loc[df_test_data['table'] == table_name]

    # read table data 
    df_table_data = pd.read_csv(f"resources/{table_name}.csv")

    # verify table columns data in metadata
    seq = 1
    for field_data in df_table_data:
        column = field_data[1],
        data_type = field_data[3]
        data_type_length = field_data[3]

        row = df_metadata.loc[df_metadata['landing_table_name'] == table_name.upper() & df_metadata['epidd_field'] == column]
        test_result = "OK"
        try:
            assert len(row) == 1
            assert row['epidd_data_type'] == data_type
            assert row['oracle_data_type'] == baseUtils.get_data_type(data_type, data_type_length)
        
            # todo: add validation of rest of fields
        except AssertionError:
            test_result = "KO"
            _, _, tb = sys.exc_info()
            traceback.print_tb(tb) # Fixed format
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]
            print('An error occurred on line {} in statement {}'.format(line, text))

        request.config.cache.set('seq', seq)
        request.config.cache.set('table', table_name)
        request.config.cache.set('field', column)
        request.config.cache.set('dataType', data_type)
        request.config.cache.set('result', test_result)
        seq +=  1

    # verify generic fields
    generic_fields = pd.read_csv(f"resources/generic_fields.csv")
    tier = table_config['tier']
    prefix = baseUtils.get_xpath_depth[tier]

    for field in generic_fields:
        field_name = field[0]
        xml_path = f"{prefix}{field_name}"
        data_type = field_data[3]
        data_type_length = field_data[3]

        row = df_metadata.loc[df_metadata['landing_table_name'] == table_name.upper() & df_metadata['xml_column_relative_xpath'] == xml_path]
        test_result = "OK"
        try:
            assert len(row) == 1
        except AssertionError:
            test_result = "KO"
            _, _, tb = sys.exc_info()
            traceback.print_tb(tb) # Fixed format
            tb_info = traceback.extract_tb(tb)
            filename, line, func, text = tb_info[-1]
            print('An error occurred on line {} in statement {}'.format(line, text))
        
        request.config.cache.set('seq', seq)
        request.config.cache.set('table', table_name)
        request.config.cache.set('field', column)
        request.config.cache.set('dataType', data_type)
        request.config.cache.set('result', test_result)
        seq +=  1


   

