import json
import pytest

from datapackage_pipelines_budgetkey.pipelines.education.tochniyot.scraper import TAARICH_STATUS, TaarichStatus_to_date, \
    send_tochniyot_request

TEST_DATA_CSV_FILE = 'data/education_programs/res_1.csv'


def test_TaarichStatus_to_date():
    """
    test the conversion from JS date format to readable date format
    :return:
    """
    with open('data/education_programs/original_data.json') as json_data:
        orig_data = json.load(json_data)

    for row in orig_data:
        TaarichStatus_to_date(row)
        assert row[TAARICH_STATUS] == '01/01/0001 00:00:00'


def test_tochniyot_url_validity():
    """
    Send a request to the data url to check if the the data is in the expected url
    :return:
    """
    assert send_tochniyot_request(1)

# def test_data_columns_exist():
#     json_data = send_tochniyot_request(1)
#     assert
