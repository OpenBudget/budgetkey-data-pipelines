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
    row = {TAARICH_STATUS: '/Date(23424234234234)/'}
    TaarichStatus_to_date(row)
    assert row[TAARICH_STATUS] == '14/04/2712 19:43:54'

    row = {TAARICH_STATUS: '/Date(-62135596800000)/'}
    TaarichStatus_to_date(row)
    assert row[TAARICH_STATUS] == '01/01/0001 00:00:00'


def test_tochniyot_url_validity():
    """
    Send a request to the data url to check if the the data is in the expected url
    :return:
    """
    assert send_tochniyot_request(1)
