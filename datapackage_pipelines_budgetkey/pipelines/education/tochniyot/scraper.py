import json
import logging
import re
from datetime import datetime

import requests
from dataflows import Flow, set_type, update_resource

DEBUG = False  # limit the number of records retrieved by the scraper

# data columns
TAARICH_STATUS = 'TaarichStatus'
TAARICH_IDKUN_RESHUMA = 'TaarichIdkunReshuma'

LONG_DATE_FORMAT = '%d/%m/%Y %H:%M:%S'
SHORT_DATE_FORMAT = '%d/%m/%Y'
short_date_regex = re.compile('[0-9]{2}\/[0-9]{2}\/[0-9]{4}')
long_date_regex = re.compile('[0-9]{2}\/[0-9]{2}\/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2}')

# data source url and parameters
tochniyot_url = 'https://apps.education.gov.il/TyhNet/ClientWs/TochnitCh.asmx/IturTochnitChByMeafyenim'
tochniyot_search_params = {"MeafyeneiTochnit": "",
                           "strTchumMerkazi": "",
                           "strMakorTochnit": "",
                           "strStatusTochnit": "4,7",
                           "CodeYechidaAchrayit": -1,
                           "Cipuschofshi": "",
                           "MisparTochnit": -1,
                           "TochnitBelivuiMisrad": -1,
                           "kyamDerugMenahalim": -1,
                           "KyemotChavotDaatMefakchim": -1,
                           "Natsig": "",
                           "ShemTochnit": "",
                           "Taagid": "",
                           "TaagidMishtamesh": "",
                           "TochnitActive": False,
                           "NidrashAlut": -1,
                           "PageNumber": 1,
                           # "PageSize": 5045,
                           "OrderBy": "",
                           "IncludeYechidotBat": True,
                           "TochnitLeumit": -1}

records_count_field_name = 'PageSize'  # the number of tochnyiot in a single url request
education_programs_count_field_name = 'TotalResultCount'  # the field name in the response json


def get_education_programs_count():
    """
    Get the number of education programs listed in on the site.
    :return: number of education
    """
    if DEBUG:  # return a limited number of records
        return 10

    res = send_tochniyot_request()
    return res['d'][education_programs_count_field_name]


def scrape():
    """
    Get all the education programs from tochniyot_url
    :return: the education programs data
    """
    education_programs_count = get_education_programs_count()
    json_data = send_tochniyot_request(education_programs_count)
    tochniyot_data = json_data['d']['Data']  # returns an array of all the education programs
    return tochniyot_data


def send_tochniyot_request(records_num=1):
    """
    Sends tochniyot_url to get education programs data
    :param records_num: the number of tochniyot to fetch.
    :return: the json data in the response. Contains the programs data and
    some metadata about it (e.g. total number of programs)
    """
    tochniyot_search_params[records_count_field_name] = records_num
    resp = requests.post(tochniyot_url, json=tochniyot_search_params)
    resp.raise_for_status()

    if resp.status_code != 200:
        logging.error('Failed request: could not get tochnyiot data.')
    json_data = json.loads(resp.text)
    return json_data


def TaarichStatus_to_date(row):
    """
    transform a JS format date to a date parsable by dataflows
    :param row: row of data
    :return: a row with parsable date
    """
    js_date_str = row[TAARICH_STATUS]  # TaarichStatus format: /Date(<some long integer>)/
    js_date_integer_only = int(js_date_str.split('(')[1].split(')')[0])  # extracts the integer from js_date_str
    try:
        row[TAARICH_STATUS] = datetime.utcfromtimestamp(js_date_integer_only / 1000).strftime(LONG_DATE_FORMAT)
    except ValueError as e:
        logging.error(f"TaarichStatus column is not in the expected date format.\n{e}")


def flow(*_):
    return Flow(scrape(),
                TaarichStatus_to_date,
                set_type('MisparTochnit', type='number'),
                set_type('CodeYechidaAchrayit', type='number'),
                set_type('CodeTchumMerkazi', type='number'),
                set_type('MisparTaagid', type='number'),
                set_type('KayamNatsigMisrad', type='number'),
                set_type('TaarichStatus', type='datetime', format='any'),
                set_type('MisparMedargimLatochnit', type='number'),
                set_type('MakorTochnit', type='number'),
                set_type('MisparTaagid', type='number'),
                set_type('TaarichIdkunReshuma', type='datetime', format='any'),

                update_resource(-1, name='education_programs', **{'dpp:streaming': True}),
                # printer(num_rows=1, tablefmt='grid')
                )


if __name__ == "__main__":
    f = flow()
    f.process()

