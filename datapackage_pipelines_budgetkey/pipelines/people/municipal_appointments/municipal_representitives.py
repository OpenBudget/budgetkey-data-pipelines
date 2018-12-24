from datetime import datetime

import dataflows
from datapackage_pipelines.utilities.resources import PROP_STREAMING

URL = 'https://www.odata.org.il/dataset/8b8d76ad-c54a-4b8a-a1ef-fc8fe3f5decc/resource/fa081b66-4c64-43fa-9086-cac8d8f381b7/download/siot_rashut.xls'
MUNICIPAL_ELECTION_DATE = datetime(2013, 10, 22)
TITLE = 'municipal_elected'

def add_fields_to_schema(package):
    package.pkg.descriptor['resources'][0]['schema']['fields'].extend([
        {'name': 'full_name', 'type': 'string'},
        {'name': 'company', 'type': 'string'},
        {'name': 'date', 'type': 'date'},
        {'name': 'title', 'type': 'string'},
        {'name': 'event', 'type': 'string'},
    ])
    package.pkg.descriptor['resources'][0][PROP_STREAMING] = True

    yield package.pkg
    yield from package


def convert_excel_row_to_people_row(row):
    return {
        'full_name': row['שם'],
        'company': '{}: {}'.format(row['שם ישוב'], row['כינוי']),
        'date': MUNICIPAL_ELECTION_DATE,
        'title': 'municipal_party_elected_{}'.format(int(row["מס' סידורי"])),
        'event': 'municipal elections',
    }
    # municipality: שם ישוב
    # municipal party: כינוי

def flow(parameters, datapackage, resources, stats):
    return dataflows.Flow(
        dataflows.load(URL, format='xls'),
        add_fields_to_schema,
        convert_excel_row_to_people_row,
    )
