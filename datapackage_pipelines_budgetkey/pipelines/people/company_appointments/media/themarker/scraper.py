import logging
import datetime

from datapackage_pipelines.wrapper import process

DATE_FORMATS = [
    '%Y-%m-%dT00:00:00',
    '%d.%m.%y',
    '%d/%m/%y'
]

DATE_COLUMN = 'date'
SOURCE_COLUMN = 'source'
PROOF_COLUMN = 'proof_url';

#
FIX_BAD_DATE = [{'from':'2/3/217', 'to':'2/3/17'}]


def modify_datapackage(datapackage, *_):
    fields = datapackage['resources'][0]['schema']['fields']

    date_field = next( (f for f in fields if f['name']==DATE_COLUMN))
    date_field['type'] = 'date'

    fields.extend([
        {'name': SOURCE_COLUMN,'type': 'string'},
        {'name': PROOF_COLUMN, 'type': 'string'}
    ])
    return datapackage


def process_row(row, *_):
    row_date = row[DATE_COLUMN]

    fix_date = [it['to'] for it in FIX_BAD_DATE if it['from'] == row_date]
    if len(fix_date) > 0:
        row_date = fix_date[0]

    try:
        row[DATE_COLUMN] = convert_to_uniform_date(row_date)
    except ValueError as e:
        # this is necessary as there are some invalid dates in the spread sheet
        logging.warning(
            'Failed to convert: %s to a uniform date, Removing it. Full error: %s' % (row_date, e)
        )
        del row[DATE_COLUMN]

    row[SOURCE_COLUMN] = 'the-marker'
    row[PROOF_COLUMN] = 'https://www.themarker.com/career/EXT-1.2577328'
    return row


def convert_to_uniform_date(raw_date):

    for format in DATE_FORMATS:
        try:
            return datetime.datetime.strptime(raw_date, format).date()
        except ValueError:
            pass
    raise ValueError("Failed to convert date using all available date formats")

process(process_row=process_row, modify_datapackage=modify_datapackage)
