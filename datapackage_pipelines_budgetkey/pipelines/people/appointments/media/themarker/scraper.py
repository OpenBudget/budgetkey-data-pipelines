import logging
import datetime

from datapackage_pipelines.wrapper import process

UNIFORM_FORMAT = '%d.%m.%y'
DATE_COLUMN = 'date'
SOURCE_COLUMN = 'source'


def modify_datapackage(datapackage, parameters, stats):
    logging.info('datapackage: %s' % datapackage)
    datapackage['resources'][0]['schema']['fields'].append({
      'name': SOURCE_COLUMN,
      'type': 'string'
    })
    logging.info('second datapackage: %s' % datapackage)
    return datapackage


def process_row(row, row_index,
                resource_descriptor, resource_index,
                parameters, stats):
    row_date = row[DATE_COLUMN]
    try:
        row[DATE_COLUMN] = convert_to_uniform_date(row_date)
    except ValueError as e:
        # this is necessary as there are some invalid dates in the spread sheet
        logging.warning(
            'Failed to convert: %s to a uniform date, Removing it. Full error: %s' % (row_date, e)
        )
        del row[DATE_COLUMN]

    row[SOURCE_COLUMN] = 'the-marker'
    return row


def convert_to_uniform_date(raw_date):
    try:
        return datetime.datetime.strptime(raw_date, '%Y-%m-%dT00:00:00').date()
    except ValueError:
        return datetime.datetime.strptime(raw_date, UNIFORM_FORMAT)


process(process_row=process_row, modify_datapackage=modify_datapackage)
