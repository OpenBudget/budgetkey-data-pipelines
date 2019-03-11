import logging
from datapackage_pipelines.wrapper import process


def modify_datapackage(datapackage, *_):
    fields = datapackage['resources'][0]['schema']['fields']
    date_field = next(filter(lambda f: f['name']=='date', fields))
    date_field['type'] = 'date'

    return datapackage


def process_row(row, *_):
    row['date'] = row['date'].date()
    return row

process(process_row=process_row, modify_datapackage=modify_datapackage)
