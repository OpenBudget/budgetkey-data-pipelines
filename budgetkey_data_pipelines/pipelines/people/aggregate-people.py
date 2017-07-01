import logging

from datapackage_pipelines.wrapper import process


def process_row(row, *_):
    details = dict((k, v) for k, v in row.items() if k != 'key')
    row = {
        'key': row['key'],
        'details': details
    }
    return row


def modify_datapackage(dp, *_):
    inner_schema = {
        'fields': []
    }
    schema = dp['resources'][0]['schema']
    for field in schema['fields']:
        if field['name'] != 'key':
            inner_schema['fields'].append(field)

    schema['fields'] = [
        {'name': 'key', 'type': 'string'},
        {'name': 'details', 'type': 'object'}
    ]

    logging.info('Aggregate-people details inner schema: %r', inner_schema)
    return dp


process(modify_datapackage=modify_datapackage,
        process_row=process_row)

