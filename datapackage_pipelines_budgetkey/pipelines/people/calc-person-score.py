import datetime
import logging
from datapackage_pipelines.wrapper import process

old = datetime.datetime.fromtimestamp(0)

def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([{
        'name': 'score',
        'type': 'number',
        'es:score-column': True
    }, {
        'name': 'person_key',
        'type': 'string',
        'es:title': True
    }])
    return dp


def start_date(x):
    ret = x.get('start_date')
    if not ret:
        ret = old
    return ret


def process_row(row, *_):
    row['score'] = 1
    row['person_key'] = row['key']
    row['details'] = sorted(row['details'], key=start_date)
    if len(row['details']) > 50:
        logging.error('VERY LONG LIST FOR %s: %d items',
                      row['person_key'], len(row['details']))
    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)