import math

import logging
from datapackage_pipelines.wrapper import process


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append({
        'name': 'score',
        'type': 'number',
        'es:score-column': True
    })
    return dp


def process_row(row, *_):
    row['score'] = 1
    row['details'] = sorted(row['details'], key=lambda i: i.get('start_date'))
    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)