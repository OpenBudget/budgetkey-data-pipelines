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
    amount = row.get('net_allocated', 0)
    if amount is None:
        logging.warning('net_allocated is None: %r', row)
        amount = 0
    year_score = (row['year'] - 1990) / 3.0
    row['score'] = math.log(max(1, amount)) + year_score
    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)