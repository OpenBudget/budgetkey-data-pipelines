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
    amount = row.get('received_amount', 0)
    if amount is None:
        logging.warning('received_amount is None: %r', row)
        amount = 0
    row['score'] = max(1, amount/1000)
    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)