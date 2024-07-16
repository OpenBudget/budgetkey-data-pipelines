import math
import datetime
import logging
from datapackage_pipelines.wrapper import process

curyear = datetime.datetime.now().date().year + 1

def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append({
        'name': 'score',
        'type': 'number',
        'es:score-column': True
    })
    return dp


def process_row(row, *_):
    amount = row.get('amount_approved') or 0
    # Account for relevance
    row['score'] = max(1, amount / 1000)
    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)