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
    amount = row.get('net_revised', 0)
    if amount is None:
        logging.warning('net_revised is None: %r', row)
        amount = 0
    # Account for relevance
    # year_score = 2**abs(curyear - row['year'])
    # amount /= year_score
    # # Account for depth
    # depth = len(row['code'])/2
    # amount /= depth
    row['score'] = max(1, amount / 1000)
    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)