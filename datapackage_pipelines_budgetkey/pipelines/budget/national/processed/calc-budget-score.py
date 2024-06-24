import math
import datetime
import logging
from datapackage_pipelines.wrapper import process

curyear = datetime.datetime.now().date().year

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
    rowyear = curyear - 10
    rowyear = row.get('year', rowyear)
    diff = curyear - rowyear
    diff = (max(1, diff) - 1) / 3
    diff_score = 2**(diff)
    row['score'] = max(1, amount / 1000) / diff_score
    code = row.get('code', '')
    if code.startswith('0000') or code.startswith('C8'):
        row['score'] /= 2
    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)
