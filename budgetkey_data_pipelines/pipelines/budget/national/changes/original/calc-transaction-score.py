import math
import datetime
import logging
from decimal import Decimal

from datapackage_pipelines.wrapper import process


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append({
        'name': 'score',
        'type': 'number',
        'es:score-column': True
    })
    return dp

amounts = [
    ('net_expense_diff', 1000),
    ('gross_expense_diff', 1000),
    ('allocated_income_diff', 1000),
    ('commitment_limit_diff', 1000),
    ('personnel_max_diff', 0.1), # One person year counts as 10K NIS
]

def process_row(row, *_):
    sum = 0
    for field, divider in amounts:
        amount = row.get(field, 0)
        if amount is None:
            logging.warning('volume is None: %r', row)
            amount = 0
        amount /= Decimal(divider)
        sum += abs(amount)
    row['score'] = max(1, sum)
    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)