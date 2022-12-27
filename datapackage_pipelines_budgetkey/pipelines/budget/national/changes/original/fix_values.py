import datetime
import logging
from decimal import Decimal

from datapackage_pipelines.wrapper import process

formats = [
    '%d/%m/%Y',
    '%Y-%m-%dT%H:%M:%S',
    '%m/%d/%y',
]

amounts = [
    'net_expense_diff',
    'gross_expense_diff',
    'allocated_income_diff',
    'commitment_limit_diff',
]

def process_row(row, *_):
    v = row.get('date')
    if v is None or v.strip() == '':
        row['date'] = None
    else:
        for fmt in formats:
            try:
                d = datetime.datetime.strptime(v, fmt).date()
                row['date'] = d
            except ValueError:
                continue

    bc = row['budget_code']
    if not bc:
        return
    bc = bc.strip()
    if len(bc) == 0:
        return
    bc = '0' * (8-len(bc)) + bc
    row['budget_code'] = bc

    for amount in amounts:
        if isinstance(row[amount], str):
            row[amount] = row[amount].replace(',', '') + '000'
        elif isinstance(row[amount], (float, Decimal)):
            row[amount] *= 1000
        else:
            logging.warning('INVALID ROW: %r' % row)
            row[amount] = 0

    return row


process(process_row=process_row)
