import datetime
import logging

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
    for k in list(row.keys()):
        if k == 'date':
            v = row[k]
            if v is None or v.strip() == '':
                row[k] = None
                continue
            for fmt in formats:
                try:
                    d = datetime.datetime.strptime(v, fmt).date()
                    row[k] = d
                except ValueError:
                    continue

    k = 'budget_code'
    v = row[k].strip()
    if len(v) == 0:
        logging.error('BAD row')
        return
    v = '0' * (8-len(v)) + v
    row[k] = v

    for amount in amounts:
        if isinstance(row[amount], str):
            row[amount] += '000'
        else:
            row[amount] *= 1000

    return row


process(process_row=process_row)
