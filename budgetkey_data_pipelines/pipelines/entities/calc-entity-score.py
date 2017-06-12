import math

import logging
from datapackage_pipelines.wrapper import process


def process_row(row, *_):
    amount = row.get('score', 0)
    if amount is None:
        amount = 0
    row['score'] = math.log(max(1, amount))
    return row

process(process_row=process_row)