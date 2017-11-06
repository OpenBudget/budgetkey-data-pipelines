import re
import datetime
import logging
from decimal import Decimal

from datapackage_pipelines.wrapper import process


def process_row(row, *_):
    start_date = row.get('start_date')
    end_date = row.get('end_date')
    row['min_year'] = start_date.year if start_date else None
    row['max_year'] = end_date.year if end_date else None
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([
        {
            'name': 'min_year',
            'type': 'integer'
        },
        {
            'name': 'max_year',
            'type': 'integer'
        }
    ])
    return dp


process(modify_datapackage=modify_datapackage,
        process_row=process_row)
