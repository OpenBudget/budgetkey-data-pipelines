import re
import datetime
import logging
from decimal import Decimal

from datapackage_pipelines.wrapper import process


def process_row(row, *_):
    start_date = row.get('start_date')
    order_date = row.get('order_date')
    end_date = row.get('end_date')
    years = []
    if start_date: years.append(start_date.year)
    if order_date: years.append(order_date.year)
    if end_date: years.append(end_date.year)
    row['min_year'] = min(years) if years else None
    row['max_year'] = max(years) if years else None
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
