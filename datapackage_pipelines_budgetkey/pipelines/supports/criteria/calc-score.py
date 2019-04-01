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
    row['score'] = 10
    start_date = row.get('start_date')
    if start_date and isinstance(start_date, datetime.date):
        row['score'] += (start_date - datetime.date(year=2010, month=1, day=1)).days

    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)
