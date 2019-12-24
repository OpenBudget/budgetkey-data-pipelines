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
    start_date = row.get('start_date')
    years = 1
    if start_date and isinstance(start_date, datetime.date):
        years = (datetime.date.today() - start_date).days / 365
    row['score'] = 30 - years
    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)
