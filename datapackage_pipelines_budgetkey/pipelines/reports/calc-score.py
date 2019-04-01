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
    report = row.get('details',{}).get('report',{}).get('total',{})
    count1 = report.get('total_amount')
    count2 = report.get('count')
    row['score'] += count1 or count2
    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)
