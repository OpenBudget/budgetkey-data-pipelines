import math
import datetime
import logging
from datapackage_pipelines.wrapper import process

decision_boosters = {
    'סגור': 2,
    'הסתיים': 2,
    'עתידי': 6,
    'פתוח': 8,
    'אושר פטור ממכרז': 8,
    'בתהליך': 8,
    'לא אושר': 4,	
}

tender_type_boosters = {
    'central': 4,
    'office': 2,
    'exemptions': 1
}

base = {
    'central': 100000,
    'office': 10000,
    'exemptions': 50
}


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append({
        'name': 'score',
        'type': 'number',
        'es:score-column': True
    })
    return dp


def process_row(row, *_):
    amount = row.get('volume') or row.get('contract_volume')
    if not amount:
        amount = base.get(row['tender_type'], 1)
    else:
        amount = amount / 1000
    amount *= decision_boosters.get(row['simple_decision'], 1)
    amount *= tender_type_boosters.get(row['tender_type'], 1)
    row['score'] = amount
    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)