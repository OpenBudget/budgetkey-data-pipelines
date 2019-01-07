import math
import datetime
import logging
from decimal import Decimal
from datapackage_pipelines.wrapper import process

decision_boosters = {
    'סגור': 2/15,
    'הסתיים': 1/15,
    'עתידי': 10/15,
    'פתוח': 15/15,
    'אושר פטור ממכרז': 8/15,
    'בתהליך': 8/15,
    'לא אושר': 4/15,	
}

tender_type_boosters = {
    'central': 4/4,
    'office': 2/4,
    'exemptions': 1/4
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
    amount *= Decimal(decision_boosters.get(row['simple_decision'], 1))
    amount *= Decimal(tender_type_boosters.get(row['tender_type'], 1))
    row['score'] = amount
    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)