from decimal import Decimal

from datapackage_pipelines.wrapper import process
from slugify import slugify


def process_row(row, *_):
    amount_total = 0
    payments = []
    for deet in row['payments']:
        new_deet = {}
        amount_total += deet['amount_total'] if deet['amount_total'] is not None else 0
        for k, v in deet.items():
            if isinstance(v, Decimal):
                v = float(v)
            new_deet[k] = v
        payments.append(new_deet)
    row['payments'] = payments
    row['amount_total'] = amount_total
    row['short_id'] = (row['entity_id'] or slugify(row['recipient'], max_length=32)).strip()
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append({
        'name': 'amount_total',
        'type': 'number'
    })
    dp['resources'][0]['schema']['fields'].append({
        'name': 'short_id',
        'type': 'string',
        'es:index': False
    })
    return dp

process(modify_datapackage=modify_datapackage,
        process_row=process_row)