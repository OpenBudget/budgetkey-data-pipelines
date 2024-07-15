from decimal import Decimal

from datapackage_pipelines.wrapper import process
from slugify import slugify


def process_row(row, *_):
    row['short_id'] = (row['entity_id'] or slugify(row['recipient'], max_length=32)).strip()
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append({
        'name': 'short_id',
        'type': 'string',
        'es:index': False
    })
    return dp

process(modify_datapackage=modify_datapackage,
        process_row=process_row)