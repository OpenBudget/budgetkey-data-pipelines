from decimal import Decimal
from datapackage_pipelines.wrapper import process

def process_row(row, *_):
    ents = {}
    for contract in row['contracts']:
        erec = ents.setdefault(
            contract['entity_id'],
            dict(
                entity_id=contract['entity_id'],
                entity_name=contract['entity_name'],
                entity_kind=contract['entity_kind'],
                volume=Decimal(0),
                executed=Decimal(0),
            )
        )
        erec['volume'] += contract.get('volume', 0)
        erec['executed'] += contract.get('executed', 0)
    ents = list(sorted(ents.values(), key=lambda x: x['volume'], reverse=True))
    row['awardees'] = ents
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([
        {
            'name': 'awardees',
            'type': 'array',
            'es:itemType': 'object',
            'es:schema': {
                'fields': [
                    {'name': 'entity_id', 'type': 'string'},
                    {'name': 'entity_name', 'type': 'string'},
                    {'name': 'entity_kind', 'type': 'string'},
                    {'name': 'volume', 'type': 'number'},
                    {'name': 'executed', 'type': 'number'},
                ]
            }
        }
    ])
    return dp

if __name__=='__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)