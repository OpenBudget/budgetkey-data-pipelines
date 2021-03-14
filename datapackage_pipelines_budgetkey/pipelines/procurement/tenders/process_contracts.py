import os
import logging
import json
from decimal import Decimal

from datapackage_pipelines.wrapper import process

from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError, OperationalError
from sqlalchemy.sql import text

connection_string = os.environ['DPP_DB_ENGINE']
engine = create_engine(connection_string)
conn = engine.connect()
contracts = {}

distinct_tender_keys = set([
    dict(r)['tender_key'] 
    for r in conn.execute('''
        select jsonb_array_elements_text(tender_key) as tender_key 
        from contract_spending
        group by 1
    ''')
])
logging.info('Found %d distinct tender keys (in contracts)', 
             len(distinct_tender_keys))

query = text("""
select entity_name, entity_id, entity_kind, executed, volume, supplier_name, payments, contract_is_active,
       jsonb_array_elements_text(tender_key) as tender_key, order_date  from contract_spending
""")
for r in conn.execute(query):
    r = dict(r)
    key = r['tender_key']
    contracts.setdefault(key, []).append(r)

logging.info('Loaded %d contract records', 
             len(contracts))

def get_all_contracts(key):
    return contracts.get(key, [])


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([
        dict(
            name='contract_volume',
            type='number',
        ),
        dict(
            name='contract_executed',
            type='number'
        ),
        {
            'name': 'awardees',
            'type': 'array',
            'es:itemType': 'object',
            'es:schema': {
                'fields': [
                    {'name': 'entity_id', 'type': 'string', 'es:keyword': True},
                    {'name': 'entity_name', 'type': 'string'},
                    {'name': 'entity_kind', 'type': 'string', 'es:keyword': True},
                    {'name': 'active', 'type': 'boolean'},
                    {'name': 'volume', 'type': 'number'},
                    {'name': 'executed', 'type': 'number'},
                    {'name': 'payments', 'type': 'array', 
                     'es:itemType': 'object', 'es:index': False},
                    {'name': 'count', 'type': 'integer'},
                ]
            }
        }
    ])
    return dp

def process_row(row, *_):
    key_fields = ('publication_id', 'tender_type', 'tender_id')
    key = json.dumps([str(row[k]) for k in key_fields])
    all_contracts_ = get_all_contracts(key)
    all_contracts = []
    for contract in all_contracts_:
        if row.get('start_date') and contract.get('order_date'):
            if row.get('start_date') <= contract.get('order_date'):
                all_contracts.append(contract)
            continue
        all_contracts.append(contract)
    row['contract_volume'] = sum(c.get('volume', 0) for c in all_contracts)
    row['contract_executed'] = sum(c.get('executed', 0) for c in all_contracts)
    timestamps = sorted(set(p['timestamp'] for c in all_contracts for p in c['payments']))
    ents = {}
    if row.get('entity_id'):
        all_contracts.append(dict(
            entity_id=row['entity_id'],
            entity_name=row['entity_name'],
            entity_kind=row['entity_kind'],
            payments=[],
            contract_is_active=False,
            volume=row['volume']
        ))
    for contract in all_contracts:
        supplier = contract['entity_name']
        if not supplier:
            if len(contract['supplier_name'])>0:
                supplier = max(contract['supplier_name'])
            else:
                supplier = 'לא ידוע'

        executed = 0
        payments = {}
        for t in timestamps:
            for p in contract['payments']:
                if p['timestamp'] == t:
                    executed = p['executed']
                    payments[t] = executed
                    break
            if t not in payments:
                payments[t] = executed

        erec = ents.setdefault(
            supplier,
            dict(
                entity_id=contract['entity_id'],
                entity_name=supplier,
                entity_kind=contract['entity_kind'],
                active=contract['contract_is_active'],
                volume=Decimal(0),
                executed=Decimal(0),
                payments=dict((t, [t, 0]) for t in timestamps),
                count=0
            )
        )
        for t in timestamps:
            erec['payments'][t][1] += payments[t]
        erec['volume'] += contract.get('volume') or  0
        erec['executed'] += contract.get('executed') or 0
        erec['count'] += 1
    ents = list(sorted(ents.values(), key=lambda x: (x['executed'], x['volume']), reverse=True))
    for e in ents:
        e['payments'] = sorted(e['payments'].values())
    row['awardees'] = ents
    return row

if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
