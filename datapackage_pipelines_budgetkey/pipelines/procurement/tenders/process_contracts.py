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

distinct_tender_keys = set([
    dict(r)['tender_key'] 
    for r in conn.execute('''
        select jsonb_array_elements_text(tender_key) as tender_key 
        from contract_spending
        group by 1
    ''')
])

query = text("""
with a as (
select entity_name, entity_id, entity_kind, executed, volume, supplier_name, payments,
       jsonb_array_elements_text(tender_key) as tender_key from contract_spending
)
select * from a where tender_key=:tk
""")

def get_all_contracts(key):
    if key in distinct_tender_keys:
        return [dict(r) for r in conn.execute(query, tk=key)]
    else:
        return []


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

def process_row(row, *_):
    key_fields = ('publication_id', 'tender_type', 'tender_id')
    key = json.dumps([str(row[k]) for k in key_fields])
    contracts = get_all_contracts(key)
    row['contract_volume'] = sum(c.get('volume', 0) for c in contracts)
    row['contract_executed'] = sum(c.get('executed', 0) for c in contracts)
    timestamps = sorted(set(p['timestamp'] for c in contracts for p in c['payments']))
    ents = {}
    for contract in contracts:
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
                volume=Decimal(0),
                executed=Decimal(0),
                payments=dict((t, [t, 0]) for t in timestamps)
            )
        )
        for t in timestamps:
            erec['payments'][t][1] += payments[t]
        erec['volume'] += contract.get('volume', 0)
        erec['executed'] += contract.get('executed', 0)
    ents = list(sorted(ents.values(), key=lambda x: (x['executed'], x['volume']), reverse=True))
    for e in ents:
        e['payments'] = sorted(e['payments'].values())
    row['awardees'] = ents
    return row

if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
