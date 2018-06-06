import os
import logging
import json

from datapackage_pipelines.wrapper import process

from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError, OperationalError
from sqlalchemy.sql import text

connection_string = os.environ['DPP_DB_ENGINE']
engine = create_engine(connection_string)

query = text("""
with a as (
select publisher_name, purchasing_unit, purpose, budget_code, budget_title, entity_name, entity_id, entity_kind,
       executed, volume, currency, explanation, payments, start_date, end_date, 
       jsonb_array_elements_text(tender_key) as tender_key
)
select * from a where tender_key=:tk
""")

def get_all_contracts(key):
    return [dict(r) for r in engine.execute(query, tk=key)]


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append({
        'name': 'contracts',
        'type': 'array',
        'es:itemType': 'object',
        'es:index': False

     })
    return dp

def process_row(row, *_):
    key_fields = ('publication_id', 'tender_type', 'tender_id')
    key = json.dumps([str(row[k]) for k in key_fields])
    row['contracts'] = get_all_contracts(key)
    return row

if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
