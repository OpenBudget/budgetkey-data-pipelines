import os
import logging
import json

from datapackage_pipelines.wrapper import process

from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError, OperationalError

TK = 'tender_key'
DISALLOWED = {'9999', '99999', '999999', '000000', '00000', '0000', '1111', 'TEST'}
db_table = 'procurement_tenders'
connection_string = os.environ['DPP_DB_ENGINE']
engine = create_engine(connection_string)

key_fields = ('publication_id', 'tender_type', 'tender_id')
to_select = ','.join(key_fields)

all_tenders = set()
for result in engine.execute(f'select {to_select} from {db_table}'):
    all_tenders.add(tuple(str(result[k]) for k in key_fields))

logging.info('Collected %d tenders and exemptions', len(all_tenders))

def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append(dict(
        name = TK,
        type = 'string'
    ))
    return dp

failed = set()
def process_row(row, *_):
    mf = row['manof_ref']
    if mf:
        mf = mf.strip()
    if mf and len(mf)>3:
        if mf not in failed:
            if mf not in DISALLOWED:
                for t in all_tenders:
                    if ((t[0] and len(t[0]) > 3 and t[0]} in mf) or 
                        (t[2] and len(t[2]) > 3 and t[2] != 'none' and t[2] in mf)):
                        row[TK] = json.dumps(list(t))
                        break
            if TK not in row:
                row[TK] = None
                logging.info('Failed to find reference for "%s"', mf)
                failed.add(mf)
        else:
            row[TK] = None
    else:
        row[TK] = None
    return row

if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
