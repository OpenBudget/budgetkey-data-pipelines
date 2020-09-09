import os
import logging
import json
import re

from datapackage_pipelines.wrapper import process

from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError, OperationalError

from fuzzywuzzy import process as fw_process, fuzz

TK = 'tender_key'
DISALLOWED = {'9999', '99999', '999999', '000000', '00000', '0000', '1111', 'TEST'}
db_table = 'procurement_tenders'
connection_string = os.environ['DPP_DB_ENGINE']
engine = create_engine(connection_string)

key_fields = ('publication_id', 'tender_type', 'tender_id', 'publisher')
to_select = ','.join(key_fields)

all_tenders = set()
for result in engine.execute(f'select {to_select} from {db_table}'):
    all_tenders.add(tuple(str(result[k]).strip() for k in key_fields))
all_tenders_dict = dict(
    [(t[0], [t]) for t in all_tenders if t[0] and len(t[0]) > 4]
)
for t in all_tenders:
    if t[2] and len(t[2]) > 4 and t[2] != 'none':
        all_tenders_dict.setdefault(t[2], []).append(t)

logging.info('Collected %d tenders and exemptions', len(all_tenders))

def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append(dict(
        name = TK,
        type = 'string'
    ))
    dp['resources'][0]['schema']['fields'].append(dict(
        name = TK + '_simple',
        type = 'string'
    ))
    return dp

splitter = re.compile('[-/0-9]{4,}')
failed = set()
def process_row(row, *_):
    if TK in row:
        del row[TK]
    manof_ref = row['manof_ref']
    if manof_ref:
        parts = splitter.findall(manof_ref)
    else:
        parts = []
    row[TK] = None
    row[TK + '_simple'] = None
    for mf in parts:
        if mf not in failed:
            if mf not in DISALLOWED:
                if mf in all_tenders_dict:
                    options = all_tenders_dict[mf]
                    if len(options) == 1:
                        selected = options[0]
                    else:
                        publisher_name = row.get('publisher_name', '')
                        options = dict((k[4], k) for k in options)
                        selected, score = fw_process.extractOne(publisher_name, list(options.keys()), processor=fuzz.ratio)
                        if not publisher_name or score < 60:
                            logging.info('Failed to find publisher match for %r: %r', publisher_name, list(options.values()))
                        selected = options[selected]
                    row[TK] = json.dumps(list(selected)[:3])
                    break
            if TK not in row:
                logging.info('Failed to find reference for "%s" (part: %s)', manof_ref, mf)
                failed.add(mf)
    if row.get(TK):
        tk = json.loads(row[TK])
        if len(tk) == 1:
            row[TK + '_simple'] = '/'.join(tk[0])
    return row

if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
