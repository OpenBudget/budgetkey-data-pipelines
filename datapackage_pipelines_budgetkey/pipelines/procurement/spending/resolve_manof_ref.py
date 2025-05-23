import os
import logging
import json
import re

from datapackage_pipelines.wrapper import process

from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError, OperationalError

from fuzzywuzzy import process as fw_process, fuzz

TK = 'tender_key'
DISALLOWED = {'9999', '99999', '999999', '000000', '00000', '0000', '1111', 'TEST'}
db_table = 'procurement_tenders_processed'
connection_string = os.environ['DPP_DB_ENGINE']
engine = create_engine(connection_string).connect()

key_fields = ('publication_id', 'tender_type', 'tender_id', 'publisher')
to_select = ','.join(key_fields)

all_tenders = set()
for result in engine.execute(text(f'select {to_select} from {db_table}')):
    result = result._asdict()
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
def process_row(row, row_index, *_):
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
                        publisher_name = row.get('report-publisher') or ''
                        selected = options[0]
                        if publisher_name:
                            options = dict((k[3], k) for k in options if k[3])
                            publishers = list(options.keys())
                            if len(publishers) > 0:
                                try:
                                    selected, score = fw_process.extractOne(publisher_name, publishers, scorer=fuzz.ratio)
                                    if score < 60:
                                        if row_index % 100 == 0:
                                            logging.info('(sampled: row %d) Failed to find publisher match for %r: %r', row_index, publisher_name, list(options.values()))
                                    selected = options[selected]
                                except:
                                    logging.exception('Failed to extract: %r %r %r', row, publisher_name, options)
                        else:
                            logging.info('Failed to find publisher %r: %r', row, publisher_name)

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
