import os
import logging

from datapackage_pipelines.wrapper import ingest, spew
from decimal import Decimal

from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

parameters, dp, res_iter = ingest()

engine = create_engine(os.environ['DPP_DB_ENGINE']).connect()


INCOME_SUFFIXES = ['', '_contracts', '_supports']
INCOME_LIST_QUERY = '''
SELECT 'org/' || kind || '/' || id as doc_id, name, received_amount{suffix} as amount
FROM entities_processed
JOIN guidestar_processed
USING (id)
WHERE association_field_of_activity='{foa}'
AND received_amount{suffix}>0
AND association_status_active
ORDER BY 3 desc
'''

TOTAL_INCOME_LIST_QUERY = '''
SELECT sum(received_amount{suffix}) as amount
FROM entities_processed
JOIN guidestar_processed
USING (id)
WHERE association_field_of_activity='{foa}'
AND received_amount{suffix}>0
AND association_status_active
'''


def make_income_list(foa, suffix):
    query = INCOME_LIST_QUERY.format(
        foa=foa, suffix=suffix
    )
    result = []
    try:
        result = engine.execute(text(query))
        results = [r._asdict() for r in results]
        for r in result:
            for k, v in r.items():
                if isinstance(v, Decimal):
                    r[k] = float(v)
    except ProgrammingError:
        logging.exception('Failed to query DB for incomes')
    return result

def make_income_total(foa, suffix):
    query = TOTAL_INCOME_LIST_QUERY.format(
        foa=foa, suffix=suffix
    )
    result = []
    try:
        result = engine.execute(text(query))
        result = list(r._asdict() for r in result)[0]['amount']
        if not result:
            result = 0
        return float(result)
    except ProgrammingError:
        logging.error('Failed to query DB for incomes')
    return result

def process_resource(res_):
    for row in res_:
        details = row['details']
        if row['key'].startswith('ngo-activity-report'):
            foa = details['field_of_activity']
            for suffix in INCOME_SUFFIXES:
                details['income_list{}'.format(suffix)] = make_income_list(
                    foa, suffix
                )
                details['income_total{}'.format(suffix)] = make_income_total(
                    foa, suffix
                )
        row['details'] = details
        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_resource(first)
    yield from res_iter_


for suffix in INCOME_SUFFIXES:
    dp['resources'][0]['schema']['fields'].append(
        {
            'name': 'income_list{}'.format(suffix),
            'type': 'array',
            'es:itemType': 'object',
            'es:index': False
        },
    )
    dp['resources'][0]['schema']['fields'].append(
        {
            'name': 'income_total{}'.format(suffix),
            'type': 'number',
        },
    )

spew(dp, process_resources(res_iter))
