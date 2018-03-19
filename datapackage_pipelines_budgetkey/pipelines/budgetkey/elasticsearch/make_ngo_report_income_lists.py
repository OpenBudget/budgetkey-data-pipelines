import os

from datapackage_pipelines.wrapper import ingest, spew

from sqlalchemy import create_engine

parameters, dp, res_iter = ingest()

engine = create_engine(os.environ['DPP_DB_ENGINE'])


INCOME_SUFFIXES = ['', '_contracts', '_supports']
INCOME_LIST_QUERY = '''
SELECT 'org/' || kind || '/' || id as doc_id, name, received_amount{suffix} 
FROM _elasticsearch_mirror__entities
JOIN guidestar_processed
USING (id)
WHERE association_field_of_activity='{foa}'
ORDER BY 3 desc
'''


def make_income_list(foa, suffix):
    query = INCOME_LIST_QUERY.format(
        foa=foa, suffix=suffix
    )
    result = engine.execute(query)
    result = list(dict(r) for r in result)

def process_resource(res_):
    for row in res_:
        if row['key'].startswith('ngo-activity-report'):
            foa = row['details']['field_of_activity']
            for suffix in INCOME_SUFFIXES:
                row['income_list{}'.format(suffix)] = make_income_list(
                    foa, suffix
                )
        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_resource(first)
    yield from res_iter_


for suffix in INCOME_SUFFIXES:
    for prefix in ['income_list', 'income_total']:
        dp['resources'][0]['schema']['fields'].append(
            {
                'name': '{}{}'.format(prefix, suffix),
                'type': 'array',
                'es:itemType': 'object',
                'es:index': False
            },
        )

spew(dp, process_resources(res_iter))
