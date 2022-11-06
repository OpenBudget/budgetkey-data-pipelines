from itertools import chain
import json

from datapackage_pipelines.wrapper import ingest, spew

tenders = {}
FIELDS = (
    'tender_type', 
    'tender_id', 
    'publication_id', 
    'regulation', 
    'description',
    'soproc_id',
    'soproc_name',
)
key_fields = ('publication_id', 'tender_type', 'tender_id')


def collect_tenders(res):
    for row in res:
        key = tuple(str(row[k]) for k in key_fields)
        tenders[key] = dict(
            (k, v)
            for k, v in row.items()
            if k in FIELDS
        )
        if row.get('subjects') and isinstance(row['subjects'], str):
            tenders[key]['subject_list'] = [x.strip() for x in row['subjects'].split(';')]
    print('Done collecting tenders, got %d tenders' % len(tenders))


def join_tenders(res):
    for row in res:
        manof_excerpts = []
        tender_keys = row['tender_key']
        if isinstance(tender_keys, list):
            for tender_key in tender_keys:
                i = tuple(json.loads(tender_key))
                t = tenders.get(i)
                if t:
                    manof_excerpts.append(dict(
                        (k, t[k] if k != 'publication_id' else int(t[k]))
                        for k in FIELDS
                    ))
        row['manof_excerpts'] = manof_excerpts
        yield row


def process_resources(resources):
    for res in resources:
        print('Processing %s' % res.spec['name'])
        if res.spec['name'] == 'tenders':
            collect_tenders(res)
        elif res.spec['name'] == 'quarterly-contract-spending-reports':
            yield join_tenders(res)
        else:
            assert False


def process_datapackage(dp):
    tenders_res = next(iter(filter(
        lambda x: x['name'] == 'tenders',
        dp['resources']
    )))
    tenders_fields = tenders_res['schema']['fields']
    tenders_fields = list(filter(
        lambda x: x['name'] in FIELDS,
        tenders_fields
    ))
    for tf in tenders_fields:
        if tf['name'] == 'publication_id':
            tf['type'] = 'integer'

    dp['resources'] = list(filter(
        lambda x: x['name'] != 'tenders',
        dp['resources']
    ))
    contract_spending_res = next(iter(filter(
        lambda x: x['name'] == 'quarterly-contract-spending-reports',
        dp['resources']
    )))
    contract_spending_res['schema']['fields'].extend([
        {
            'name': 'manof_excerpts',
            'type': 'array',
            'es:itemType': 'object',
            'es:schema': {
                'fields': tenders_fields
            }
        }
    ])
    return dp


if __name__ == '__main__':
    with ingest() as ctx:
        parameters, datapackage, res_iter = tuple(ctx)
        ctx.datapackage = process_datapackage(datapackage)
        ctx.resource_iterator = process_resources(res_iter)
