from itertools import chain

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()


tenders = {}
FIELDS = (
    'tender_type', 'tender_id', 'publication_id', 'regulation', 'description')


def collect_tenders(res):
    for row in res:
        pid = row['publication_id']
        if not pid:
            pid = row['tender_id']
        pid = str(pid)
        tenders[pid] = dict(
            (k, v)
            for k, v in row.items()
            if k in FIELDS
        )


def join_tenders(res):
    for row in res:
        manof_excerpts = []
        manof_refs = row['manof_ref']
        if isinstance(manof_refs, list):
            for i in manof_refs:
                t = tenders.get(i)
                if t:
                    manof_excerpts.append(dict(
                        (k, t[k])
                        for k in FIELDS
                    ))
        row['manof_excerpts'] = manof_excerpts
        yield row


def process_resources(resources):
    for res in resources:
        if res.spec['name'] == 'tenders':
            collect_tenders(res)
        elif res.spec['name'] == 'contract-spending':
            yield join_tenders(res)
        else:
            yield res


def process_datapackage(dp):
    tenders_res = next(iter(filter(
        lambda x: x['name'] == 'tenders',
        dp['resources']
    )))
    tenders_fields = tenders_res['schema']['fields']
    dp['resources'] = list(filter(
        lambda x: x['name'] != 'tenders',
        dp['resources']
    ))
    contract_spending_res = next(iter(filter(
        lambda x: x['name'] == 'contract-spending',
        dp['resources']
    )))
    contract_spending_res['schema']['fields'].append({
        'name': 'manof_excerpts',
        'type': 'array',
        'es:itemType': 'object',
        'es:schema': {
            'fields': list(filter(
                lambda x: x['name'] in FIELDS,
                tenders_fields
            ))
        }
    })
    return dp


spew(process_datapackage(datapackage),
     process_resources(res_iter))