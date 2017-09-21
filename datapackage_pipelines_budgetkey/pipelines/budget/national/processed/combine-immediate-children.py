import logging
from datapackage_pipelines.wrapper import ingest, spew


def nop(x):
    return x


FIELDS = ['code', 'title', 'net_allocated']


params, dp, res_iter = ingest()
res_name = params['resource-name']


def combine_immediate_children(rows):
    for row in rows:
        row['children-net_allocated'] = map(int, row['children-net_allocated'])
        try:
            children = zip(*(row['children-'+x] for x in FIELDS))
        except TypeError:
            logging.error('Failed to process children for row %r', row)
            raise
        children = [dict(zip(FIELDS, x)) for x in children]
        row['children'] = children
        for x in FIELDS:
            del row['children-'+x]
        yield row


def process_resources(res_iter_):
    for res in res_iter_:
        if res.spec['name'] == res_name:
            yield combine_immediate_children(res)
        else:
            yield res


res = next(filter(lambda r: r['name'] == res_name, dp['resources']))
res['schema']['fields'] = list(filter(
    lambda f: not f['name'].startswith('children-'),
    res['schema']['fields']
))
res['schema']['fields'].append({
    'name': 'children',
    'type': 'array'
})

spew(dp, process_resources(res_iter))
