import logging
from datapackage_pipelines.wrapper import ingest, spew
from itertools import zip_longest

def nop(x):
    return x


FIELDS = ['code', 'title', 'net_allocated', 'net_revised']


params, dp, res_iter = ingest()
res_name = params['resource-name']


def combine_immediate_children(rows):
    for row in rows:
        if row['children-net_revised'] is None:
            logging.warning('children-net_revised is undefined, %r', row)
            row['children-net_revised'] = row['children-net_allocated']
        for f in ('children-net_revised', 'children-net_allocated'):
            row[f] = map(int, row[f])
        try:
            children = zip_longest(*(row.get('children-'+x, []) for x in FIELDS))
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
