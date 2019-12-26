from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()


def process_resource(res_):
    for row in res_:
        code = row['code'][2:]
        row['simple-code'] = code
        nice_code = code[:2]
        code = code[2:]
        while len(code) > 0:
            nice_code += '.'+code[:2]
            code = code[2:]
        row['nice-code'] = nice_code
        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_resource(first)
    yield from res_iter_


dp['resources'][0]['schema']['fields'].extend([
    {
        'name': 'nice-code',
        'type': 'string',
        'es:keyword': True
    },
    {
        'name': 'simple-code',
        'type': 'string',
        'es:keyword': True
    }
])

spew(dp, process_resources(res_iter))
