from os import sep
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()


def process_resource(res_):
    for row in res_:
        code = row['code']
        prefixes = []
        if code.startswith('00'):
            code = code[2:]
            for prefix in range(2, len(code) + 2, 2):
                prefix = code[:prefix]
                prefixes.append(prefix)
                prefixes.append('00' + prefix)
                separated = [prefix[x:x+2] for x in range(0, len(prefix), 2)]
                prefixes.append('.'.join(separated))
                if len(separated) > 3:
                    prefixes.append(separated[:-2])
        row['nice-prefixes'] = sorted(set(prefixes))
        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_resource(first)
    yield from res_iter_


dp['resources'][0]['schema']['fields'].extend([
    {
        'name': 'nice-prefixes',
        'type': 'array',
        'es:itemType': 'string',
        'es:keyword': True
    },
])

spew(dp, process_resources(res_iter))
