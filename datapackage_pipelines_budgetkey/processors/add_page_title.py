from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()

key_pattern = parameters['page-title-pattern']


def process_resource(res):
    for row in res:
        row['page_title'] = key_pattern.format(**row)
        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_resource(first)
    yield from res_iter_


if 'page_title' not in key_pattern:
    dp['resources'][0]['schema']['fields'].append(
        {'name': 'page_title', 'type': 'string', 'es:index': False}
    )

spew(dp, process_resources(res_iter))
