from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()

revision = int(parameters['revision'])


def process_resource(res_):
    for row in res_:
        row['__revision'] = revision
        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_resource(first)
    yield from res_iter_


dp['resources'][0]['schema']['fields'].append({
    'name': '__revision',
    'type': 'integer'
})

spew(dp, process_resources(res_iter))
