from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()


def process_resource(res_):
    for row in res_:
        for k in ['__last_updated_at', '__last_modified_at', '__created_at']:
            if k in row and row[k]:
                row['rev'+k[1:]] = row[k].date()
        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_resource(first)
    yield from res_iter_


dp['resources'][0]['schema']['fields'].extend([
    {'name': 'rev_last_updated_at', 'type': 'date'},
    {'name': 'rev_last_modified_at', 'type': 'date'},
    {'name': 'rev_created_at', 'type': 'date'},
])

spew(dp, process_resources(res_iter))
