from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()

key_pattern = parameters['doc-id-pattern']

def process_resources(res_iter_):
    for res in res_iter_:
        for row in res:
            row['doc_id'] = key_pattern.format(**row),
            yield row

dp['resources'][0]['schema']['fields'].append(
    {'name': 'doc_id', 'type': 'string'}
)

spew(dp, [process_resources(res_iter)])
