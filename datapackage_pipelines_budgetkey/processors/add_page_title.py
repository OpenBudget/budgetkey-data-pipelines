from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()

key_pattern = parameters['page-title-pattern']

def process_resources(res_iter_):
    for res in res_iter_:
        for row in res:
            row['page_title'] = key_pattern.format(**row)
            yield row

dp['resources'][0]['schema']['fields'].append(
    {'name': 'page_title', 'type': 'string'}
)

spew(dp, [process_resources(res_iter)])
