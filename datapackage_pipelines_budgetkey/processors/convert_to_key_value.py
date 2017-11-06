from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()


def process_resources(res_iter_):
    for res in res_iter_:
        for row in res:
            yield {
                'key': row['doc_id'],
                'value': dict(row)
            }

dp['resources'] = [
    {
        'name': 'document',
        PROP_STREAMING: True,
        'path': 'data/documents.csv',
        'schema': {
            'fields': [
                {'name': 'key', 'type': 'string'},
                {'name': 'value', 'type': 'object', 'es:index': False}
            ],
            'primaryKey': ['key']
        }
    }
]

spew(dp, [process_resources(res_iter)])
