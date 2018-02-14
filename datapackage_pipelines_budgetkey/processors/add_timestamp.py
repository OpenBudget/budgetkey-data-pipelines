import datetime 

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()

resource_name = parameters['resource']
today = datetime.datetime.now().date()


def process_resource(res):
    for row in res:
        row['__updated_timestamp'] = today
        yield row


def process_resources(res_iter_):
    for res in res_iter_:
        if res.spec['name'] == resource_name:
            yield process_resource(res)
        else:
            yield res


for resource in dp['resources']:
    if resource['name'] == resource_name:
        resource['schema']['fields'].append(
            {'name': '__updated_timestamp', 'type': 'date'}
        )

spew(dp, process_resources(res_iter))
