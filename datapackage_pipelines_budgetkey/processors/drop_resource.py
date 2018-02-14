import collections 

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()
resource_name = parameters['resource']


def process_resources(res_iter_):
    for res in res_iter_:
        if res.spec['name'] != resource_name:
            yield res
        else:
            collections.deque(res, 0)


dp['resources'] = list(filter(lambda r: r['name'] != resource_name, dp['resources']))


spew(dp, process_resources(res_iter))
