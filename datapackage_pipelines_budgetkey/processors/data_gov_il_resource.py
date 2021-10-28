import logging
import time
import random

from dataflows import Flow, load, update_resource
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from datapackage_pipelines_budgetkey.common.data_gov_il import get_resource, get_resource_by_id
from datapackage_pipelines_budgetkey.common.google_chrome import google_chrome_driver


def add_source(title, path):
    def func(package):
        package.pkg.descriptor.setdefault('sources', []).append({
            'title': title,
            'path': path
        })
        yield package.pkg
        yield from package
    return func


def get_gcd():
    return google_chrome_driver()


def flow(parameters):
    dataset_name = str(parameters['dataset-name'])
    resource_id = resource_name = None
    if 'resource-name' in parameters:
        resource_name = str(parameters['resource-name'])
    else:
        resource_id = str(parameters['resource-id'])
    resource = parameters.get('resource', {})
    resource.update({
        'dpp:streaming': True,
    })

    if parameters.get('gcd'):
        gcd = parameters['gcd']
    else:
        gcd = get_gcd
    if resource_id is not None:
        url, path = get_resource_by_id(dataset_name, resource_id)
    else:
        url, path = get_resource(gcd, dataset_name, resource_name)

    args = {
        'name': resource_name or dataset_name,
        'http_timeout': 30
    }
    args.update(parameters.get('options', {}))
    if '.xls' in path:
        args['force_strings'] = True

    return Flow(
        add_source('{}/{}'.format(dataset_name, resource_name), url),
        load(path, **args),
        update_resource(resource_name, **resource),
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
