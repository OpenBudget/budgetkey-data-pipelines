import logging

from dataflows import Flow, load, update_resource
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from datapackage_pipelines_budgetkey.common.data_gov_il import get_resource
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


def finalize(f):
    def func(package):
        yield package.pkg
        yield from package
        try:
            logging.warning('Finalizing connection')
            f()
        except Exception:
            logging.exception('Failed to finalize')
    return func


def flow(parameters):
    dataset_name = str(parameters['dataset-name'])
    resource_name = str(parameters['resource-name'])
    resource = parameters.get('resource', {})
    resource.update({
        'dpp:streaming': True,
    })

    gcd = parameters.get('gcd') if parameters and parameters.get('gcd') else google_chrome_driver()
    url, path = get_resource(gcd, dataset_name, resource_name)

    args = {
        'name': resource_name,
        'http_timeout': 30
    }
    if '.xls' in path:
        args['force_strings'] = True

    return Flow(
        add_source('{}/{}'.format(dataset_name, resource_name), url),
        load(path, **args),
        update_resource(resource_name, **resource),
        finalize(gcd.teardown),
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
