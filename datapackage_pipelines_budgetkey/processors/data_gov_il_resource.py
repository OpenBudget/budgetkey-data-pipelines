from dataflows import Flow, load, update_resource
from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow

from datapackage_pipelines_budgetkey.common.data_gov_il import get_resource


def add_source(title, path):
    def func(package):
        package.pkg.descriptor.setdefault('sources', []).append({
            'title': title,
            'path': path
        })
    return func
        

def flow(parameters):
    dataset_name = str(parameters['dataset-name'])
    resource_name = str(parameters['resource-name'])
    resource = parameters.get('resource', {})

    data_gov_il_resource = get_resource(dataset_name, resource_name)

    url = data_gov_il_resource['url']
    args = {
        'name': resource_name
    }
    if '.xls' in url:
        args['force_strings'] = True

    return Flow(
        add_source('{}/{}'.format(dataset_name, resource_name), url),
        load(url, **args),
        update_resource(resource_name, **resource)
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
