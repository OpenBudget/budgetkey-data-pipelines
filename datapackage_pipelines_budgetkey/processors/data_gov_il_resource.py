from datapackage_pipelines_budgetkey.common.data_gov_il import get_resource
from datapackage_pipelines.utilities.resources import PATH_PLACEHOLDER, PROP_STREAMED_FROM

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()

dataset_name = str(parameters['dataset-name'])
resource_name = str(parameters['resource-name'])
resource = parameters.get('resource', {})

data_gov_il_resource = get_resource(dataset_name, resource_name)

url = data_gov_il_resource['url']
resource[PROP_STREAMED_FROM] = url
resource['path'] = PATH_PLACEHOLDER
if '.xls' in url:
    resource['force_strings'] = True

datapackage['resources'].append(resource)
datapackage.setdefault('sources', []).append({
    'title': '{}/{}'.format(dataset_name, resource_name),
    'path': data_gov_il_resource['url']
})

spew(datapackage, res_iter)
