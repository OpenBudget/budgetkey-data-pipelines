from budgetkey_data_pipelines.common.data_gov_il import get_resource
from datapackage_pipelines.utilities.resources import PATH_PLACEHOLDER, PROP_STREAMED_FROM

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()

dataset_name = str(parameters['dataset-name'])
resource_name = str(parameters['resource-name'])
resource = parameters.get('resource', {})

data_gov_il_resource = get_resource(dataset_name, resource_name)

resource[PROP_STREAMED_FROM] = data_gov_il_resource['url']
resource['path'] = PATH_PLACEHOLDER

datapackage['resources'].append(resource)
datapackage.setdefault('sources', []).append({
    'title': '{}/{}'.format(dataset_name, resource_name),
    'path': data_gov_il_resource['url']
})

spew(datapackage, res_iter)
