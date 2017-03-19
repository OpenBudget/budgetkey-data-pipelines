from budgetkey_data_pipelines.common.data_gov_il import get_resource

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()

dataset_name = str(parameters['dataset-name'])
resource_name = str(parameters['resource-name'])
resource = parameters.get('resource', {})

data_gov_il_resource = get_resource(dataset_name, resource_name)

resource['url'] = data_gov_il_resource['url']

datapackage['resources'].append(resource)
datapackage.setdefault('sources', []).append({
    'web': data_gov_il_resource['url']
})

spew(datapackage, res_iter)
