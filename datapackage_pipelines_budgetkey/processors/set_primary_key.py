from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()

for resource in datapackage['resources']:
    if resource['name'] in parameters:
        resource['schema']['primaryKey'] = parameters[resource['name']]

spew(datapackage, res_iter)
