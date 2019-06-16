import os

import datapackage

from datapackage_pipelines.wrapper import ingest
from datapackage_pipelines.utilities.flow_utils import spew_flow
from datapackage_pipelines.utilities.resources import PROP_STREAMING

from dataflows import Flow, load, update_resource


# def modify_datapackage(dp, parameters, *_):
#     datapackage_url = parameters['datapackage']
#     source_datapackage = datapackage.DataPackage(datapackage_url)
#     for resource in source_datapackage.resources:
#         descriptor = resource.descriptor
#         if descriptor['name'] == parameters['resource']:
#             e = create_engine(os.environ['DPP_DB_ENGINE'])
#             rp = e.execute("""SELECT column_name
#                               FROM information_schema.columns
#                               WHERE table_schema='public'
#                               AND table_name='{}'""".format(parameters['table']))
#             db_fields = [x[0] for x in rp.fetchall()]
#             logging.info('GOT DB fields: %s', db_fields)

#             # override field attributes based on parameters and matching on field name
#             override_fields = {field["name"]: field for field in parameters.get("fields", [])}
#             schema = descriptor["schema"]
#             fields = schema["fields"]
#             for field in fields:
#                 field.update(override_fields.get(field["name"], {}))
#             fields = {f['name']: f for f in fields}
#             fields = [fields[f] for f in db_fields]
#             descriptor['schema']['fields'] = fields
#             resource = {
#                 'name': descriptor['name'],
#                 PROP_STREAMED_FROM: os.environ['DPP_DB_ENGINE'],
#                 'schema': descriptor['schema'],
#                 'path': PATH_PLACEHOLDER,
#                 'table': parameters['table']
#             }
#             dp['resources'].append(resource)
#             break
#     return dp

# process(modify_datapackage=modify_datapackage)

def flow(parameters):
    source_datapackage = datapackage.DataPackage(parameters['datapackage'])
    resource_name = parameters['resource']
    resource = source_datapackage.get_resource(resource_name)

    return Flow(
        load('env://DPP_DB_ENGINE',
             table=parameters['table'],
             name=resource_name),
        update_resource(resource_name,
                        **resource.descriptor),
        update_resource(resource_name,
                        **{PROP_STREAMING: True})
    )


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(flow(ctx.parameters), ctx)
