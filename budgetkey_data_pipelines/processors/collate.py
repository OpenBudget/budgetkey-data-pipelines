from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher

parameters, dp, res_iter = ingest()

resource = ResourceMatcher(parameters['resource'])
key = parameters['key']
collated_field_name = parameters['collated-field-name']
assert isinstance(key, list)


for res in dp['resources']:
    if resource.match(res['name']):
        outer_fields = []
        inner_fields = []
        for field in resource['schema']['fields']:
            if field['name'] in key:
                outer_fields.append(field)
            else:
                inner_fields.append(field)
        outer_fields.append({
            'name': collated_field_name,
            'type': 'object',
            'es:schema': inner_fields
        })
        schema = {
            'fields': outer_fields,
        }
        pk = resource['schema'].get('primaryKey')
        if pk is not None:
            schema['primaryKey'] = pk
        resource['schema'] = schema


def process_resource(res):
    for row in res:
        inner = dict(
            (k, v)
            for k, v in row.items()
            if k not in key
        )
        outer = dict(
            (k, v)
            for k, v in row.items()
            if k in key
        )
        outer[collated_field_name] = inner
        yield row


def process_resources(res_iter_):
    for res in res_iter_:
        if resource.match(res.spec['name']):
            yield process_resource(res)
        else:
            yield res

spew(dp, process_resources(res_iter))