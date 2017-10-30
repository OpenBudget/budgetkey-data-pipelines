from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher
from datetime import date

parameters, dp, res_iter = ingest()

resource_matcher = ResourceMatcher(parameters['resource'])
key = parameters['key']
collated_field_name = parameters['collated-field-name']
assert isinstance(key, list)


for res in dp['resources']:
    if resource_matcher.match(res['name']):
        outer_fields = []
        inner_fields = []
        for field in res['schema']['fields']:
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
        pk = res['schema'].get('primaryKey')
        if pk is not None:
            schema['primaryKey'] = pk
        res['schema'] = schema
        dp['collated-schema:{}:{}'.format(res['name'], collated_field_name)] = {
            'fields': inner_fields
        }


def treat(v):
    if isinstance(v, date):
        return v.isoformat()
    return v


def process_resource(res):
    for row in res:
        inner = dict(
            (k, treat(v))
            for k, v in row.items()
            if k not in key
        )
        outer = dict(
            (k, v)
            for k, v in row.items()
            if k in key
        )
        outer[collated_field_name] = inner
        yield outer


def process_resources(res_iter_):
    for res in res_iter_:
        if resource_matcher.match(res.spec['name']):
            yield process_resource(res)
        else:
            yield res

spew(dp, process_resources(res_iter))