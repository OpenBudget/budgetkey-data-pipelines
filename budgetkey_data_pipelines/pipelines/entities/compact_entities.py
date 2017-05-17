import re

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()


def process_datapackage(datapackage):
    new_resources = []
    for resource in datapackage['resources']:
        if resource['name'] in parameters:
            new_resource = {
                'name': resource['name'],
                'path': resource['path'],
                'schema': {
                    'fields': [
                        {'name': 'id', 'type': 'string'},
                        {'name': 'name', 'type': 'string'},
                        {'name': 'name_en', 'type': 'string'},
                        {'name': 'kind', 'type': 'string'},
                        {'name': 'details', 'type': 'object'},
                    ]
                }
            }
            new_resources.append(new_resource)
        else:
            new_resources.append(resource)
    datapackage['resources'] = new_resources
    return datapackage


def process_resource(resource):
    spec = resource.spec
    if spec['name'] in parameters:
        res_params = parameters[spec['name']]
        prefix = res_params.get('remove-prefix', '')
        prefix = re.compile('^'+prefix)
        kind = res_params.get('kind')
        kind_column = res_params.get('kind-column')
        conds = [kind is not None, kind_column is not None]
        assert any(conds) and not all(conds)
        id_column = res_params['id-column']
        name_column = res_params['name-column']
        name_en_column = res_params.get('name-en-column')

        for row in resource:
            if kind_column is not None and not row[kind_column]:
                continue
            if row[id_column] and row[name_column]:
                new_row = {
                    'kind': kind if kind is not None else row[kind_column],
                    'id': row[id_column],
                    'name': row[name_column],
                    'name-en': row.get(name_en_column),
                    'details': dict(
                        (prefix.sub('', k), v)
                        for k, v in row.items()
                        if k not in [name_column, name_en_column, id_column]
                    )
                }
                yield new_row
    else:
        yield from resource


def process_resources(res_iter_):
    for resource in res_iter_:
        yield process_resource(resource)


spew(process_datapackage(datapackage),
     process_resources(res_iter))