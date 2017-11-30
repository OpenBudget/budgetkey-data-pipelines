import datetime
import re

import logging
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew
from decimal import Decimal

parameters, datapackage, res_iter = ingest()

subschema = {'fields': []}
subschema_field_names = set()


def process_datapackage(datapackage):
    new_resources = []
    for resource in datapackage['resources']:
        if resource['name'] in parameters:
            res_params = parameters[resource['name']]
            new_resource = {
                'name': resource['name'],
                PROP_STREAMING: True,
                'path': resource['path'],
                'schema': {
                    'fields': [
                        {'name': 'id', 'type': 'string'},
                        {'name': 'name', 'type': 'string'},
                        {'name': 'name_en', 'type': 'string'},
                        {'name': 'kind', 'type': 'string'},
                        {'name': 'kind_he', 'type': 'string'},
                        {'name': 'details', 'type': 'object', 'es:schema': subschema},
                    ]
                }
            }
            new_resources.append(new_resource)

            kind_column = res_params.get('kind-column')
            id_column = res_params['id-column']
            name_column = res_params['name-column']
            name_en_column = res_params.get('name-en-column')

            prefix = res_params.get('remove-prefix', '')
            prefix = re.compile('^'+prefix)

            for field in resource['schema']['fields']:
                if field['name'] not in [kind_column, id_column, name_column, name_en_column]:
                    field['name'] = prefix.sub('', field['name'])
                    if field['name'] not in subschema_field_names:
                        subschema['fields'].append(field)
                        subschema_field_names.add(field['name'])
                    else:
                        existing = next(iter(filter(lambda f: f['name'] == field['name'],
                                                    subschema['fields'])))
                        assert existing['type'] == field['type'], \
                            "Duplicate field name %r != %r (%r)" % (field, existing, res_params)
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
        kind_he_column = res_params.get('kind-he-column')
        kind_he = res_params.get('kind-he')
        id_column = res_params['id-column']
        name_column = res_params['name-column']
        name_en_column = res_params.get('name-en-column')

        conds = [kind is not None, kind_column is not None]
        assert any(conds) and not all(conds)

        for i, row in enumerate(resource):
            if i % 1000 == 0:
                logging.info('%s loaded %d rows', spec['name'], i)
            if kind_column is not None and not row[kind_column]:
                continue
            if row[id_column] and row[name_column]:
                details = {}
                for k, v in row.items():
                    if k not in [name_column, name_en_column, id_column, kind_column]:
                        k = prefix.sub('', k)
                        if isinstance(v, datetime.date):
                            v = v.isoformat()
                        elif isinstance(v, Decimal):
                            v = float(v)
                        details[k] = v
                new_row = {
                    'kind': kind if kind is not None else row[kind_column],
                    'kind_he': kind_he if kind_he is not None else row[kind_he_column],
                    'id': row[id_column],
                    'name': row[name_column],
                    'name_en': row.get(name_en_column),
                    'details': details
                }
                yield new_row
    else:
        yield from resource


def process_resources(res_iter_):
    for resource in res_iter_:
        yield process_resource(resource)


spew(process_datapackage(datapackage),
     process_resources(res_iter))