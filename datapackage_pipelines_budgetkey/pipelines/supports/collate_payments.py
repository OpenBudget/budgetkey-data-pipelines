import decimal

from datapackage_pipelines.wrapper import process

def fix_decimal(value):
    if isinstance(value, decimal.Decimal):
        value = float(value)
    return value

def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    fields = parameters['fields']
    field = parameters['field']
    key_field = parameters['key-field']
    key_field_sources = parameters['key-field-sources']
    if spec['name'] == parameters['resource']:
        rec = dict(
            (k, fix_decimal(row[k]))
            for k in fields
        )
        row[field] = rec
        for f in key_field_sources:
            v = rec.get(f)
            if v:
                row[key_field] = v
                break
    return row


def modify_datapackage(dp, parameters, stats):
    for resource in dp['resources']:
        if resource['name'] == parameters['resource']:
            resource['schema']['fields'].append({
                'name': parameters['field'],
                'type': 'object',
            })
            resource['schema']['fields'].append({
                'name': parameters['key-field'],
                'type': 'string',
            })
    return dp

process(modify_datapackage=modify_datapackage,
        process_row=process_row)