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
    if spec['name'] == parameters['resource']:
        rec = dict(
            (k, fix_decimal(row[k]))
            for k in fields
        )
        row[field] = rec
    return row


def modify_datapackage(dp, parameters, stats):
    for resource in dp['resources']:
        if resource['name'] == parameters['resource']:
            resource['schema']['fields'].append({
                'name': parameters['field'],
                'type': 'object',
            })
    return dp

process(modify_datapackage=modify_datapackage,
        process_row=process_row)