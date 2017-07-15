from datapackage_pipelines.wrapper import process


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    if spec['name'] == 'national-budget-changes':
        if row['budget_code'].startswith('0047') and row['leading_item'] != 47:
            return None
        row['budget_code_title'] = '{budget_code}:{budget_title}'.format(**row)
        del row['budget_code']
        del row['budget_title']
    return row


def modify_datapackage(dp, *_):
    for resource in dp['resources']:
        if resource['name'] == 'national-budget-changes':
            fields = []
            for field in resource['schema']['fields']:
                if not field['name'].startswith('budget-'):
                    fields.append(field)
                fields.append({
                    'name': 'budget_code_title',
                    'type': 'string'
                })
            resource['schema']['fields'] = fields
    return dp


process(modify_datapackage=modify_datapackage,
        process_row=process_row)