from datapackage_pipelines.wrapper import process


def process_row(row, _1, spec, _2, params, _3):
    resource = params['resource']
    target_field = params['target-field']
    source_field = params['source-field']
    if spec['name'] == resource:
        code = row[source_field]
        code = code[2:]
        nice_code = ''
        while code:
            nice_code += code[:2] + '.'
            code = code[2:]
        row[target_field] = nice_code[:-1]
    return row


def modify_datapackage(dp, params, *_):
    resource = params['resource']
    target_field = params['target-field']
    for res in dp['resources']:
        if res['name'] == resource:
            res['schema']['fields'].append({
                'name': target_field,
                'type': 'string',
                'es:keyword': True
            })
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
