from datapackage_pipelines.wrapper import process


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    row['pending'] = spec['name'] == parameters['pending-resource-name']
    return row


def modify_datapackage(dp, *_):
    for res in dp['resources']:
        res['schema']['fields'].append({
            'name': 'pending',
            'type': 'boolean'
        })
    return dp

process(modify_datapackage=modify_datapackage,
        process_row=process_row)
