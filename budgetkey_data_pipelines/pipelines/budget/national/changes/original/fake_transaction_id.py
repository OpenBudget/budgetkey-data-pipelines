from datapackage_pipelines.wrapper import process

def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append({
        'name': 'transaction_id',
        'type': 'string'
    })
    return dp

def process_row(row, *_):
    row['transaction_id'] = '{year}/{leading_item:02d}-{req_code:02d}'.format(**row)
    return row

process(modify_datapackage=modify_datapackage,
        process_row=process_row)