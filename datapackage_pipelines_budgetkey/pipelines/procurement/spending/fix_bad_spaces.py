from datapackage_pipelines.wrapper import process

def modify_datapackage(dp, *_):
    for res in dp['resources']:
        for field in res['schema']['fields']:
            field['name'] = field['name'].replace('\xa0', ' ')
    return dp

def process_row(row, *_):
    row = dict(
        (k.replace('\xa0', ' '), v)
        for k, v in row.items()
    )
    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)