from datapackage_pipelines.wrapper import process


def modify_datapackage(datapackage, *_):
    datapackage['resources'][0]['schema']['fields'] = [field for field in
                                                       datapackage['resources'][0]['schema']['fields']
                                                       if field['name'] != 'is_latest']
    return datapackage

def process_row(row, *_):
    if row['is_latest']:
        row['last_date'] = None
    del row['is_latest']
    return row

process(modify_datapackage=modify_datapackage,
        process_row=process_row)