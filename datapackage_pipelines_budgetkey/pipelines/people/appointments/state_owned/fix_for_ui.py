from datapackage_pipelines.wrapper import process


def modify_datapackage(datapackage, *_):
    datapackage['resources'][0]['schema']['fields'].extend([
        {'name':'proof_url', 'type':'string'},
        {'name':'source', 'type':'string'},
        {'name':'verb', 'type':'string'},
    ])
    return datapackage

def process_row(row, *_):
    row['source'] = 'רשות החברות הממשלתיות'
    row['proof_url'] = row['urls'][0]
    row['verb'] = 'מונתה' if row['gender'] == 'woman' else 'מונה'

    return row

process(modify_datapackage=modify_datapackage,
        process_row=process_row)