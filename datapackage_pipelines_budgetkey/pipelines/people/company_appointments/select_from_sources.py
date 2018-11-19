from datapackage_pipelines.wrapper import process


def process_row(row, *_):
    sources = row['sources']
    if sources:
        sources = sorted(sources, key=lambda x:x['date'])
        row['when'] = sources[0]['date']
        row['title'] = sources[0]['details']
    row['event'] = 'appointment'
    return row

def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([
        {'name': 'event', 'type': 'string'},
        {'name': 'title', 'type': 'string'},
        {'name': 'when', 'type': 'string'}
    ])
    return dp

process(modify_datapackage=modify_datapackage,
        process_row=process_row)