import datetime
from datapackage_pipelines.wrapper import process

old = datetime.datetime.fromtimestamp(0)

def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append({
        'name': 'score',
        'type': 'number',
        'es:score-column': True
    })
    return dp


def start_date(x):
    ret = x.get('start_date')
    if not ret:
        ret = old
    return ret


def process_row(row, *_):
    row['score'] = 1
    row['details'] = sorted(row['details'], key=start_date)
    return row


process(modify_datapackage=modify_datapackage,
        process_row=process_row)