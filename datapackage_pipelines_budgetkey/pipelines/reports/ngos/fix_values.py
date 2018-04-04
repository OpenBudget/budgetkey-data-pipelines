from datapackage_pipelines.wrapper import process


def process_row(row, *_):
    reports = dict(
        (r.pop('subset'), r)
        for r in row['report']
    )
    row['report'] = reports
    return row


def modify_datapackage(dp, *_):
    for x in dp['resources'][0]['schema']['fields']:
        if x['name'] == 'report':
            x['type'] = 'object'
    return dp

if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
