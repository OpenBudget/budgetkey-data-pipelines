from datapackage_pipelines.wrapper import process


def process_row(row, _1, spec, _2, params, _3):
    if spec['name'] == 'supports':
        payments = row.get('payments', [])
        if payments:
            row['support_title'] = payments[0].get('support_title')
            row['supporting_ministry'] = payments[0].get('supporting_ministry', '')

    return row


def modify_datapackage(dp, params, *_):
    for res in dp['resources']:
        if res['name'] == 'supports':
            res['schema']['fields'].append({
                'name': 'support_title',
                'type': 'string',
                'es:title': True
            })
            res['schema']['fields'].append({
                'name': 'supporting_ministry',
                'type': 'string',
                'es:keyword': True
            })
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
