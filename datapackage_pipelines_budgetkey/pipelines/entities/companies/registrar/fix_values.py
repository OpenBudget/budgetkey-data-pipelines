from datapackage_pipelines.wrapper import process

def process_row(row, *_):
    row['company_address'] = []
    if row.get('company_address') and \
            row.get('company_street_number') and \
            row.get('company_city'):
        row['company_address'].append('{company_street} {company_street_number},'.format(**row))
    elif row.get('company_pob') and \
            row.get('company_pob_city') and \
            row.get('company_pob_postal_code'):
        row['company_address'].append('ת.ד.' + '{company_pob} {company_pob_city} {company_pob_postal_code}'.format(**row))
    if row.get('company_located_at'):
        row['company_address'].append('אצל: ' + '{company_located_at}'.format(**row))

    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append({
        'name': 'company_address',
        'type': 'array',
        'es:itemType': 'string'
    })
    return dp


process(modify_datapackage=modify_datapackage,
        process_row=process_row)
