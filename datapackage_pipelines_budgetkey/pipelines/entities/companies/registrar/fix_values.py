from datapackage_pipelines.wrapper import process


def process_row(row, *_):
    row['company_address_lines'] = []
    if row.get('company_street') and \
            row.get('company_street_number') and \
            row.get('company_city'):
        row['company_address_lines'].append('{company_street} {company_street_number}, {company_city}'.format(**row))
    elif row.get('company_pob') and \
            row.get('company_city') and \
            row.get('company_postal_code'):
        row['company_address_lines'].append('ת.ד.' + '{company_pob} {company_city} {company_postal_code}'.format(**row))
    if row.get('company_country') and row['company_country'] != 'ישראל':
        row['company_address_lines'].append(row['company_country'])
    if row.get('company_located_at'):
        row['company_address_lines'].append('אצל: ' + '{company_located_at}'.format(**row))

    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append({
        'name': 'company_address_lines',
        'type': 'array',
        'es:itemType': 'string'
    })
    return dp


process(modify_datapackage=modify_datapackage,
        process_row=process_row)
