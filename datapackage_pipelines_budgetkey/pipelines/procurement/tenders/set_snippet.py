from datapackage_pipelines.wrapper import process

kinds = {
    'central': 'מכרז מרכזי',
    'office': 'מכרז משרדי',
    'exemptions': 'רכש פטור ממכרז',
}

def process_row(row, *_):
    if not row['description'] and row['page_title']:
        row['description'] = row['page_title']
    snippet = ''
    if not row['publisher']:
        row['publisher'] = 'מינהל הרכש הממשלתי'
    snippet += row['publisher'] + ': ' + kinds[row['tender_type']]
    if row['description']:
        snippet += ' - ' + row['description']
    row['snippet'] = snippet
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append(dict(
        name='snippet',
        type='string'
    ))
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
