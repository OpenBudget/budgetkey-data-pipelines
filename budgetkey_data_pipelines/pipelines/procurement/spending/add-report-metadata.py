import re
from fuzzywuzzy import fuzz

from datapackage_pipelines.wrapper import process


def process_row(row, *_):
    title = row.get('report-title')
    publisher = row.get('report-publisher')

    # Fix bad hyphens
    title = title.replace('–', '-')
    title = title.replace('–', '-')

    # Calculate subunit
    subunit = re.split('((20\d\d)|(רבעון\s+[1-4]))\s*-?\s*', title)[-1]
    if fuzz.partial_ratio(publisher, subunit) > 80:
        subunit = publisher

    # Fix bad titles
    if title.startswith('תקשרויות '):
        title = 'ה' + title

    row.update({
        'report-year': None,
        'report-period': None,
        'report-title': title,
        'report-subunit': subunit
    })

    # Calculate report period
    if title.startswith('התקשרויות ') or title.startswith(u'דו'):
        preamble = title[10:]
        year = re.findall('20[0-9][0-9]',preamble)
        if len(year) == 1:
            preamble = preamble.replace(year[0], '')
            year = int(year[0])
            period = re.findall('רבעון ([0-9]+)', preamble)
            if len(period) == 1:
                period = int(period[0])
            else:
                period = None
            row['report-year'] = year
            row['report-period'] = period
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([
        {
            'name': 'report-subunit',
            'type': 'string'
        },
        {
            'name': 'report-year',
            'type': 'integer'
        },
        {
            'name': 'report-period',
            'type': 'integer'
        },
    ])
    return dp

process(modify_datapackage=modify_datapackage,
        process_row=process_row)