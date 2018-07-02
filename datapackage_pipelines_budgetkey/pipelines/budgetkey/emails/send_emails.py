from datapackage_pipelines.wrapper import process

import logging
import requests
import urllib.parse
import json
import datetime

now = datetime.datetime.now().date()
today = now.isoformat()
last_week = (now - datetime.timedelta(days=7)).isoformat()
next_week = (now + datetime.timedelta(days=7)).isoformat()
suff1 = ' 00:00:00'
suff2 = 'T00:00:00Z'

SECTIONS = [
    ('מכרזים שנסגרים השבוע', 
     'הזדמנות אחרונה להגיש הצעות!',
     dict(claim_date__gte=today + suff1,
          claim_date__lte=next_week + suff1,
          tender_type=['central', 'office']
     )
    ),
    ('מכרזים חדשים',
     'מכרזים חדשים שעשויים לעניין אותך',
     dict(
        __created_at__gte=last_week + suff2,
        tender_type=['central', 'office']
     )
    ),
    ('עדכונים נוספים',
     'מכרזים מעניינים נוספים שהתעדכנו בשבוע האחרון',
     dict(
        claim_date__gte=next_week + suff1,
        last_update_date__lte=last_week,
        __created_at__lte=last_week + suff2,
        tender_type=['central', 'office']
     )
    ),
    ('בקשות חדשות לפטור ממכרז',
     'בקשות פטור ממכרז בנושאים אלו',
     dict(
        __created_at__gte=last_week + suff2,
        tender_type=['exemptions']
     )
    ),
]

def query_url(term, types, filters):
    term = urllib.parse.quote_plus(term)
    types = ','.join(types)
    filters = urllib.parse.quote_plus(json.dumps(filters))
    return f'https://next.obudget.org/s/?q={term}&dd={types}&filters={filters}'


def process_row(row, *_):
    logging.info('ROW: %r', row)
    items = row['items']
    sections = []
    for header, subheader, filters in sections:
        terms = []
        section = dict(
            header=header,
            subheader=subheader,
            terms=terms
        )
        for item in items:
            props = item['properties']
            if 'displayDocsTypes' in props:
                if 'tenders' in props['displayDocsTypes']:
                  terms.append(dict(
                      term=item['term'],
                      query_url=query_url(item['term'], ['tenders'], filters)
                  ))
        sections.append(section)
    ret = dict(
        sections=sections,
        email=row['email']
    )
    logging.info('DATAS: %r', ret)
    result = requests.post('http://budgetkey-emails:8000/', json=ret).json()
    logging.info('RESULT: %r', result)


if __name__ == '__main__':
    process(process_row=process_row)


