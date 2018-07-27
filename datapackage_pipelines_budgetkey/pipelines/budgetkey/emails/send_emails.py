from datapackage_pipelines.wrapper import process

import logging
import requests
import urllib.parse
import json
import datetime


SECTIONS = [
    ('מכרזים שנסגרים השבוע', 
     'הזדמנות אחרונה להגיש הצעות!',
     'dd=tenders&theme=govbuy&focused=closing'
    ),
    ('מכרזים חדשים',
     'מכרזים חדשים שעשויים לעניין אותך',
     'dd=tenders&theme=govbuy&focused=new'
    ),
    ('עדכונים נוספים',
     'מכרזים מעניינים נוספים שהתעדכנו בשבוע האחרון',
     'dd=tenders&theme=govbuy&focused=updated'
    ),
    ('בקשות חדשות לפטור ממכרז',
     'בקשות פטור ממכרז בנושאים אלו',
     'dd=tenders&theme=govbuy&focused=new'
    ),
]

def query_url(term, filters):
    term = urllib.parse.quote_plus(term)
    return f'https://next.obudget.org/s/?q={term}&{filters}'


def process_row(row, *_):
    logging.info('ROW: %r', row)
    items = row['items']
    sections = []
    for header, subheader, filters in SECTIONS:
        terms = []
        section = dict(
            header=header,
            subheader=subheader,
            terms=terms
        )
        for item in items:
            try:
                props = item['properties']
                props = json.loads(props)
            except Exception as e:
                logging.error('Failed to parse properties %s', e)
                continue
            if props and 'displayDocsTypes' in props:
                if 'tenders' in props['displayDocsTypes']:
                  terms.append(dict(
                      term=props['term'],
                      query_url=query_url(props['term'], filters)
                  ))
        if len(terms) > 0:
            sections.append(section)
    if len(sections) > 0:
        ret = dict(
            sections=sections,
            email=row['email']
        )
        logging.info('DATAS: %r', ret)
        result = requests.post('http://budgetkey-emails:8000/', json=ret).json()
        logging.info('RESULT: %r', result)
    else:
        logging.info('SKIPPING %s as has no relevant subscriptions', row['email'])


if __name__ == '__main__':
    process(process_row=process_row)


