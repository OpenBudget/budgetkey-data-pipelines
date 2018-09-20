from datapackage_pipelines.wrapper import process

import logging
import requests
import urllib.parse
import json
import datetime
import time


SECTIONS = [
    ('מכרזים שנסגרים השבוע', 
     'הזדמנות אחרונה להגיש הצעות!',
     'dd=tenders&theme=govbuy&focused=closing'
    ),
    ('מכרזים חדשים',
     'מכרזים חדשים שעשויים לעניין אותך',
     'dd=tenders&theme=govbuy&focused=new'
    ),
    ('בקשות חדשות לפטור ממכרז',
     'משרדי ממשלה ויחידות פרסמו השבוע תהליכי רכש בפטור ממכרז בנושאים אלו',
     'dd=exemptions&theme=govbuy&focused=new'
    ),
    ('התקשרויות חדשות',
     'התקשרויות חדשות בנושאים שמעניינים אותך',
     'dd=contracts&theme=govbuy&focused=new'
    ),
    ('ומה חוץ מזה?',
     'עוד כמה עדכונים שקשורים בחיפושים השמורים שלך',
     'dd=tenders&theme=govbuy&focused=updated'
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
                if any(x in props['displayDocsTypes'] for x in ('tenders', 'all'):
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
        for retry in range(2):
            try:
                result = requests.post('http://budgetkey-emails:8000/', 
                                    json=ret,
                                    timeout=630)
                if result.status_code == 200:
                    result = result.json()
                    logging.info('RESULT #%d: %r', retry, result)
                    break
                else:
                    result = '%s: %s' % (result.status_code, result)
            except Exception as e:
                result = str(e)
            logging.info('RESULT #%d: %r', retry, result)
            time.sleep(60)
    else:
        logging.info('SKIPPING %s as has no relevant subscriptions', row['email'])


if __name__ == '__main__':
    process(process_row=process_row)


