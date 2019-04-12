from datapackage_pipelines.wrapper import process

import logging
import requests
import urllib.parse
import json
import time

SECTIONS = [
    (
        'מכרזים שנסגרים השבוע', 
        'הזדמנות אחרונה להגיש הצעות!',
        'dd=tenders&theme=govbuy&focused=closing',
        ['tenders']
    ),
    (
        'מכרזים חדשים',
        'מכרזים חדשים שעשויים לעניין אותך',
        'dd=tenders&theme=govbuy&focused=new',
        ['tenders']
    ),
    (
        'בקשות חדשות לפטור ממכרז',
        'משרדי ממשלה ויחידות פרסמו השבוע תהליכי רכש בפטור ממכרז בנושאים אלו',
        'dd=exemptions&theme=govbuy&focused=new',
        ['tenders']
    ),
    (
        'התקשרויות חדשות',
        'התקשרויות חדשות בנושאים שמעניינים אותך',
        'dd=contracts&theme=govbuy&focused=new',
        ['tenders']
    ),
    (
        'קולות קוראים שנסגרים השבוע', 
        'הזדמנות אחרונה להגיש הצעות!',
        'dd=opportunities&theme=socialmap&focused=closing',
        ['support_criteria', 'calls_for_bids']
    ),
    (
        'קולות קוראים ומבחני חדשים',
        'הזדמנויות חדשות שעשויות לעניין אותך',
        'dd=opportunities&theme=socialmap&focused=new',
        ['support_criteria', 'calls_for_bids']
    ),
    (
        'ומה חוץ מזה?',
        'עוד כמה עדכונים שקשורים בחיפושים השמורים שלך',
        'dd=tenders&theme=govbuy&focused=updated',
        ['tenders']
    ),
]


def query_url(term, filters):
    term = urllib.parse.quote_plus(term)
    return f'https://next.obudget.org/s/?q={term}&{filters}'


def process_row(row, *_):
    logging.info('ROW: %r', row)
    items = row['items']
    sections = []
    for header, subheader, filters, relevant_types in SECTIONS:
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
            doctypes = props.get('displayDocsTypes',
                                 props.get('docType', {}).get('types'))
            if doctypes is not None:
                if any(x in doctypes
                       for x in (*relevant_types, 'all')):
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
        logging.info('SKIPPING %s as has no relevant subscriptions',
                     row['email'])


if __name__ == '__main__':
    process(process_row=process_row)


