from datapackage_pipelines.wrapper import process

import os
import logging
import requests
import urllib.parse
import json
import time
import datetime
import sqlalchemy

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
        'dd=opportunities&theme=socialmap&kind=calls_for_bids&focused=closing',
        ['calls_for_bids']
    ),
    (
        'קולות קוראים חדשים',
        'קולות קוראים חדשים שעשויים לעניין אותך',
        'dd=opportunities&theme=socialmap&kind=calls_for_bids&focused=new',
        ['calls_for_bids']
    ),
    (
        'מבחני תמיכה חדשים',
        'מבחני תמיכה חדשים שעשויים לעניין אותך',
        'dd=opportunities&theme=socialmap&kind=support_criteria&focused=new',
        ['support_criteria']
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


e = sqlalchemy.create_engine(os.environ['PRIVATE_DATABASE_URL']).connect()


def update_db(email, result):
    t = sqlalchemy.sql.text("insert into sendlog (email, send_time, result) values (:email, :send_time, :result)")
    e.execute(t, email=email, send_time=datetime.datetime.now(), result=result)


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    logging.info('ROW: %r', row)
    items = row['items']
    sections = []
    stats.setdefault('subscriptions', 0)
    stats.setdefault('errors', 0)
    stats['subscriptions'] += 1
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
        logging.info('DATAS: #%s - %r', row_index, ret)
        for retry in range(2):
            try:
                result = requests.post('http://budgetkey-emails:8000/',
                                       json=ret,
                                       timeout=630)
                if result.status_code == 200:
                    stats.setdefault('sent', 0)
                    stats['sent'] += 1
                    result = result.json()
                    update_db(row['email'], json.dumps(result))
                    logging.info('RESULT #%d: %r', retry, result)
                    break
                else:
                    stats['errors'] += 1
                    result = '%s: %s' % (result.status_code, result)
            except Exception as e:
                result = str(e)
            stats['errors'] += 1
            logging.info('RESULT #%d: %r', retry, result)
            time.sleep(60)
    else:
        stats.setdefault('skipped', 0)
        stats['skipped'] += 1
        logging.info('SKIPPING %s as has no relevant subscriptions',
                     row['email'])


if __name__ == '__main__':
    process(process_row=process_row)
