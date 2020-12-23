import os
from decimal import Decimal
import json

from sqlalchemy import create_engine
from sqlalchemy.sql import text

import dataflows as DF

engine = None  

MAPPINGS = {
    'activities/שירות חברתי/משרד החינוך/תכנית קדם עתידים': [
        dict(code='0020460242', year=2019, part=100)
    ],
    'activities/שירות חברתי/משרד הבריאות/מכשירי שיקום וניידות – אספקה, התאמה, תיקון וחלוקת מכשירי שיקום וניידות': [
        dict(code='0024070311', year=2019, part=50),
        dict(code='0024070511', year=2017, part=50),
    ],
    'activities/שירות חברתי/משרד הבריאות/שיקום נכי נפש בקהילה- שירותי שיקום בדיור (הוסטלים)': [
        dict(code='0024071460', year=2019, part=100)
    ],
    'activities/שירות חברתי/משרד הבריאות/אשפוזיות גמילה מסמים, אלכוהול וחומרים פסיכואקטיביים': [
        dict(code='0024071456', year=2019, part=50)
    ],
    'activities/שירות חברתי/משרד הרווחה/תעסוקה מוגנת': [
        dict(code='0023062262', year=2019, part=100)
    ],
    'activities/שירות חברתי/משרד החינוך/שירותי הפעלת וועדות מומחים יעוץ והדרכה ושירותים פסיכולוגיים': [
        dict(code='0020460306', year=2019, part=50),
        dict(code='0020460308', year=2019, part=50)
    ],
}

def expand_mappings(mappings):
    ret = []
    for mapping in mappings:
        TITLE_QUERY = text('SELECT title from raw_budget where code=:code and year=:year')
        mapping['title'] = engine.execute(TITLE_QUERY, **mapping).fetchone().title
        ITEMS_QUERY = text('SELECT year, code, title, net_allocated, net_revised, net_executed from raw_budget where code=:code and title=:title and net_revised > 0')
        for r in engine.execute(ITEMS_QUERY, **mapping).fetchall():
            ret.append(dict(
                code=r.code,
                year=r.year,
                title=r.title,
                net_allocated=r.net_allocated,
                net_revised=r.net_revised,
                net_executed=r.net_executed,
                part=mapping['part']
            ))
    return ret


def fetch_spending(budget_code):
    SPENDING = text('''
        SELECT volume, executed, currency,
               min_year, max_year,
               purpose,
               'contract-spending/' || publisher_name || '/' || order_id || '/' || budget_code AS cs_item_id,
               case when entity_name is null then supplier_name->>0 else entity_name end as supplier, 
               case when entity_id is null 
               then ('s?q=' || (supplier_name->>0))
               else ('i/org/' || entity_kind || '/' || entity_id) end as entity_item_id,
               purchase_method->>0 AS purchase_method,
               ((publisher->>0) || '/' || (purchasing_unit->>0)) AS purchasing_unit,
               order_date, start_date, end_date,
               tender_key
               FROM contract_spending
               WHERE budget_code=:code
               ORDER BY volume desc nulls last
    ''')
    return [dict(r) for r in engine.execute(SPENDING, code=budget_code).fetchall()]


def fetch_tenders(**kw):
    TENDER = text('''
        SELECT publication_id, tender_id, tender_type, tender_type_he,
               start_date, claim_date, last_update_date, end_date,
               contact, contact_email,
               decision, description, reason, regulation, page_title, page_url,
               publisher, publisher_id, 
               entity_id, entity_kind, entity_name, volume, contract_volume
        FROM procurement_tenders_processed
        WHERE publication_id=:publication_id AND tender_id=:tender_id AND tender_type=:tender_type
    ''')
    return dict(engine.execute(TENDER, **kw).fetchone())

def format_date(x):
    if x:
        return x.strftime('%d/%m/%Y')
    else:
        return ''


def fetch_extra_data(row):
    if row['doc_id'] in MAPPINGS:
        mappings = MAPPINGS[row['doc_id']]
        mappings = expand_mappings(mappings)
        
        budget_composition = dict(
            title='תקנות תקציביות',
            long_title='מהן התקנות התקציביות מהן יוצא התקציב?',
            type='template',
            template_id='table',
            chart=dict(
                item=dict(
                    headers=['שנה', 'קוד', 'כותרת', 'אחוז תרומה לתקציב'],
                    data=[
                        [
                            r['year'],
                            '.'.join(r['code'][i:i+2] for i in range(2, 10, 2)),
                            '<a href="/i/budget/{code}/{year}">{title}</a>'.format(**r),
                            '{part}%'.format(**r),
                        ]
                        for r in sorted(
                            mappings,
                            key=lambda m: '{year}/{code}'.format(**m)
                        )
                    ]
                )
            )
        )

        # Budget
        budget = dict()
        for mapping in mappings:
            year = mapping['year']
            budget.setdefault(year, dict(year=year))
            for f in ('net_allocated', 'net_revised', 'net_executed'):
                if mapping[f] is not None:
                    budget[year].setdefault(f, 0)
                    budget[year][f] += int(mapping[f]) * mapping['part'] / 100
        budget = sorted(budget.values(), key=lambda x: x['year'])
        budget_history = dict(
            title='התקציב המוקצה לשירות זה',
            long_title='מה היה התקציב שהוקצה לשירות זה במהלך השנים?',
            type='plotly',
            chart=[
                dict(
                    x=[i['year'] for i in budget],
                    y=[i.get(measure) for i in budget],
                    mode='lines+markers',
                    name=name
                )
                for measure, name in (
                    ('net_allocated', 'תקציב מקורי'),
                    ('net_revised', 'אחרי שינויים'),
                    ('net_executed', 'ביצוע בפועל')
                )                
            ],
            layout=dict(
                xaxis=dict(
                    title='שנה',
                    type='category'
                ),
                yaxis=dict(
                    title='תקציב ב-₪',
                    rangemode='tozero',
                    separatethousands=True
                )
            )
        )

        # Spending
        budget_codes = list(set(r['code'] for r in mappings))
        
        spending = []
        for budget_code in budget_codes:
            spending.extend(fetch_spending(budget_code))

        top_contracts = dict(
            title='התקשרויות',
            long_title='אילו התקשרויות רכש משויכות לשירות זה?',
            description='100 ההתקשרויות בעלות ההיקף הגדול ביותר מוצגות מתוך {}'.format(len(spending)) if len(spending) > 100 else None,
            type='template',
            template_id='table',
            chart=dict(
                item=dict(
                    headers=['יחידה רוכשת', 'ספק', 'כותרת', 'היקף', 'ביצוע', 'אופן רכישה', 'מועד הזמנה', 'מועד סיום'],
                    data=[
                        [
                            r['purchasing_unit'],
                            '<a href="/{entity_item_id}">{supplier}</a>'.format(**r),
                            r['purpose'],
                            '₪{volume:,.2f}'.format(**r),
                            '₪{executed:,.2f}'.format(**r),
                            r['purchase_method'],
                            format_date(r['order_date']),
                            format_date(r['end_date']),
                        ]
                        for r in spending[:100]
                    ]
                )
            )
        )

        per_tender_spending = dict()
        for r in spending:
            if r.get('tender_key'):
                tks = r['tender_key']
                tks = [tuple(json.loads(t)) for t in tks]
                for tk in tks:
                    dd = per_tender_spending.setdefault(tk, dict(svc_executed=0, svc_volume=0))
                    dd['svc_executed'] += r['executed']
                    dd['svc_volume'] += r['volume']

        # Suppliers
        suppliers_grouped = dict()
        for c in spending:
            suppliers_grouped.setdefault(c['entity_item_id'], []).append(c)
        supplier_table = []
        for eid, contracts in suppliers_grouped.items():
            max_years = [x['max_year'] for x in contracts if x['max_year']]
            min_years = [x['min_year'] for x in contracts if x['min_year']]
            supplier_table.append([
                '<a href="/{eid}">{supplier}</a>'.format(eid=eid, supplier=max(x['supplier'] for x in contracts)),
                '₪{:,.2f}'.format(sum(x['volume'] for x in contracts)),
                '₪{:,.2f}'.format(sum(x['executed'] for x in contracts)),
                '{}-{}'.format(
                    min(min_years) if min_years else '',
                    max(max_years) if max_years else '',
                )
            ])
        top_suppliers = dict(
            title='ספקים',
            long_title='מול אילו ספקים קיימות התקשרויות במסגרת שירות זה?',
            type='template',
            template_id='table',
            chart=dict(
                item=dict(
                    headers=[
                        'שם הספק',
                        'סך היקף ההתקשרויות',
                        'סך ביצוע ההתקשרויות',
                        'תקופת הפעילות',],
                    data=sorted(supplier_table, key=lambda x: float(x[1][1:].replace(',', '')), reverse=True)
                )
            )
        )

        # Tenders
        tender_keys = []
        for x in spending:
            if x['tender_key']:
                tk = [tuple(json.loads(t)) for t in x['tender_key']]
                tender_keys.extend(tk)
        tender_keys = list(set(tender_keys))
        tenders = []
        for tk in tender_keys:
            tender = fetch_tenders(publication_id=tk[0], tender_type=tk[1], tender_id=tk[2])
            tender.update(per_tender_spending[tk])
            tenders.append(tender)
        top_tenders = dict(
            title='מכרזים',
            long_title='אילו מכרזים משויכים לשירות זה?',
            type='template',
            template_id='table',
            chart=dict(
                item=dict(
                    headers=[
                        'מפרסם',
                        'סוג המכרז',
                        'סטטוס',
                        'כותרת',
                        'סך התקשרויות בשירות זה',
                        'פרסום במנו״ף',
                        'מועד תחילה',
                        'מועד סיום',
                        'לפי תקנה'],
                    data=[
                        [
                            r['publisher'],
                            r['tender_type_he'],
                            r['decision'],
                            '<a href="/i/tenders/{tender_type}/{publication_id}/{tender_id}">{description}</a>'.format(**r),
                            '₪{svc_volume:,.2f}'.format(**r),
                            '<a href="{page_url}">{publication_id}</a>'.format(**r),
                            format_date(r['start_date']),
                            format_date(r['end_date']),
                            r['regulation'],
                        ]
                        for r in sorted(tenders, key=lambda r: r['svc_volume'] or 0, reverse=True)
                    ]
                )
            )
        )

        row['charts'] = [
            budget_history,
            top_tenders,
            top_suppliers,
            top_contracts,
            budget_composition,
        ]


def flow(*_):
    global engine
    engine = create_engine(os.environ['DPP_DB_ENGINE'])
    return DF.Flow(
        DF.add_field(
            'charts', 'array', default=[], **{
                'es:itemType': 'object',
                'es:index': False
            }
        ),
        fetch_extra_data
    )

if __name__ == '__main__':
    os.environ['DPP_DB_ENGINE'] = 'postgresql://readonly:readonly@data-next.obudget.org/budgetkey'
    DF.Flow(
        [{'doc_id': doc_id} for doc_id in MAPPINGS.keys()],
        flow(),
        DF.printer()
    ).process()