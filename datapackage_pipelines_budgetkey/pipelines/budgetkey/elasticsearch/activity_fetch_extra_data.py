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
    ]
}

def expand_mappings(mappings):
    ret = []
    for mapping in mappings:
        TITLE_QUERY = text('SELECT title from raw_budget where code=:code and year=:year')
        mapping['title'] = engine.execute(TITLE_QUERY, **mapping).fetchone().title
        ITEMS_QUERY = text('SELECT year, code, title, net_allocated, net_revised, net_executed from raw_budget where code=:code and title=:title')
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
               min_year||'-'||max_year AS period,
               purpose,
               'contract-spending/' || publisher_name || '/' || order_id || '/' || budget_code AS cs_item_id,
               case when entity_name is null then supplier_name->>0 else entity_name end as supplier, 
               case when entity_id is null then null else ('org/' || entity_kind || '/' || entity_id) end as entity_item_id,
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
               entity_id, entity_kind, entity_name, volume
        FROM procurement_tenders_processed
        WHERE publication_id=:publication_id AND tender_id=:tender_id AND tender_type=:tender_type
        ORDER BY claim_date desc nulls last
    ''')
    return dict(engine.execute(TENDER, **kw).fetchone())


def fetch_extra_data(row):
    if row['doc_id'] in MAPPINGS:
        charts = []

        mappings = MAPPINGS[row['doc_id']]
        mappings = expand_mappings(mappings)
        
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
        charts.append(dict(
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
        ))

        # Spending
        budget_codes = list(set(r['code'] for r in mappings))
        
        spending = []
        for budget_code in budget_codes:
            spending.extend(fetch_spending(budget_code))

        # Tenders
        tender_keys = []
        for x in spending:
            if x['tender_key']:
                tk = [tuple(json.loads(t)) for t in x['tender_key']]
                tender_keys.extend(tk)
        tender_keys = list(set(tender_keys))
        tenders = []
        for tk in tender_keys:
            tenders.append(fetch_tenders(publication_id=tk[0], tender_type=tk[1], tender_id=tk[2]))
            print(tenders[-1]['tender_type'], tenders[-1]['volume'], tenders[-1]['description'])

        row['charts'] = charts
        print(charts)


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
        [{'doc_id': 'activities/שירות חברתי/משרד החינוך/תכנית קדם עתידים'}],
        flow(),
        DF.printer()
    ).process()