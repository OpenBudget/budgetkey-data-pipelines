import dataflows as DF
from decimal import Decimal
import datetime

CURRENT_YEAR = 2021

def datarecords(kind):
    return map(
        lambda r: dict(r['value'], created_at=r['created_at'], updated_at=r['updated_at']),
        DF.Flow(
            DF.load(f'https://data-input.obudget.org/api/datarecords/{kind}', format='json', property='result')
        ).results()[0][0]
    )

def services():
    for k in datarecords('social_service'):
        for f in ['target_audience', 'target_age_group', 'intervention', 'subject', 'manualBudget']:
            k.setdefault(f, [])
        k.setdefault('catalog_number', None)
        k.setdefault('deleted', False)
        yield k

def fetch_codelist(dr_name):
    codelist = datarecords(dr_name)
    codelist = dict((x.pop('id'), x.pop('name')) for x in codelist)
    return codelist


def splitter(field_name, dr_name=None):
    dr_name = dr_name or field_name
    codelist = fetch_codelist(dr_name)
    print(field_name, codelist)
    def func(row):
        row[field_name] = [codelist[i] for i in row[field_name] or []]
    return DF.Flow(
        func,
        DF.set_type(field_name, **{'es:keyword': True, 'es:itemType': 'string'})
    )

def floater(field):
    def func(row):
        val = row.get(field)
        if val and isinstance(val, list):
            n = []
            for i in val:
                n.append(dict(
                    (k, float(v) if isinstance(v, Decimal) else v)
                    for k, v in i.items()
                ))
            row[field] = n
    return DF.Flow(
        func,
        DF.set_type(field, **{'es:itemType': 'object', 'es:index': False})
    )

def fix_tenders():
    today = datetime.date.today().isoformat()

    def func(row):
        tenders = row.get('tenders') or []
        for tender in tenders:
            date_range = tender.get('date_range')
            end_date = tender.get('end_date')
            if end_date:
                end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')
            option_duration = tender.get('option_duration')
            if end_date and option_duration:
                option_duration = int(option_duration)
                end_date_extended = datetime.date(end_date.year + option_duration, end_date.month, end_date.day)
                tender['end_date_extended'] = end_date_extended.isoformat()
            if date_range and len(date_range) > 22 and not end_date:
                tender['end_date'] = date_range[-10:]
            tender['sub_kind_he'] = 'אחר'
            if tender.get('sub_kind'):
                tender['sub_kind_he'] = dict(
                    regular='מכרז רגיל',
                    closed='מכרז סגור',
                    frame='מכרז מסגרת',
                    pool='מכרז מאגר',
                    na='אחר'
                )[tender.get('sub_kind')]
            elif tender.get('regulation'):
                reg = tender.get('regulation')
                orgs = [
                    'הקרן הקיימת לישראל',
                    "ג'וינט ישראל",
                    'ההסתדרות הציונית',
                    'הסוכנות היהודית',
                ]
                if 'תקנה 3(4)(ב)' in reg or 'התקשרות המשך' in reg:
                    tender['sub_kind_he'] = 'התקשרות המשך'
                elif 'מכרז סגור' in reg:
                    tender['sub_kind_he'] = 'מכרז סגור'
                elif 'ספק יחיד' in reg:
                    tender['sub_kind_he'] = 'ספק יחיד'
                elif 'מימוש זכות ברירה' in reg:
                    tender['sub_kind_he'] = 'מימוש אופציה'
                elif 'מיזם משותף' in reg or any(org in reg for org in orgs):
                    tender['sub_kind_he'] = 'מיזם משותף'
                elif 'רשות מקומית' in reg:
                    tender['sub_kind_he'] = 'התקשרות עם רשות מקומית'
                else:
                    print('REGREG', reg)
            end_date = tender.get('end_date')
            if tender.get('tender_type') == 'exemptions' and tender.get('active') is None and end_date:
                tender['active'] = 'yes' if end_date > today else 'no'

    return func

def fix_suppliers():
    geo = fetch_codelist('geo_region')
    def func(row):
        kinds = set()
        suppliers = row.get('suppliers') or []
        eids = set()
        eids_association = set()
        eids_company = set()
        eids_municipality = set()
        geos = set()
        actual_suppliers = []
        for v in suppliers:
            for f in ['entity_id', 'entity_name']:
                if v.get(f):
                    v[f] = v[f].replace('<em>', '').replace('</em>', '')
            for f in ('year_activity_start', 'year_activity_end'):
                if f in v and not v[f]:
                    del v[f]
            start_year = max(v.get('year_activity_start') or 2020, row['min_activity_year'])
            end_year = min(v.get('year_activity_end') or CURRENT_YEAR, row['max_activity_year'])
            v['activity_years'] = list(range(start_year, end_year+1))
            if len(v['activity_years']) == 0:
                continue
            actual_suppliers.append(v)
            v['geo'] = [geo[i] for i in v.get('geo', [])]
            if v.get('year_activity_end') is None: # still active, so counted
                geos.update(v['geo'])
                eid = v['entity_id']
                eids.add(eid)
                ekind = v['entity_kind']
                if ekind == 'company':
                    kinds.add('עסקי')
                    eids_company.add(eid)
                elif ekind in ('association', 'ottoman-association', 'cooperative'):
                    kinds.add('מגזר שלישי')
                    eids_association.add(eid)
                elif ekind == 'municipality':
                    kinds.add('רשויות מקומיות')
                    eids_municipality.add(eid)
                else:
                    kinds.add('אחר')
        row['suppliers'] = actual_suppliers
        row['supplier_count'] = len(eids)
        row['supplier_count_company'] = len(eids_company)
        row['supplier_count_association'] = len(eids_association)
        row['supplier_count_municipality'] = len(eids_municipality)
        row['geo_coverage'] = 'ארצי' if 'ארצי' in geos else 'אזורי'

        if len(kinds) == 0:
            row['supplier_kinds'] = None
        elif len(kinds) == 1:
            row['supplier_kinds'] = kinds.pop()
        else:
            row['supplier_kinds'] = 'משולב'
        if len(suppliers) == 0:
            row['supplier_count_category'] = None
        elif len(suppliers) == 1:
            row['supplier_count_category'] = '1'
        elif 2 <= len(suppliers) <= 5:
            row['supplier_count_category'] = '2-5'
        else:
            row['supplier_count_category'] = '6+'

    return DF.Flow(
        DF.add_field('supplier_count_category', 'string'),
        DF.add_field('supplier_kinds', 'string'),
        DF.add_field('supplier_count', 'integer'),
        DF.add_field('supplier_count_company', 'integer'),
        DF.add_field('supplier_count_association', 'integer'),
        DF.add_field('supplier_count_municipality', 'integer'),
        DF.add_field('geo_coverage', 'string'),
        func
    )

def get_score(r):
    mb = r.get('current_budget')
    if mb:
        return mb/1000
    return 1000

def add_current_budget():
    def func(row):
        row['current_budget'] = None
        if row.get('manualBudget') and len(row.get('manualBudget')) > 0:
            for entry in row['manualBudget']:
                if entry.get('approved') and entry['approved'] > 0 and entry['year'] == CURRENT_YEAR:
                    row['current_budget'] = entry['approved']
                    row['min_activity_year'] = min(entry['year'], row.get('min_activity_year', entry['year']))
                    row['max_activity_year'] = max(entry['year'], row.get('max_activity_year', entry['year']))
                    break
        utilization = None
        for item in row['manualBudget']:
            if item['approved'] and item['executed'] and item['year'] == CURRENT_YEAR:
                utilization = item['executed']/item['approved']*100
                break
        row['budget_utilization'] = utilization

    return DF.Flow(
        DF.add_field('current_budget', 'number'),
        DF.add_field('budget_utilization', 'number'),
        DF.add_field('min_activity_year', 'number'),
        DF.add_field('max_activity_year', 'number'),
        func
    )

def add_current_beneficiaries():
    kinds = fetch_codelist('beneficiary_kind')
    def func(row):
        if row.get('beneficiaries') and len(row.get('beneficiaries')) > 0:
            for b in row.get('beneficiaries'):
                if b.get('num_beneficiaries') and b['year'] <= CURRENT_YEAR:
                    row['current_beneficiaries'] =  b.get('num_beneficiaries')
                    break

    return DF.Flow(
        DF.add_field('current_beneficiaries', 'integer'),
        DF.add_field('beneficiary_kind_name', 'string', lambda r: kinds.get(r['beneficiary_kind'], 'מקבלי שירות').replace('אנשים', 'מקבלי שירות')),
        func
    )


def flow(*_):
    now = datetime.datetime.now()

    return DF.Flow(
        services(),
        DF.set_type('created_at', type='datetime', transform=lambda x: datetime.datetime.strptime(x[:19], '%Y-%m-%dT%H:%M:%S')),
        DF.set_type('updated_at', type='datetime', transform=lambda x: datetime.datetime.strptime(x[:19], '%Y-%m-%dT%H:%M:%S')),
        DF.delete_fields(['__tab', 'complete', 'non_suppliers', 'non_tenders', 'notes']),
        DF.add_field('publisher_name', 'string', lambda r: r['office'], **{'es:keyword': True}),
        splitter('target_audience'),
        splitter('subject'),
        splitter('intervention'),
        splitter('target_age_group'),
        floater('beneficiaries'),
        floater('budgetItems'),
        floater('manualBudget'),
        floater('tenders'),
        floater('suppliers'),
        floater('virtue_of_table'),
        add_current_budget(),
        fix_suppliers(),
        fix_tenders(),
        add_current_beneficiaries(),
        DF.add_field('min_year', 'integer', 2020),
        DF.add_field('max_year', 'integer', CURRENT_YEAR),
        DF.add_field('kind', 'string', 'gov_social_service', **{'es:keyword': True, 'es:exclude': True}),
        DF.add_field('kind_he', 'string', 'שירות חברתי', **{'es:keyword': True, 'es:exclude': True}),
        DF.set_type('name',  **{'es:title': True}),
        DF.set_type('description', **{'es:itemType': 'string', 'es:boost': True}),
        DF.add_field('score', 'number', get_score, **{'es:score-column': True}),
        DF.set_primary_key(['kind', 'id']),
        DF.update_resource(-1, name='activities', path='activities.csv'),
        DF.dump_to_sql(dict(
            all_activities={'resource-name': 'activities'}
        )),
        DF.filter_rows(lambda r: not r['deleted']),
        DF.delete_fields(['deleted']),

        DF.duplicate('activities', 'new_activities'),
        DF.filter_rows(lambda r: (now - r['updated_at']).days < 14, resources='new_activities'),

        DF.dump_to_path('/var/datapackages/activities/social_services', format='json'),
        DF.delete_resource(['new_activities']),
        DF.dump_to_path('/var/datapackages/activities/social_services'),
        DF.dump_to_sql(dict(
            activities={'resource-name': 'activities'}
        )),
        DF.update_resource(None, **{'dpp:streaming': True}),
    )


if __name__ == '__main__':
    DF.Flow(
        flow(),
        DF.printer(),
    ).process()
