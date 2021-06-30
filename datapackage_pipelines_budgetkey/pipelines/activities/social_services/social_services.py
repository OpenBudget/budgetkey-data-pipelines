import dataflows as DF
from decimal import Decimal


def datarecords(kind):
    return map(
        lambda r: r['value'],
        DF.Flow(
            DF.load(f'https://data-input.obudget.org/api/datarecords/{kind}', format='json', property='result')
        ).results()[0][0]
    )

def services():
    for k in datarecords('social_service'):
        for f in ['target_audience', 'target_age_group', 'intervention', 'subject', 'manualBudget']:
            k.setdefault(f, [])
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

def fix_suppliers():
    geo = fetch_codelist('geo_region')
    def func(row):
        for v in row.get('suppliers') or []:
            v['geo'] = [geo[i] for i in v.get('geo', [])]
            start_year = v.get('year_activity_start') or 2020
            end_year = v.get('year_activity_start') or 2020
            v['activity_years'] = list(range(start_year, end_year+1))
    return func

def get_score(r):
    mb = r.get('manualBudget')
    if mb and len(mb):
        if mb[0]['approved']:
            return mb[0]['approved']/1000
    return 1000

def add_current_budget():
    def func(row):
        if row.get('manualBudget') and len(row.get('manualBudget')) > 0:
            row['current_budget'] = row['manualBudget'][0]['approved']

    return DF.Flow(
        DF.add_field('current_budget', 'number'),
        func
    )


def flow(*_):
    return DF.Flow(
        services(),
        DF.delete_fields(['__tab', 'complete', 'non_suppliers', 'non_tenders', 'notes', ]),
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
        fix_suppliers(),
        add_current_budget(),
        DF.add_field('min_year', 'integer', 2020),
        DF.add_field('max_year', 'integer', 2020),
        DF.add_field('kind', 'string', 'gov_social_service', **{'es:keyword': True, 'es:exclude': True}),
        DF.add_field('kind_he', 'string', 'שירות חברתי', **{'es:keyword': True, 'es:exclude': True}),
        DF.set_type('name',  **{'es:title': True}),
        DF.set_type('description', **{'es:itemType': 'string', 'es:boost': True}),
        DF.add_field('score', 'number', get_score, **{'es:score-column': True}),
        DF.set_primary_key(['kind', 'id']),
        DF.update_resource(-1, name='activities', **{'dpp:streaming': True}),
        DF.dump_to_path('/var/datapackages/activities/social_services'),
        DF.dump_to_sql(dict(
            activities={'resource-name': 'activities'}
        ))
    )


if __name__ == '__main__':
    DF.Flow(
        flow(),
        DF.printer(),
    ).process()
