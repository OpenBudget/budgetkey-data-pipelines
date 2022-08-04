import dataflows as DF
from pathlib import Path
from decimal import Decimal


DATACITY_DB = 'postgresql://readonly:readonly@db.datacity.org.il:5432/datasets'


def get_score(row):
    return (row['revised'] or row['executed'] or row['allocated'] or Decimal(1000)) / Decimal(1000.0)

def codetitle(cts):
    ret = []
    for ct in cts:
        if ct is None:
            continue
        code, title = ct.split(':', 1)
        ret.append(dict(code=code, title=title))
    return ret

def parent(code):
    if code in ('EXPENDITURE', 'REVENUE', None):
        return None
    if len(code) == 1:
        return None
    if len(code) < 4:
        return code[:-1]
    return code[1:4]
    

def flow(*_):
    QUERY = Path(__file__).with_name('query.sql').read_text()
    DF.Flow(
        DF.load(DATACITY_DB, query=QUERY, name='muni_budgets'),
        DF.set_type('.*code', type='string'),
        DF.dump_to_path('/var/datapackages/budget/municipal/datacity-budgets-raw'),
    ).process()
    DF.Flow(
        DF.load('/var/datapackages/budget/municipal/datacity-budgets-raw/datapackage.json'),
        DF.add_field('parent', 'string', default=lambda row: parent(row['code'])),
        DF.add_field('code_title', 'string', default=lambda row: '{}:{}'.format(row['code'], row['title'])),
        DF.select_fields(['parent', 'code_title', 'code', 'muni_code', 'year']),
        DF.update_resource(-1, name='hierarchy'),
        DF.dump_to_path('/var/datapackages/budget/municipal/datacity-budgets-hierarchy'),
    ).process()
    return DF.Flow(
        DF.load('/var/datapackages/budget/municipal/datacity-budgets-hierarchy/datapackage.json'),
        DF.load('/var/datapackages/budget/municipal/datacity-budgets-raw/datapackage.json'),
        DF.join('hierarchy', ['parent', 'muni_code', 'year'], 
                'muni_budgets', ['code', 'muni_code', 'year'],
                dict(
                    children=dict(name='code_title', aggregate='array')
                ),
                source_delete=False
        ),

        DF.add_field('parent1code', 'string', default=lambda row: parent(row['code']), resources=-1),
        DF.add_field('parent2code', 'string', default=lambda row: parent(row['parent1code']), resources=-1),
        DF.add_field('parent3code', 'string', default=lambda row: parent(row['parent2code']), resources=-1),
        DF.join('hierarchy', ['code', 'muni_code', 'year'], 
                'muni_budgets', ['parent1code', 'muni_code', 'year'],
                dict(
                    parent1codetitle=dict(name='code_title')
                ),
                source_delete=False
        ),
        DF.join('hierarchy', ['code', 'muni_code', 'year'], 
                'muni_budgets', ['parent2code', 'muni_code', 'year'],
                dict(
                    parent2codetitle=dict(name='code_title')
                ),
                source_delete=False
        ),
        DF.join('hierarchy', ['code', 'muni_code', 'year'], 
                'muni_budgets', ['parent3code', 'muni_code', 'year'],
                dict(
                    parent3codetitle=dict(name='code_title')
                )
        ),
        DF.add_field('breadcrumbs', 'array', default=lambda row: codetitle([row[f'parent{x}codetitle'] for x in (1,2,3)])),
        DF.delete_fields(['parent.+']),

        DF.add_field('history', 'object', 
            lambda row: dict(
                year=row['year'],
                title=row['title'],
                revised=float(row['revised']) if row['revised'] else None,
                executed=float(row['executed']) if row['executed'] else None,
                allocated=float(row['allocated']) if row['allocated'] else None,
                children=row.get('children'),
            )),
        DF.sort_rows('{year}'),
        DF.join_with_self('muni_budgets', ['code', 'muni_code'], dict(
            muni_code=None,
            muni_name=dict(aggregate='last'),
            code=None,
            title=None,
            year=dict(aggregate='last'),
            breadcrumbs=dict(aggregate='last'),
            func_1_code=dict(aggregate='last'),
            func_1_name=dict(aggregate='last'),
            func_2_code=dict(aggregate='last'),
            func_2_name=dict(aggregate='last'),
            func_3_code=dict(aggregate='last'),
            func_3_name=dict(aggregate='last'),
            allocated=dict(aggregate='last'),
            revised=dict(aggregate='last'),
            executed=dict(aggregate='last'),
            history=dict(aggregate='array'),
        )),
        DF.add_field('children', 'array'),
        lambda row: row.update(row['history'][-1]),
        DF.set_type('.*code', type='string'),
        DF.set_type('.*name', type='string'),
        DF.set_type('title', type='string'),
        DF.set_type('allocated', type='number'),
        DF.set_type('revised', type='number'),
        DF.set_type('executed', type='number'),
        DF.set_type('title', **{'es:title': True}),
        DF.set_type('muni_name', **{'es:boost': True}),
        DF.set_type('history', **{'es:itemType': 'object', 'es:index': False}),
        DF.set_type('breadcrumbs', **{'es:itemType': 'object', 'es:index': False}),
        DF.set_type('children', **{'es:itemType': 'object', 'es:index': False}),
        DF.add_field('score', 'number', get_score, **{'es:score-column': True}),
        DF.set_primary_key(['muni_code', 'code', 'year']),
        DF.update_resource(-1, **{'dpp:streaming': True}),
        DF.dump_to_path('/var/datapackages/budget/municipal/datacity-budgets'),
        DF.dump_to_sql(dict(
            muni_budgets={
                'resource-name': 'muni_budgets',
                'indexes_fields': [['muni_code', 'code', 'year']],
            }
        ))
    )

if __name__ == '__main__':
    DF.Flow(
        flow(),
        DF.printer()
    ).process()