import dataflows as DF
from pathlib import Path
from decimal import Decimal


DATACITY_DB = 'postgresql://readonly:readonly@db.datacity.org.il:5432/datasets'

def get_code_to_entity_id_map():
    rows = DF.Flow(
        DF.load('https://next.obudget.org/datapackages/lamas-municipal-data/datapackage.json'),
        DF.select_fields(['symbol_municipality_2015', 'entity_id'])
    ).results()[0][0]
    return dict(
        (row['symbol_municipality_2015'], row['entity_id'])
        for row in rows
    )


def get_score(row):
    return (row['revised'] or row['executed'] or row['allocated'] or Decimal(1000)) / Decimal(1000.0)


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
    eid_map = get_code_to_entity_id_map()
    DF.Flow(
        DF.load('/var/datapackages/budget/municipal/datacity-budgets-raw/datapackage.json'),
        DF.add_field('parent', 'string', default=lambda row: parent(row['code'])),
        DF.add_field('entry', 'object', default=lambda row: dict(
            code=row['code'],
            title=row['title'],
            allocated=float(row['allocated']) if row['allocated'] else None,
            revised=float(row['revised']) if row['revised'] else None,
            executed=float(row['executed']) if row['executed'] else None,
        )),
        DF.select_fields(['parent', 'entry', 'code', 'muni_code', 'year']),
        DF.update_resource(-1, name='hierarchy'),
        DF.dump_to_path('/var/datapackages/budget/municipal/datacity-budgets-hierarchy'),
    ).process()
    return DF.Flow(
        DF.load('/var/datapackages/budget/municipal/datacity-budgets-hierarchy/datapackage.json'),
        DF.load('/var/datapackages/budget/municipal/datacity-budgets-raw/datapackage.json'),
        DF.join('hierarchy', ['parent', 'muni_code', 'year'], 
                'muni_budgets', ['code', 'muni_code', 'year'],
                dict(
                    children=dict(name='entry', aggregate='array')
                ),
                source_delete=False
        ),

        DF.add_field('parent1code', 'string', default=lambda row: parent(row['code']), resources=-1),
        DF.add_field('parent2code', 'string', default=lambda row: parent(row['parent1code']), resources=-1),
        DF.add_field('parent3code', 'string', default=lambda row: parent(row['parent2code']), resources=-1),
        DF.join('hierarchy', ['code', 'muni_code', 'year'], 
                'muni_budgets', ['parent1code', 'muni_code', 'year'],
                dict(
                    parent1entry=dict(name='entry')
                ),
                source_delete=False
        ),
        DF.join('hierarchy', ['code', 'muni_code', 'year'], 
                'muni_budgets', ['parent2code', 'muni_code', 'year'],
                dict(
                    parent2entry=dict(name='entry')
                ),
                source_delete=False
        ),
        DF.join('hierarchy', ['code', 'muni_code', 'year'], 
                'muni_budgets', ['parent3code', 'muni_code', 'year'],
                dict(
                    parent3entry=dict(name='entry')
                )
        ),
        DF.add_field('breadcrumbs', 'array', default=lambda row: [x for x in (row[f'parent{x}entry'] for x in (3,2,1)) if x]),
        DF.delete_fields(['parent.+']),

        DF.add_field('history', 'object', 
            lambda row: dict(
                year=row['year'],
                title=row['title'],
                allocated=float(row['allocated']) if row['allocated'] else None,
                revised=float(row['revised']) if row['revised'] else None,
                executed=float(row['executed']) if row['executed'] else None,
                children=row.get('children'),
            )),
        DF.sort_rows('{year}'),
        DF.join_with_self('muni_budgets', ['code', 'muni_code'], dict(
            muni_code=None,
            muni_name=dict(aggregate='last'),
            code=None,
            title=None,
            year=dict(aggregate='last'),
            direction=dict(aggregate='last'),
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
        DF.add_field('entity_id', 'string', default=lambda row: eid_map.get(row['muni_code'])),
        DF.add_field('entity_doc_id', 'string', default=lambda row: f'org/municipality/{row["entity_id"]}'),
        DF.add_field('children', 'array'),
        lambda row: row.update(row['history'][-1]),
        DF.set_type('.*code', type='string'),
        DF.set_type('.*name', type='string'),
        DF.set_type('title', type='string'),
        DF.set_type('direction', type='string'),
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