import dataflows as DF
from pathlib import Path
from decimal import Decimal


DATACITY_DB = 'postgresql://readonly:readonly@db.datacity.org.il:5432/datasets'


def get_score(row):
    return (row['revised'] or row['executed'] or row['allocated'] or Decimal(1000)) / Decimal(1000.0)


def flow(*_):
    QUERY = Path(__file__).with_name('query.sql').read_text()
    return DF.Flow(
        DF.load(DATACITY_DB, query=QUERY, name='muni_budgets'),
        DF.set_type('.*code', type='string'),
        DF.add_field('history', 'object', 
            lambda row: dict(
                year=row['year'],
                revised=float(row['revised']) if row['revised'] else None,
                executed=float(row['executed']) if row['executed'] else None,
                allocated=float(row['allocated']) if row['allocated'] else None,
            )),
        DF.sort_rows('{year}'),
        DF.join_with_self('muni_budgets', ['code', 'title', 'muni_code'], dict(
            muni_code=None,
            muni_name=dict(aggregate='last'),
            code=None,
            title=None,
            year=dict(aggregate='last'),
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
        DF.set_type('.*code', type='string'),
        DF.set_type('.*name', type='string'),
        DF.set_type('title', type='string'),
        DF.set_type('allocated', type='number'),
        DF.set_type('revised', type='number'),
        DF.set_type('executed', type='number'),
        DF.set_type('title', **{'es:title': True}),
        DF.set_type('muni_name', **{'es:boost': True}),
        DF.set_type('history', **{'es:itemType': 'object', 'es:index': False}),
        DF.add_field('score', 'number', get_score, **{'es:score-column': True}),
        DF.set_primary_key(['muni_code', 'code', 'year']),
        DF.update_resource(-1, **{'dpp:streaming': True}),
        DF.dump_to_path('/var/datapackages/budget/municipal/datacity-budgets'),
    )

if __name__ == '__main__':
    DF.Flow(
        flow(),
        DF.printer()
    ).process()