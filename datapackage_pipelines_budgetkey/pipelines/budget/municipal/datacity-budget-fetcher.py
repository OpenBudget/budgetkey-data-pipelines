import csv
import os
from collections import defaultdict

import dataflows as DF
from pathlib import Path
from decimal import Decimal


DATACITY_DB = 'postgresql://readonly:readonly@db.datacity.org.il:5432/datasets'
# dpp re-runs this flow when this file changes, not when the .sql files do: bump this with every query edit
QUERY_REVISION = 3
RAW = '/var/datapackages/budget/municipal/datacity-budgets-raw'
# The municipalities' budget pages, found by ./budget/municipal/budget_files/scraper
BUDGET_FILES = os.environ.get('MUNICIPAL_BUDGET_FILES_DATAPACKAGE',
                              '/var/datapackages/budget/municipal/budget_files/datapackage.json')
# Authorities renamed since datacity loaded their budgets (datacity still matches rows by the old name)
RENAMED = dict(
    (x['from'], x['to']) for x in
    csv.DictReader(open(Path(__file__).parents[2] / 'lamas' / 'renamed_authorities.csv'))
)
# datacity names a chapter (8, 81, 817) by the Interior Ministry's chart, but Jerusalem, Haifa and Tel Aviv number
# their chapters their own way: Tel Aviv's and Haifa's 817 holds their high schools, the Ministry's holds
# additional school services. chapter-names.csv names their chapters by what they hold, in the chart's words where
# one fits, so that a name means the same in every city
CHAPTER_NAMES = dict(
    ((x['muni_code'], x['code']), x['name']) for x in
    csv.DictReader(open(Path(__file__).with_name('chapter-names.csv')))
)


def name_chapters(rows):
    if rows.res.name != 'muni_budgets':
        yield from rows
        return
    for row in rows:
        for level in (1, 2, 3):
            name = CHAPTER_NAMES.get((row['muni_code'], row[f'func_{level}_code']))
            if name:
                row[f'func_{level}_name'] = name
        if row['code'] and len(row['code']) <= 3:
            row['title'] = CHAPTER_NAMES.get((row['muni_code'], row['code']), row['title'])
        yield row

def get_code_to_entity_id_map():
    rows = DF.Flow(
        DF.load('https://next.obudget.org/datapackages/lamas-municipal-data/datapackage.json'),
        DF.select_fields(['symbol_municipality_2015', 'entity_id'])
    ).results()[0][0]
    return dict(
        (row['symbol_municipality_2015'], row['entity_id'])
        for row in rows
    )


def cbs_symbol(authority_code):
    # muni_code as datacity writes it: regional councils are 55xx at the Interior Ministry and "xx" (two digits)
    # at the CBS; the others share one code, in four digits (Ofakim, 31, is "0031"; Nahal Sorek, 5531, is "31")
    code = int(authority_code)
    return '%02d' % (code - 5500) if 5500 < code < 5600 else '%04d' % code


def get_budget_pages():
    """CBS symbol -> the authority's budget page and entity id, from the budget files scraper."""
    rows = DF.Flow(
        DF.load(BUDGET_FILES, resources=['budget_pages']),
    ).results()[0][0]
    return dict(
        (cbs_symbol(row['authority_code']), row)
        for row in rows
        if row['authority_code']
    )


def rollup_sources(raw):
    """(muni_code, functional-classification code, year) -> the files of the budget items under it."""
    sources = defaultdict(set)

    def collect(rows):
        for row in rows:
            if row['source_files']:
                for level in ('func_1_code', 'func_2_code', 'func_3_code'):
                    if row[level]:
                        sources[(row['muni_code'], row[level], row['year'])].update(row['source_files'])
            yield row

    DF.Flow(DF.load(raw), collect).process()
    return sources


def get_score(row):
    val = float(row['revised'] or row['executed'] or row['allocated'] or 1000)/1000
    if val < 0:
        val = -val
    return Decimal(val)


def parent(code):
    if code in ('EXPENDITURE', 'REVENUE', None):
        return None
    if len(code) == 1:
        return None
    if len(code) < 4:
        return code[:-1]
    return code[1:4]
    

def nice_code(row):
    code = row['code']
    if len(code) < 4:
        return code
    else:
        return code[1:-3] + '.' + code[-3:]


def nice_breadcrumbs(row):
    items = []
    history = row['history']
    if history and len(history) > 0:
        if len(history) > 1:
            items.append('{} - {}'.format(
                history[0]['year'], history[-1]['year']
            ))
        else:
            items.append(str(history[0]['year']))
    bc = row['breadcrumbs']
    if bc and len(bc) > 0:
        items.extend(i['title'] for i in bc)
    items = [x for x in items if x]
    if len(items):
        return ' / '.join(items)
    return ''



def flow(*_):
    QUERY = Path(__file__).with_name('query.sql').read_text()
    SOURCES = Path(__file__).with_name('sources.sql').read_text()
    DF.Flow(
        DF.load(DATACITY_DB, query=SOURCES, name='sources'),
        DF.load(DATACITY_DB, query=QUERY, name='muni_budgets'),
        DF.set_type('.*code', type='string'),
        name_chapters,
        DF.join('sources', ['muni_code', 'code', 'year'],
                'muni_budgets', ['muni_code', 'code', 'year'],
                dict(source_files=None)),
        DF.dump_to_path(RAW),
    ).process()
    rollups = rollup_sources(RAW + '/datapackage.json')

    def fill_rollup_sources(row):
        if 'source_files' in row and not row['source_files']:
            row['source_files'] = sorted(rollups.get((row['muni_code'], row['code'], row['year']), [])) or None
    eid_map = get_code_to_entity_id_map()
    pages = get_budget_pages()

    def page(row):
        return pages.get(row['muni_code']) or {}
    DF.Flow(
        DF.load(RAW + '/datapackage.json'),
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
        DF.load(RAW + '/datapackage.json'),
        fill_rollup_sources,
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
                source_files=row['source_files'],
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
        DF.add_field('entity_id', 'string',
                     default=lambda row: eid_map.get(row['muni_code']) or page(row).get('entity_id')),
        DF.add_field('entity_doc_id', 'string', default=lambda row: f'org/municipality/{row["entity_id"]}'),
        lambda row: row.update(muni_name=RENAMED.get(row['muni_name'], row['muni_name'])),

        DF.add_field('nice-category', 'string', default=lambda row: row['func_1_name'] or 'סעיף ראשי'),
        DF.add_field('nice-code', 'string', default=nice_code),
        DF.add_field('nice-breadcrumbs', 'string', default=nice_breadcrumbs),

        # Provenance: the authority's web page holding its budget files, and the files datacity loaded
        # (our processed copies, in the budgetkey-files bucket) for each year - the doc's year here and
        # every year in history
        DF.add_field('source_page_url', 'string', default=lambda row: page(row).get('budget_page_url'),
                     **{'es:index': False}),
        DF.add_field('source_files', 'array', **{'es:itemType': 'string', 'es:index': False}),
        DF.add_field('children', 'array'),
        lambda row: row.update(row['history'][-1]),
        DF.set_type('.*code', type='string'),
        DF.set_type('.*name', type='string'),
        DF.set_type('title', type='string', transform=lambda v, row: v if v else f'סעיף {row["code"]}'),
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