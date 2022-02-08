import logging
import itertools
import os
import shutil

from decimal import Decimal

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from fuzzywuzzy import fuzz
import plyvel

from datapackage_pipelines.utilities.extended_json import json, LazyDict
from datapackage_pipelines.wrapper import ingest, spew

logging.getLogger().setLevel(logging.INFO)
curated = {}
errors = []

parameters, dp, res_iter = ingest()

CURRENT_DB = 'connected_items.db'
NEW_CURRENT_DB = 'new_connected_items.db'
UNMATCHED1 = 'unmatched1.db'
UNMATCHED2 = 'unmatched2.db'


def similar(s1, s2):
    return fuzz.partial_ratio(s1, s2) > 80


def process_curated(rows):
    for row in rows:
        curated[tuple(row['current'])] = tuple(row['previous'])


def put(db, key, value):
    assert value is not None
    enc = json.dumps(value)
    db.put(key.encode('utf8'), enc.encode('ascii'))


def get(db, key):
    v = db.get(key.encode('utf8'))
    if v is not None:
        return json.loads(v.decode('ascii'))


def delete(db, key):
    db.delete(key.encode('utf8'))


def iterate_values(db):
    for k, v in db:
        ret = json.loads(v)
        assert ret is not None
        yield ret


def normalize(obj):
    if isinstance(obj, (str, bool, int, float)):
        return obj
    elif isinstance(obj, (list, set)):
        return [normalize(i) for i in obj]
    elif isinstance(obj, dict):
        return dict(
            (k, normalize(v))
            for k, v in obj.items()
        )
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, LazyDict):
        return normalize(obj.inner)
    elif obj is None:
        return None
    assert False, 'Bad object %r' % obj


def update_equiv(e1, e2):
    for k, v in e2.items():
        if k != 'year' and isinstance(v, (int, float)):
            e1.setdefault(k, 0)
            e1[k] += v
    e1.setdefault('code_titles', [])
    if 'code' in e2:
        e1['code_titles'].append('%s:%s' % (e2['code'], e2['title']))
    else:
        e1['code_titles'].extend(e2['code_titles'])
    e1['code_titles'] = sorted(set(e1['code_titles']))


def calc_equivs(cur_year, rows, connected_items, new_connected_items, to_delete, unmatched_db):

    mapped_levels = {}
    unmatched_count = 0

    def append_unmatched(row):
        put(unmatched_db, '%08d' % (10000000 - unmatched_count), row)

    for iii, row in enumerate(rows):
        row = normalize(row)
        equivs = []
        parent = row['parent']
        children = row['children']

        if iii % 1000 == 0:
            logging.info('%d: Processed %d rows..., unmatched %d', cur_year, iii, unmatched_count)

        ids = [{'code': row['code'], 'title': row['title']}]
        while len(ids) > 0:
            logging.debug('%d/%r: ids: %r', cur_year, row['code'], ids)
            id = ids.pop(0)

            test_value = sum(
                abs(row[f])
                for f in ('net_allocated','gross_allocated','net_revised','commitment_allocated','net_used')
                if row.get(f) is not None
            )
            non_repeating = row.get('non_repeating', [])
            non_repeating = '1' in non_repeating and len(non_repeating) == 1
            if (test_value == 0 and not row['code'].endswith('99')) or non_repeating:
                if not row.get('code').startswith('C'):
                    append_unmatched(row)
                    unmatched_count += 1
                    row = None
                    break

            # Find curated record for id
            curated_items = curated.get((cur_year, id['code']))
            if curated_items is not None:
                if len(curated_items) == 0:
                    append_unmatched(row)
                    unmatched_count += 1
                    row = None
                    break

                for year, code in curated_items:
                    assert year == cur_year-1
                    value = get(connected_items, code)
                    if value is not None:
                        equivs.append(value)
                    else:
                        logging.warning('%d/%s: Failed to find curated item %s/%s',
                                        cur_year, id['code'], year, code)
                if len(equivs) > 0:
                    logging.debug('FOUND CURATED ITEM for %r', id)
                    continue
                else:
                    logging.warning('FOUND 0 CURATED ITEMS for %r', id)

            # Find connected item with same code and title
            connected_item = get(connected_items, id['code'])
            if connected_item is not None:
                if similar(id['title'], connected_item['title']):
                    logging.debug('FOUND EXACT ITEM for %r', id)
                    equivs.append(connected_item)
                    continue

            # Try to find similar named items which moved to a new parent
            if parent is not None:
                connected_item = get(new_connected_items, parent)
                if connected_item is not None:
                    parent = None
                    assert connected_item['year'] == cur_year
                    prev_year_rows = connected_item['history'].get(cur_year-1, [])
                    candidates = []
                    for prev_year_row in prev_year_rows:
                        prev_year_children = prev_year_row['children']
                        if prev_year_children is None:
                            continue
                        for prev_year_child in prev_year_children:
                            if similar(prev_year_child['title'], id['title']):
                                candidates.append(prev_year_row)
                    if len(candidates) == 1:
                        connected_item = get(connected_items, candidates[0]['code'])
                        if connected_item is not None:
                            logging.debug('FOUND MOVED ITEM for %r', id)
                            equivs.append(connected_item)
                            continue

            # Split into children
            if children is not None and len(children) > 0:
                logging.debug('SPLITTING TO CHILDREN for %r', id)
                ids.extend({'code': x['code'], 'title': x['title']} for x in children)
                children = None
                continue

            # Couldn't find match - no point in continuing
            logging.debug('FAILED TO FIND MATCH for %s/%s', cur_year, id)
            append_unmatched(row)
            unmatched_count += 1
            row = None
            break

        # Found match
        if row is not None:
            assert len(equivs) > 0
            new_history = {}
            codes = set()
            for equiv in equivs:
                if equiv['code'] in codes:
                    continue
                codes.add(equiv['code'])
                s = mapped_levels.setdefault(equiv['code'], set())
                if len(row['code']) in s:
                    logging.warning('DOUBLE BOOKING for %s/%s from %s/%s', equiv['year'], equiv['code'], row['year'], row['code'])
                    for nci in iterate_values(new_connected_items):
                        for hist_item in nci.get('history', {}).get(equiv['year'], []):
                            if hist_item['code'] == equiv['code']:
                                logging.warning('FOUND')
                                logging.warning('%s', json.dumps(nci, indent=2))
                else:
                    s.add(len(row['code']))
                to_delete.add(equiv['code'])
                for year, hist_item in equiv['history'].items():
                    update_equiv(new_history.setdefault(year, {}), hist_item)
                update_equiv(new_history.setdefault(equiv['year'], {}), equiv)
            row['history'] = new_history
            put(new_connected_items, row['code'], row)

    logging.error('UNMATCHED %d: %d', cur_year, unmatched_count)


def process_budgets(rows_):

    shutil.rmtree(CURRENT_DB, ignore_errors=True)
    shutil.rmtree(NEW_CURRENT_DB, ignore_errors=True)

    for cur_year, rows in itertools.groupby(rows_, lambda r: r['year']):

        connected_items = plyvel.DB(CURRENT_DB, create_if_missing=True)
        new_connected_items = plyvel.DB(NEW_CURRENT_DB, create_if_missing=True)

        shutil.rmtree(UNMATCHED1, ignore_errors=True)
        shutil.rmtree(UNMATCHED2, ignore_errors=True)
        unmatched1 = plyvel.DB(UNMATCHED1, create_if_missing=True)
        unmatched2 = plyvel.DB(UNMATCHED2, create_if_missing=True)

        codes_to_delete = set()

        calc_equivs(cur_year, rows,
                    connected_items, new_connected_items, codes_to_delete,
                    unmatched1)
        calc_equivs(cur_year, iterate_values(unmatched1),
                    connected_items, new_connected_items, codes_to_delete,
                    unmatched2)

        for code in codes_to_delete:
            delete(connected_items, code)

        for row in iterate_values(unmatched2):
            row['history'] = {}
            put(new_connected_items, row['code'], row)

        unmatched1.close()
        unmatched2.close()
        del unmatched1
        del unmatched2

        yield from iterate_values(connected_items)

        del connected_items
        new_connected_items.close()
        del new_connected_items

        shutil.rmtree(CURRENT_DB)
        os.replace(NEW_CURRENT_DB, CURRENT_DB)

    connected_items = plyvel.DB(CURRENT_DB, create_if_missing=True)
    yield from iterate_values(connected_items)
    del connected_items

    shutil.rmtree(CURRENT_DB)


def process_resources(res_iter_):
    for res in res_iter_:
        logging.error('IC %r', res.spec['name'])
        if res.spec['name'] == 'curated':
            process_curated(res)
        if res.spec['name'] == 'budget':
            yield process_budgets(res)
    # yield errors

schema = dp['resources'][1]['schema']
schema['fields'].append({
    'name': 'history',
    'type': 'object'
})
dp['resources'] = [
    {
        'name': 'budget',
        PROP_STREAMING: True,
        'path': 'data/budget.csv',
        'schema': schema
    },
]

spew(dp, process_resources(res_iter))