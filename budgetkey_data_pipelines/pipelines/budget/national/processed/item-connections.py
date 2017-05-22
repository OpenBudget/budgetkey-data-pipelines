import json
import logging
import itertools
from fuzzywuzzy import fuzz

from datapackage_pipelines.wrapper import ingest, spew

curated = {}
errors = []

parameters, dp, res_iter = ingest()


def similar(s1, s2):
    return fuzz.partial_ratio(s1, s2) > 80


def process_curated(rows):
    for row in rows:
        curated[tuple(row['current'])] = tuple(row['previous'])


def calc_equivs(cur_year, rows, connected_items, new_connected_items):

    logging.info('cur_year: %r', cur_year)
    # logging.info('connected_items: %r', connected_items)
    # logging.info('new_connected_items: %r', new_connected_items)

    mapped_levels = {}
    unmatched = []
    for row in rows:
        equivs = []
        parent = row['parent']
        children = row['children']

        ids = [{'code': row['code'], 'title': row['title']}]
        while len(ids) > 0:
            logging.info('%d/%r: ids: %r', cur_year, row['code'], ids)
            id = ids.pop(0)

            # Find curated record for id
            curated_items = curated.get((cur_year, id['code']))
            if curated_items is not None:
                for year, code in curated_items:
                    assert year == cur_year-1
                    if code in connected_items:
                        equivs.append(connected_items[code])
                    else:
                        logging.warning('%d/%r: Failed to find curated item %s/%s',
                                        cur_year, row['code'], year, code)
                if len(equivs) > 0:
                    logging.info('FOUND CURATED ITEM for %r', id)
                    continue
                else:
                    logging.warning('FOUND 0 CURATED ITEMS for %r', id)

            # Find connected item with same code and title
            if id['code'] in connected_items:
                connected_item = connected_items[id['code']]
                if similar(id['title'], connected_item['title']):
                    logging.info('FOUND EXACT ITEM for %r', id)
                    equivs.append(connected_item)
                    continue

            # Try to find similar named items which moved to a new parent
            if parent is not None:
                if parent in new_connected_items:
                    connected_item = new_connected_items[parent]
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
                        connected_item = connected_items.get(candidates[0]['code'])
                        if connected_item is not None:
                            logging.info('FOUND MOVED ITEM for %r', id)
                            equivs.append(connected_item)
                            continue

            # Split into children
            if children is not None and len(children) > 0:
                logging.info('SPLITTING TO CHILDREN for %r', id)
                ids.extend({'code': x['code'], 'title': x['title']} for x in children)
                children = None
                continue

            # Couldn't find match - no point in continuing
            logging.info('FAILED TO FIND MATCH for %s/%s', cur_year, id)
            unmatched.append(row)
            row = None
            break

        # Found match
        if row is not None:
            assert len(equivs) > 0
            new_history = {}
            # logging.info(', '.join(x['code'] for x in equivs))
            codes = set()
            for equiv in equivs:
                if equiv['code'] in codes:
                    continue
                codes.add(equiv['code'])
                s = mapped_levels.setdefault(equiv['code'], set())
                if len(row['code']) in s:
                    logging.warning('DOUBLE BOOKING for %s/%s from %s/%s', equiv['year'], equiv['code'], row['year'], row['code'])
                    logging.warning('DOUBLE BOOKING %r', equivs)
                    for nci in new_connected_items.values():
                        for hist_item in nci.get('history', {}).get(equiv['year'], []):
                            if hist_item['code'] == equiv['code']:
                                logging.warning('FOUND\n%s', json.dumps(nci, indent=2))
                else:
                    s.add(len(row['code']))
                if equiv['code'] in connected_items:
                    del connected_items[equiv['code']]
                for year, items in equiv['history'].items():
                    new_history.setdefault(year, []).extend(items)
                del equiv['history']
                new_history.setdefault(equiv['year'], []).append(equiv)
            row['history'] = new_history
            new_connected_items[row['code']] = row

    logging.error('UNMATCHED %d: %d', cur_year, len(unmatched))

    return unmatched


def process_budgets(rows_):
    connected_items = {}
    new_connected_items = {}

    for cur_year, rows in itertools.groupby(rows_, lambda r: r['year']):

        unmatched = calc_equivs(cur_year, rows,
                                connected_items, new_connected_items)
        unmatched = calc_equivs(cur_year, reversed(unmatched),
                                connected_items, new_connected_items)

        for row in unmatched:
            row['history'] = {}
            new_connected_items[row['code']] = row

        yield from iter(connected_items.values())
        connected_items = new_connected_items
        new_connected_items = {}


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
        'name': 'budget-items',
        'path': 'data/budget-items.csv',
        'schema': schema
    },
    # {
    #     'name': 'unmatched',
    #     'path': 'data/unmatched.csv',
    #     'schema': schema
    # }
]

spew(dp, process_resources(res_iter))