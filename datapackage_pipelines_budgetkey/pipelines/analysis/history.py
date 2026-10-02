"""Budget trends that survive renumbering, built from the budget items' `history`.

The budget pipeline's item-connections step links each budget line year by year to the codes it had before
(same code, a line that moved under its parent, a split, or a curated mapping), and stores the result in the
item's `history`: {year: {net_allocated, net_revised, net_executed, code_titles, ...}}. Summing a line's current
amounts with its history gives one continuous series even when its code changed.

The links are not complete. Where a significant line's history stops, the years before that are marked as
estimates on the chart rather than researched further.
"""
import json

from datapackage_pipelines_budgetkey.pipelines.analysis.query import query

MIN_CODE_SHARE = 0.05      # lines below this share of the yearly total don't decide where estimates start
DEFAULT_YEARS = 15         # chart span when the agent doesn't ask for one
EXCLUDED_ECONOMIC_CLASSES = ('העברות  פנים תקציביות', 'הכנסות  מיועדות', 'רזרבות', 'חשבונות מעבר')


def mirror_code(code):
    """'20.62.01' -> '00206201' (the budget documents' code format)."""
    return '00' + code.replace('.', '')


def _num(v):
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def fetch(codes):
    """{code: {'title', 'year', 'years': {year: {'allocated','revised','used','linked'}}}} from each code's latest document."""
    if not codes:
        return {}
    by_mirror = {mirror_code(c): c for c in codes}
    rows = query('''
        SELECT DISTINCT ON (code) code, year, title, net_allocated, net_revised, net_executed, history
        FROM _elasticsearch_mirror__budget WHERE code IN (%s) ORDER BY code, year DESC
    ''' % ','.join("'%s'" % c for c in by_mirror))
    out = {}
    for r in rows:
        history = r['history']
        if isinstance(history, str):
            history = json.loads(history)
        years = {}
        for y, h in (history or {}).items():
            years[int(y)] = dict(allocated=_num(h.get('net_allocated')), revised=_num(h.get('net_revised')),
                                 used=_num(h.get('net_executed')), linked=bool(h.get('code_titles')))
        years[int(r['year'])] = dict(allocated=_num(r['net_allocated']), revised=_num(r['net_revised']),
                                     used=_num(r['net_executed']), linked=True)
        out[by_mirror[r['code']]] = dict(title=r['title'], year=int(r['year']), years=years)
    return out


def excluded_codes(codes):
    """Level-4 codes whose latest economic class double-counts (transfers, earmarked income, reserves)."""
    lines = [c for c in codes if c.count('.') == 3]
    if not lines:
        return []
    rows = query('''
        SELECT DISTINCT ON (code) code, economic_class_primary FROM budget_items_data
        WHERE code IN (%s) ORDER BY code, year DESC
    ''' % ','.join("'%s'" % c for c in lines))
    return sorted(r['code'] for r in rows if r['economic_class_primary'] in EXCLUDED_ECONOMIC_CLASSES)


def trend(codes, from_year=None):
    """The topic's yearly totals over its scope codes, continuity-aware.

    Returns dict(rows=[{year, allocated, revised, used}], estimate_before=year or None, codes=[...],
    excluded=[...], missing={code: first linked year}).
    """
    codes = sorted(set(codes))
    excluded = excluded_codes(codes)
    codes = [c for c in codes if c not in excluded]
    data = fetch(codes)
    unknown = [c for c in codes if c not in data]
    latest = max((d['year'] for d in data.values()), default=None)
    if latest is None:
        return dict(rows=[], estimate_before=None, codes=codes, excluded=excluded, unknown=unknown, missing={})
    start = max(1997, from_year or latest - DEFAULT_YEARS + 1)
    years = list(range(start, latest + 1))

    rows = []
    for y in years:
        row = dict(year=y, allocated=None, revised=None, used=None)
        for d in data.values():
            v = d['years'].get(y)
            if not v:
                continue
            for k in ('allocated', 'revised', 'used'):
                if v[k] is not None:
                    row[k] = (row[k] or 0) + v[k]
        rows.append(row)

    while rows and all(rows[0][k] is None for k in ('allocated', 'revised', 'used')):
        rows.pop(0)
    if not rows:
        return dict(rows=[], estimate_before=None, codes=codes, excluded=excluded, unknown=unknown, missing={})
    start = rows[0]['year']

    # Where does every significant line have a continuous, linked history back to?
    totals = {r['year']: abs(r['revised'] or r['allocated'] or 0) for r in rows}
    first_linked = {}
    for code, d in data.items():
        peak = max([abs((v['revised'] or v['allocated'] or 0)) / totals[y]
                    for y, v in d['years'].items() if totals.get(y)] or [0])
        if peak < MIN_CODE_SHARE:
            continue
        first = d['year']
        while first - 1 >= start and d['years'].get(first - 1, {}).get('linked'):
            first -= 1
        if first > start:
            first_linked[code] = first
    estimate_before = max(first_linked.values()) if first_linked else None
    return dict(rows=rows, estimate_before=estimate_before, codes=codes, excluded=excluded, unknown=unknown,
                missing=first_linked)


def code_amounts(codes):
    """{code: {year: amount}} for every year in which the code has a budget row (by its code, no history)."""
    if not codes:
        return {}
    rows = query("SELECT code, year, MAX(COALESCE(amount_revised, amount_allocated, 0)) AS amount "
                 "FROM budget_items_data WHERE code IN (%s) GROUP BY code, year"
                 % ','.join("'%s'" % c for c in codes))
    out = {}
    for r in rows:
        out.setdefault(r['code'], {})[int(r['year'])] = r['amount']
    return out


def budget_items(codes):
    """Title and link for each code, from its latest year with money in it (the site doesn't index all-zero years)."""
    if not codes:
        return []
    return query('''
        SELECT DISTINCT ON (code) code, title, item_url FROM budget_items_data WHERE code IN (%s)
        ORDER BY code, (COALESCE(amount_allocated, 0) <> 0 OR COALESCE(amount_revised, 0) <> 0
                        OR COALESCE(amount_used, 0) <> 0) DESC, year DESC
    ''' % ','.join("'%s'" % c for c in codes))
