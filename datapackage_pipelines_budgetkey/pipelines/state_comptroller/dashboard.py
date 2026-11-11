"""Aggregated tables for the State Comptroller dashboard.

From the audits' closed taxonomy (taxonomy.py), the publications and the tracked issues:
- state_comptroller_dash_comptrollers: one row per State Comptroller since 2000: volume and format of the audits
  published in his term.
- state_comptroller_dash_counts: how many audits each domain and each lens covered, overall, per comptroller and per
  year (the topic map and its change over time).
- state_comptroller_dash_trends: per domain and lens, its share in each comptroller's term, the long-term trend
  (odds ratio per decade) and whether the comptroller's term explains more than the trend does.
- state_comptroller_dash_events: the special reports, named and classified (an event such as the Carmel fire, a
  theme, a recurring report, a follow-up), with the reports about the same event (the 7 October war) as one row.
- state_comptroller_dash_chronic_issues: issues that the comptroller audited more than once and, at the last audit,
  found not fixed, longest-running first.

Every row that a dashboard links from carries the search page or document URL to link to.
"""
import collections
import datetime
import logging
import math
import os

import dataflows as DF
import numpy as np

import sc_common as sc
from taxonomy import AXES, SEARCH_URL

CHAPTERS_PATH = os.environ.get('CHAPTERS', '/var/datapackages/state_comptroller/chapters')
ISSUES_PATH = os.environ.get('ISSUES', '/var/datapackages/state_comptroller/issues')
TAXONOMY_PATH = os.environ.get('TAXONOMY', '/var/datapackages/state_comptroller/taxonomy')
OUT_PATH = '/var/datapackages/state_comptroller/dashboard'
SITE = 'https://next.obudget.org'

# The State Comptrollers whose reports were analysed (2000 on): key, name, took office, left office.
# Englman's term ended in July 2026 and the office has been vacant since. Until a successor is appointed, the reports
# published since then count as his (his row has no end date). A successor is a new row here, with Englman's end date
# set to the day before.
COMPTROLLERS = [
    ('goldberg', 'אליעזר גולדברג', datetime.date(1998, 7, 1), datetime.date(2005, 7, 3)),
    ('lindenstrauss', 'מיכה לינדנשטראוס', datetime.date(2005, 7, 4), datetime.date(2012, 7, 1)),
    ('shapira', 'יוסף חיים שפירא', datetime.date(2012, 7, 2), datetime.date(2019, 6, 30)),
    ('englman', 'מתניהו אנגלמן', datetime.date(2019, 7, 1), None),
]
NAMES = {k: n for k, n, _, _ in COMPTROLLERS}
UNRESOLVED = {'not_rectified': 'לא תוקן', 'partially_rectified': 'תוקן באופן חלקי'}
P_THRESHOLD = 0.01


def comptroller(date):
    if not date:
        return None
    for key, _, start, end in COMPTROLLERS:
        if start <= date and (end is None or date <= end):
            return key
    return None


def read(path, resource, fields=None):
    steps = [DF.load(os.path.join(path, 'datapackage.json'), resources=[resource])]
    if fields:
        steps.append(DF.select_fields(fields))
    return DF.Flow(*steps).results()[0][0]


def as_date(v):
    if isinstance(v, datetime.date):
        return v
    try:
        return datetime.date.fromisoformat(str(v)[:10]) if v else None
    except ValueError:
        return None


# --- statistics (numpy only: the pipelines image has no scipy/statsmodels) ---

def logit(X, y, iterations=50):
    """Logistic regression by IRLS, with a tiny ridge so that a category absent from one period still converges.
    Returns (coefficients, log-likelihood)."""
    beta = np.zeros(X.shape[1])
    for _ in range(iterations):
        p = 1 / (1 + np.exp(-X @ beta))
        w = np.clip(p * (1 - p), 1e-9, None)
        H = X.T @ (X * w[:, None]) + 1e-6 * np.eye(X.shape[1])
        step = np.linalg.solve(H, X.T @ (y - p) - 1e-6 * beta)
        beta += step
        if np.abs(step).max() < 1e-8:
            break
    p = np.clip(1 / (1 + np.exp(-X @ beta)), 1e-12, 1 - 1e-12)
    return beta, float(np.sum(y * np.log(p) + (1 - y) * np.log(1 - p))), H


def chi2_sf(x, k):
    """P(X > x) for a chi-squared variable with an integer k degrees of freedom."""
    if x <= 0:
        return 1.0
    if k % 2 == 0:
        term, total = 1.0, 1.0
        for i in range(1, k // 2):
            term *= (x / 2) / i
            total += term
        return math.exp(-x / 2) * total
    total, term = 0.0, math.sqrt(2 * x / math.pi)
    for i in range(1, (k - 1) // 2 + 1):
        total += term
        term *= x / (2 * i + 1)
    return math.erfc(math.sqrt(x / 2)) + math.exp(-x / 2) * total


def trend_test(years, periods, flags):
    """For one category: the odds ratio per decade of a linear trend, its p-value (Wald), and the p-value of the
    comptrollers' terms beyond that trend (likelihood ratio of trend + term dummies against the trend alone)."""
    y = np.asarray(flags, float)
    t = (np.asarray(years, float) - 2000) / 10
    X0 = np.column_stack([np.ones_like(t), t])
    b0, ll0, H0 = logit(X0, y)
    se = math.sqrt(max(np.linalg.inv(H0)[1, 1], 1e-12))
    p_trend = math.erfc(abs(b0[1] / se) / math.sqrt(2))
    keys = [k for k, _, _, _ in COMPTROLLERS if k in set(periods)]
    dummies = np.column_stack([[1.0 if p == k else 0.0 for p in periods] for k in keys[1:]]) if len(keys) > 1 else None
    if dummies is None:
        return float(math.exp(b0[1])), p_trend, None
    _, ll1, _ = logit(np.column_stack([X0, dummies]), y)
    return float(math.exp(b0[1])), p_trend, chi2_sf(2 * (ll1 - ll0), dummies.shape[1])


def pattern(odds_ratio, p_trend, p_comptroller):
    if p_comptroller is not None and p_comptroller < P_THRESHOLD:
        return 'comptroller'
    if p_trend < P_THRESHOLD:
        return 'rising' if odds_ratio > 1 else 'falling'
    return 'stable'


# --- tables ---

def audit_rows(taxonomy):
    rows = []
    for a in taxonomy:
        d = as_date(a.get('publish_date'))
        if not d or not a.get('domain'):
            continue
        rows.append(dict(a, publish_date=d, year=d.year, comptroller=comptroller(d)))
    return rows


def comptroller_rows(audits, findings):
    by_chapter = collections.defaultdict(list)
    for f in findings:
        by_chapter[f['chapter_id']].append(f)
    rows = []
    for key, name, start, end in COMPTROLLERS:
        mine = [a for a in audits if a['comptroller'] == key]
        if not mine:
            continue
        fs = [f for a in mine for f in by_chapter.get(a['chapter_id'], [])]
        rows.append(dict(
            comptroller=key, name=name, start_date=start, end_date=end, n_audits=len(mine), n_findings=len(fs),
            findings_per_audit=round(len(fs) / len(mine), 1),
            special_share=round(sum(a['publication_type'] == 'special' for a in mine) / len(mine), 3),
            follow_up_share=round(sum((a.get('follow_up') or 'none') != 'none' for a in mine) / len(mine), 3),
            high_severity_share=round(sum(f.get('severity') == 'high' for f in fs) / len(fs), 3) if fs else None,
            first_year=min(a['year'] for a in mine), last_year=max(a['year'] for a in mine),
        ))
    return rows


def count_rows(audits, labels):
    periods = [('all', 'all', 'כל השנים', lambda a: True)]
    periods += [('comptroller', k, NAMES[k], (lambda k: lambda a: a['comptroller'] == k)(k))
                for k, _, _, _ in COMPTROLLERS if any(a['comptroller'] == k for a in audits)]
    periods += [('year', str(y), str(y), (lambda y: lambda a: a['year'] == y)(y))
                for y in sorted({a['year'] for a in audits})]
    rows = []
    for axis, (keys, field) in AXES.items():
        overall = {k: sum(k in a[field] for a in audits) / len(audits) for k in keys}
        for kind, period, period_label, member in periods:
            mine = [a for a in audits if member(a)]
            for key in keys:
                n = sum(key in a[field] for a in mine)
                share = n / len(mine) if mine else 0
                rows.append(dict(axis=axis, key=key, label=labels[(axis, key)], period_kind=kind, period=period,
                                 period_label=period_label, n=n, total=len(mine), share=round(share, 4),
                                 lift=round(share / overall[key], 3) if overall[key] else None,
                                 search_url=SEARCH_URL.format(param=axis, key=key)))
    return rows


def trend_rows(audits, labels):
    rows = []
    years = [a['year'] for a in audits]
    periods = [a['comptroller'] for a in audits]
    for axis, (keys, field) in AXES.items():
        for key in keys:
            flags = [key in a[field] for a in audits]
            if sum(flags) < 10:
                continue
            odds, p_trend, p_comp = trend_test(years, periods, flags)
            shares = {}
            for k, _, _, _ in COMPTROLLERS:
                mine = [f for f, p in zip(flags, periods) if p == k]
                if mine:
                    shares[k] = round(sum(mine) / len(mine), 4)
            rows.append(dict(
                axis=axis, key=key, label=labels[(axis, key)], n=sum(flags), share=round(sum(flags) / len(flags), 4),
                share_by_comptroller=shares, peak_comptroller=max(shares, key=shares.get),
                low_comptroller=min(shares, key=shares.get), odds_ratio_per_decade=round(odds, 3),
                p_trend=float('%.3g' % p_trend), p_comptroller=float('%.3g' % p_comp) if p_comp is not None else None,
                pattern=pattern(odds, p_trend, p_comp), search_url=SEARCH_URL.format(param=axis, key=key),
            ))
    return rows


def event_rows(special_reports, audits, findings):
    """One row per event (the special reports about it grouped together) or other special report."""
    by_pub = collections.defaultdict(list)
    for a in audits:
        by_pub[a['pub_id']].append(a)
    by_chapter = collections.defaultdict(list)
    for f in findings:
        by_chapter[f['chapter_id']].append(f)
    groups = collections.defaultdict(list)
    for r in special_reports:
        groups[r['event']].append(r)
    rows = []
    for key, reports in groups.items():
        reports.sort(key=lambda r: (as_date(r['publish_date']) or datetime.date.min, r['pub_id']))
        mine = sorted((a for r in reports for a in by_pub.get(r['pub_id'], [])), key=lambda a: a['chapter_id'])
        if not mine:
            continue
        fs = [f for a in mine for f in by_chapter.get(a['chapter_id'], [])]
        domains = collections.Counter(k for a in mine for k in a['domains'])
        lenses = collections.Counter(k for a in mine for k in a['lenses'])
        dates = [as_date(r['publish_date']) for r in reports if r['publish_date']]
        first, last = (min(dates), max(dates)) if dates else (None, None)
        kinds = collections.Counter(r['kind'] for r in reports)
        head = reports[0]
        rows.append(dict(
            event=key, name=head['event_name'], kind=kinds.most_common(1)[0][0], event_when=head['event_when'],
            description=head['event_description'], first_published=first, last_published=last,
            year=first.year if first else None, comptroller=comptroller(first),
            comptroller_name=NAMES.get(comptroller(first)),
            n_reports=len(reports), n_chapters=len(mine), n_findings=len(fs),
            n_high_severity=sum(f.get('severity') == 'high' for f in fs),
            domains=[k for k, _ in domains.most_common(3)], lenses=[k for k, _ in lenses.most_common(3)],
            reports=[dict(pub_id=r['pub_id'], title=r['title'], publish_date=str(as_date(r['publish_date']) or ''),
                          page_url=r['page_url']) for r in reports],
            chapters=[dict(chapter_id=a['chapter_id'], title=a['title'],
                           url='{}/i/state_comptroller_audits/{}'.format(SITE, a['chapter_id'])) for a in mine],
            search_url=SEARCH_URL.format(param='event', key=key),
        ))
    rows.sort(key=lambda r: (r['last_published'] or datetime.date.min), reverse=True)
    logging.info('Dashboard: %d special-report groups, %s', len(rows), collections.Counter(r['kind'] for r in rows))
    return rows


def chronic_rows(issues, by_chapter):
    """Issues the comptroller audited at least twice and, at the last audit with a verdict, found not fixed."""
    rows = []
    for i in issues:
        timeline = i.get('timeline') or []
        if sum(1 for e in timeline if e.get('kind') != 'body_response') < 2 or (i.get('n_audits') or 0) < 2:
            continue
        verdicts = [e for e in timeline if e.get('kind') == 'follow_up' and e.get('status')]
        if not verdicts or verdicts[-1]['status'] not in UNRESOLVED:
            continue
        last = verdicts[-1]
        first = as_date(i.get('first_reported'))
        last_date = as_date(last.get('date'))
        if not first or not last_date or last_date <= first:
            continue
        findings = [e for e in timeline if e.get('kind') in ('finding', 'follow_up')]
        chapters = list(dict.fromkeys(e.get('chapter_id') for e in findings if e.get('chapter_id')))
        responses = [e for e in timeline if e.get('kind') == 'response' and e.get('status')]
        tags = [by_chapter[c] for c in chapters if c in by_chapter]
        rows.append(dict(
            issue_id=i['issue_id'], title=i.get('title'), description=i.get('description'),
            bodies=i.get('bodies') or [], severity=i.get('severity'),
            domains=list(dict.fromkeys(k for t in tags for k in t['domains'])),
            lenses=list(dict.fromkeys(k for t in tags for k in t['lenses'])),
            first_reported=first, first_chapter_id=chapters[0] if chapters else None,
            first_chapter_title=findings[0].get('source') if findings else None,
            latest_verdict=last['status'], latest_verdict_label=UNRESOLVED[last['status']],
            latest_verdict_date=last_date,
            latest_verdict_chapter_id=last.get('chapter_id'), latest_verdict_chapter_title=last.get('source'),
            years_open=round((last_date - first).days / 365.25, 1), n_audits=i.get('n_audits'),
            n_responses=i.get('n_responses'), government_status=responses[-1]['status'] if responses else None,
            url='{}/i/state_comptroller_issues/{}'.format(SITE, i['issue_id']),
        ))
    rows.sort(key=lambda r: (-r['years_open'], -(r['n_audits'] or 0), r['issue_id']))
    # Several findings of the same pair of audits often qualify together: chain_rank 1 is each chain's longest-running
    # one, for cards that should each tell a different story
    chains = collections.Counter()
    for rank, r in enumerate(rows, 1):
        chain = (r['first_chapter_id'], r['latest_verdict_chapter_id'])
        chains[chain] += 1
        r['rank'], r['chain_rank'] = rank, chains[chain]
    logging.info('Dashboard: %d chronic issues', len(rows))
    return rows


COMPTROLLER_FIELDS = [
    ('comptroller', 'string'), ('name', 'string'), ('start_date', 'date'), ('end_date', 'date'),
    ('n_audits', 'integer'), ('n_findings', 'integer'), ('findings_per_audit', 'number'), ('special_share', 'number'),
    ('follow_up_share', 'number'), ('high_severity_share', 'number'), ('first_year', 'integer'),
    ('last_year', 'integer'),
]
COUNT_FIELDS = [
    ('axis', 'string'), ('key', 'string'), ('label', 'string'), ('period_kind', 'string'), ('period', 'string'),
    ('period_label', 'string'), ('n', 'integer'), ('total', 'integer'), ('share', 'number'), ('lift', 'number'),
    ('search_url', 'string'),
]
TREND_FIELDS = [
    ('axis', 'string'), ('key', 'string'), ('label', 'string'), ('n', 'integer'), ('share', 'number'),
    ('share_by_comptroller', 'object'), ('peak_comptroller', 'string'), ('low_comptroller', 'string'),
    ('odds_ratio_per_decade', 'number'), ('p_trend', 'number'), ('p_comptroller', 'number'), ('pattern', 'string'),
    ('search_url', 'string'),
]
EVENT_FIELDS = [
    ('event', 'string'), ('name', 'string'), ('kind', 'string'), ('event_when', 'string'), ('description', 'string'),
    ('first_published', 'date'), ('last_published', 'date'), ('year', 'integer'), ('comptroller', 'string'),
    ('comptroller_name', 'string'), ('n_reports', 'integer'), ('n_chapters', 'integer'), ('n_findings', 'integer'),
    ('n_high_severity', 'integer'), ('domains', 'array'), ('lenses', 'array'), ('reports', 'array'),
    ('chapters', 'array'), ('search_url', 'string'),
]
CHRONIC_FIELDS = [
    ('rank', 'integer'), ('chain_rank', 'integer'), ('issue_id', 'string'), ('title', 'string'), ('description', 'string'), ('bodies', 'array'),
    ('severity', 'string'), ('domains', 'array'), ('lenses', 'array'), ('first_reported', 'date'),
    ('first_chapter_id', 'integer'), ('first_chapter_title', 'string'), ('latest_verdict', 'string'),
    ('latest_verdict_label', 'string'), ('latest_verdict_date', 'date'), ('latest_verdict_chapter_id', 'integer'),
    ('latest_verdict_chapter_title', 'string'), ('years_open', 'number'), ('n_audits', 'integer'),
    ('n_responses', 'integer'), ('government_status', 'string'), ('url', 'string'),
]


def build(chapters_path, issues_path, taxonomy_path):
    taxonomy = read(taxonomy_path, 'audit_taxonomy')
    labels = {(r['axis'], r['key']): r['label'] for r in read(taxonomy_path, 'taxonomy')}
    audits = audit_rows(taxonomy)
    findings = read(chapters_path, 'findings', ['chapter_id', 'severity'])
    issues = read(issues_path, 'issues', ['issue_id', 'title', 'description', 'bodies', 'severity', 'first_reported',
                                          'n_audits', 'n_responses', 'timeline'])
    by_chapter = {a['chapter_id']: a for a in audits}
    return dict(
        comptrollers=comptroller_rows(audits, findings),
        counts=count_rows(audits, labels),
        trends=trend_rows(audits, labels),
        events=event_rows(read(taxonomy_path, 'special_reports'), audits, findings),
        chronic_issues=chronic_rows(issues, by_chapter),
    )


def output_flow(tables):
    return DF.Flow(
        sc.typed_resource(tables['comptrollers'], 'comptrollers', COMPTROLLER_FIELDS, ['comptroller']),
        sc.typed_resource(tables['counts'], 'counts', COUNT_FIELDS, ['axis', 'key', 'period_kind', 'period']),
        sc.typed_resource(tables['trends'], 'trends', TREND_FIELDS, ['axis', 'key']),
        sc.typed_resource(tables['events'], 'events', EVENT_FIELDS, ['event']),
        sc.typed_resource(tables['chronic_issues'], 'chronic_issues', CHRONIC_FIELDS, ['issue_id']),
    )


def flow(*_):
    from datapackage_pipelines_budgetkey.common.dump_to_sql_atomic import dump_to_sql_atomic
    tables = build(CHAPTERS_PATH, ISSUES_PATH, TAXONOMY_PATH)
    return DF.Flow(
        output_flow(tables),
        DF.update_resource(None, **{'dpp:streaming': True}),
        DF.dump_to_path(OUT_PATH),
        dump_to_sql_atomic({'state_comptroller_dash_' + name: {'resource-name': name} for name in tables}),
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    for noisy in ('httpx', 'httpcore', 'google_genai', 'urllib3'):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    tables = build(CHAPTERS_PATH, ISSUES_PATH, TAXONOMY_PATH)
    DF.Flow(output_flow(tables), DF.dump_to_path(os.environ.get('OUT', 'state_comptroller_dashboard_out'))).process()
