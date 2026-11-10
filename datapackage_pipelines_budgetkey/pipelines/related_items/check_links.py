"""Sanity checks for the related_items tables after a run: read-only queries, each with what to expect.

    python -m datapackage_pipelines_budgetkey.pipelines.related_items.check_links          # via next.obudget.org's API
    python -m datapackage_pipelines_budgetkey.pipelines.related_items.check_links --engine postgresql://...
    python -m datapackage_pipelines_budgetkey.pipelines.related_items.check_links --sql    # print the queries

Each query is a single SELECT, so it can also be pasted into /api/query. A check whose table doesn't exist yet
(state_comptroller_audits before the first chapters run) reports an error and the rest go on. Exits 1 if a check
fails, or if the table can't be read at all.
"""
import argparse
import json
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

API = 'https://next.obudget.org/api/query?query='
SITE = 'https://next.obudget.org/i/'
MIN_CONFIDENCE = 0.8
# Seconds for a whole API query: each runs in seconds; a stalled one is given up on (and retried).
QUERY_TIMEOUT = 300


def _none(rows):
    return 'ok' if not rows else 'fail'


def _none_warn(rows):
    return 'ok' if not rows else 'warn'


def _zero(field):
    return lambda rows: 'ok' if rows and not rows[0][field] else 'fail'


def _few(limit):
    """Some dangling links are expected (targets deleted or re-keyed since the run); many mean a doc-id mismatch."""
    def check(rows):
        r = rows[0] if rows else {}
        if not r.get('links'):
            return 'ok'
        return 'ok' if r['dangling'] <= limit * r['links'] else 'fail'
    return check


def _totals(rows):
    r = rows[0] if rows else {}
    if not r.get('links'):
        return 'fail'
    return 'ok' if float(r['min_confidence']) >= MIN_CONFIDENCE and r['versions'] == 1 else 'warn'


# The audits that have a page (state_comptroller/es_documents.py's indexed_audit).
INDEXED_AUDITS = ("select chapter_id from state_comptroller_audits where language = 'he' and analysis_status in "
                  "('ok', 'low-verification', 'partial-text', 'no-findings') and coalesce(text_md, '') <> ''")


def _dangling(kind, join, key):
    """Links of target_kind `kind` whose target is missing from its table."""
    return """select count(*) as links, count(*) filter (where {key} is null) as dangling,
  (array_agg(distinct r.target_doc_id order by r.target_doc_id) filter (where {key} is null))[1:5] as examples
from related_items r {join}
where r.target_kind = '{kind}'""".format(kind=kind, join=join, key=key)


CHECKS = [
    dict(name='totals', expect=_totals,
         doc='links > 0, every confidence >= {}, a single linker_version'.format(MIN_CONFIDENCE),
         sql="""select count(*) as links, count(distinct source_doc_id) as sources,
  count(distinct target_doc_id) as targets, min(confidence) as min_confidence,
  count(distinct linker_version) as versions, min(source_date) as first_source, max(source_date) as last_source
from related_items"""),
    dict(name='by_kind', doc='source kind x target kind: links, sources, targets, mean confidence',
         sql="""select source_kind, target_kind, count(*) as links, count(distinct source_doc_id) as sources,
  count(distinct target_doc_id) as targets, round(avg(confidence)::numeric, 3) as confidence
from related_items group by 1, 2 order by 1, 3 desc"""),
    dict(name='by_method', doc='method x relation (derived:issue = finding links lifted to issues)',
         sql="""select method, relation, count(*) as links, count(distinct source_doc_id) as sources
from related_items group by 1, 2 order by 3 desc"""),
    dict(name='mentions', doc='mentions by kind and status (resolved ones are links; the rest explain why not)',
         sql="""select mention_kind, status, count(*) as mentions, count(distinct source_doc_id) as sources
from related_items_mentions group by 1, 2 order by 1, 3 desc"""),
    dict(name='duplicate_link_ids', expect=_none, doc='no rows',
         sql="""select link_id, count(*) as n from related_items group by 1 having count(*) > 1 limit 20"""),
    dict(name='self_links', expect=_zero('n'), doc='n = 0',
         sql="""select count(*) as n from related_items where source_doc_id = target_doc_id"""),
    dict(name='doc_id_prefixes', expect=_none, doc='no rows: every doc id starts with its kind\'s prefix',
         sql="""select 'source' as side, source_kind as kind, count(*) as n, min(source_doc_id) as example
from related_items
where source_doc_id not like (case source_kind when 'entities' then 'org' else source_kind end) || '/%'
group by 1, 2
union all
select 'target', target_kind, count(*), min(target_doc_id) from related_items
where target_doc_id not like (case target_kind when 'entities' then 'org' else target_kind end) || '/%'
group by 1, 2"""),
    dict(name='missing_fields', expect=_none_warn, doc='no rows: titles and source dates are set',
         sql="""select source_kind, target_kind, count(*) filter (where coalesce(source_title, '') = '') as no_source_title,
  count(*) filter (where source_date is null) as no_source_date,
  count(*) filter (where coalesce(target_title, '') = '') as no_target_title
from related_items group by 1, 2
having count(*) filter (where coalesce(source_title, '') = '' or source_date is null
                        or coalesce(target_title, '') = '') > 0"""),
    dict(name='dangling_entities', expect=_few(0.01), doc='dangling <= 1% of links',
         sql=_dangling('entities', "left join entities e on e.kind = split_part(r.target_doc_id, '/', 2) "
                                   "and e.id = split_part(r.target_doc_id, '/', 3)", 'e.id')),
    dict(name='dangling_decisions', expect=_few(0.01), doc='dangling <= 1% of links',
         sql=_dangling('gov_decisions', "left join (select distinct id from government_decisions) d "
                                        "on d.id = split_part(r.target_doc_id, '/', 2)", 'd.id')),
    dict(name='dangling_budget', expect=_few(0.01), doc='dangling <= 1% of links (the connected item\'s doc)',
         sql=_dangling('budget', "left join budget b on b.code = split_part(r.target_doc_id, '/', 2) "
                                 "and b.year::text = split_part(r.target_doc_id, '/', 3)", 'b.code')),
    dict(name='dangling_support_programs', expect=_few(0.01), doc='dangling <= 1% of links',
         sql=_dangling('support_programs', "left join support_programs p "
                                           "on 'support_programs/' || p.program_key = r.target_doc_id",
                       'p.program_key')),
    dict(name='dangling_tenders', expect=_few(0.01), doc='dangling <= 1% of links',
         sql=_dangling('tenders', "left join procurement_tenders_processed t on 'tenders/' || t.tender_type || '/' "
                                  "|| t.publication_id || '/' || t.tender_id = r.target_doc_id", 't.tender_id')),
    dict(name='dangling_audits', expect=_few(0.01),
         doc='dangling <= 1% of links (cited earlier chapters without a page: not an indexed audit)',
         sql=_dangling('state_comptroller_audits', "left join ({}) c "
                                                   "on 'state_comptroller_audits/' || c.chapter_id = r.target_doc_id"
                       .format(INDEXED_AUDITS), 'c.chapter_id')),
    dict(name='unindexed_audit_sources', expect=_zero('n'), doc='n = 0: every audit source is an indexed audit',
         sql="""select count(*) as n, min(r.source_doc_id) as example
from related_items r left join ({}) c on 'state_comptroller_audits/' || c.chapter_id = r.source_doc_id
where r.source_kind = 'state_comptroller_audits' and c.chapter_id is null""".format(INDEXED_AUDITS)),
    dict(name='dangling_contracts', expect=_few(0.01), doc='dangling <= 1% of links (from analysis pages)',
         sql=_dangling('contract-spending', "left join contract_spending c on 'contract-spending/' || c.publisher_key "
                                            "|| '/' || c.order_id || '/' || c.budget_code = r.target_doc_id",
                       'c.order_id')),
    dict(name='dangling_supports', expect=_few(0.01), doc='dangling <= 1% of links (from analysis pages)',
         sql=_dangling('supports', "left join supports_by_request_year s on 'supports/' || s.budget_code "
                                   "|| '/' || s.year_requested || '/' || s.short_id || '/' || s.request_type "
                                   "= r.target_doc_id", 's.short_id')),
    dict(name='dangling_activities', expect=_few(0.01), doc='dangling <= 1% of links (from analysis pages)',
         sql=_dangling('activities', "left join activities a on 'activities/' || a.kind || '/' || a.id "
                                     "= r.target_doc_id", 'a.id')),
    dict(name='busiest_sources', doc='the documents with the most links (a long list may be boilerplate)',
         sql="""select source_doc_id, max(source_title) as title, count(*) as links,
  count(distinct target_doc_id) as targets
from related_items group by 1 order by 3 desc limit 10"""),
    dict(name='most_linked_targets', doc='the most linked targets: expect ministries; '
                                         'a body named in passing everywhere (as the Knesset was) needs ubiquitous',
         sql="""select target_doc_id, max(target_title) as title, count(distinct source_doc_id) as sources,
  string_agg(distinct method, ',') as methods
from related_items group by 1 order by 3 desc limit 15"""),
    dict(name='sample', doc='20 links, a few of each target kind, for a hand check (stable across runs)',
         sql="""select source_doc_id, source_title, target_doc_id, target_title, relation, method, confidence,
  mention, left(excerpt, 240) as excerpt
from (select r.*, row_number() over (partition by target_kind order by md5(link_id)) as k from related_items r) s
where k <= 4 order by k, target_kind limit 20"""),
]


def _fetch(req, timeout):
    """The JSON at `req`, within `timeout` seconds in all. urlopen's own timeout bounds each socket read, not the
    request: a connection that stays open without finishing (one /api/query call did, for over 90 minutes) would
    block the run."""
    box = {}

    def get():
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                box['ret'] = json.load(resp)
        except Exception as e:  # handed to the caller's thread
            box['error'] = e
    thread = threading.Thread(target=get, daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        raise TimeoutError('no complete answer in {}s'.format(timeout))
    if 'error' in box:
        raise box['error']
    return box['ret']


def api_query(sql, retries=3, timeout=None):
    req = urllib.request.Request(API + urllib.parse.quote(sql), headers={'User-Agent': 'budgetkey-related-items'})
    for attempt in range(retries):
        try:
            ret = _fetch(req, timeout or QUERY_TIMEOUT)
            break
        except (urllib.error.HTTPError, TimeoutError) as e:
            if (isinstance(e, urllib.error.HTTPError) and e.code < 500) or attempt == retries - 1:
                raise
            time.sleep(5 * (attempt + 1))
    if not ret.get('success'):
        raise RuntimeError(ret.get('error', 'query failed').split('\n')[0])
    return ret['rows']


def engine_query(engine):
    from sqlalchemy import create_engine, text
    # The same bound as the API's, on the server's side.
    connect_args = {'options': '-c statement_timeout={}'.format(QUERY_TIMEOUT * 1000)} \
        if engine.startswith('postgres') else {}
    eng = create_engine(engine, connect_args=connect_args)

    def query(sql):
        with eng.connect() as conn:
            result = conn.execute(text(sql))
            return [dict(zip(result.keys(), r)) for r in result]
    return query


def run(query, checks=CHECKS):
    """[(check, rows, status)]; status is ok / warn / fail for checks with an expectation, else info; error if
    the query failed."""
    out = []
    for check in checks:
        try:
            rows = query(check['sql'])
        except Exception as e:
            out.append((check, str(e).split('\n')[0][:300], 'error'))
            continue
        out.append((check, rows, check['expect'](rows) if check.get('expect') else 'info'))
    return out


def _cell(v):
    if isinstance(v, float):
        return '{:g}'.format(v)
    s = '' if v is None else str(v)
    return s.replace('\n', ' ')


def report(results, out=sys.stdout):
    for check, rows, status in results:
        out.write('\n== {} [{}] {}\n'.format(check['name'], status.upper(), check['doc']))
        if status == 'error':
            out.write('   ' + rows + '\n')
            continue
        if not rows:
            out.write('   (no rows)\n')
            continue
        if check['name'] == 'sample':
            for r in rows:
                out.write('- {} -> {} [{} / {} / {}]\n'.format(
                    r['source_title'], r['target_title'], r['relation'], r['method'], _cell(r['confidence'])))
                if r['mention']:
                    out.write('  mention: {}\n'.format(r['mention']))
                if r['excerpt']:
                    out.write('  {}\n'.format(_cell(r['excerpt'])))
                out.write('  {}{}\n  {}{}\n'.format(SITE, r['source_doc_id'], SITE, r['target_doc_id']))
            continue
        cols = list(rows[0].keys())
        table = [cols] + [[_cell(r[c])[:70] for c in cols] for r in rows]
        widths = [max(len(row[i]) for row in table) for i in range(len(cols))]
        for row in table:
            out.write('   ' + '  '.join(v.ljust(w) for v, w in zip(row, widths)).rstrip() + '\n')
    counts = {}
    for _, _, status in results:
        counts[status] = counts.get(status, 0) + 1
    out.write('\n{}\n'.format(', '.join('{} {}'.format(n, s) for s, n in sorted(counts.items()))))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    parser.add_argument('--engine', help='an SQLAlchemy URL; default: next.obudget.org\'s query API')
    parser.add_argument('--sql', action='store_true', help='print the queries and exit')
    parser.add_argument('--only', nargs='*', help='run only these checks')
    args = parser.parse_args(argv)
    checks = [c for c in CHECKS if not args.only or c['name'] in args.only]
    if args.sql:
        for c in checks:
            print('-- {}: {}\n{};\n'.format(c['name'], c['doc'], c['sql']))
        return 0
    results = run(engine_query(args.engine) if args.engine else api_query, checks)
    report(results)
    failed = any(status == 'fail' or (status == 'error' and check['name'] == 'totals')
                 for check, _, status in results)
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
