"""check_links: its queries are single SELECTs the query API accepts; expectations and the report."""
import io
import re

from datapackage_pipelines_budgetkey.pipelines.related_items import check_links as C


def test_queries_are_single_selects():
    names = [c['name'] for c in C.CHECKS]
    assert len(names) == len(set(names))
    for c in C.CHECKS:
        sql = c['sql'].strip()
        assert re.match(r'(select|with)\b', sql, re.I), c['name']
        assert ';' not in sql and '(?:' not in sql, c['name']  # the API wraps the query; text() binds '(?:'
        assert not re.search(r'(?<![:\w\\]):(\w+)(?!:)', sql), c['name']  # SQLAlchemy's bind-parameter pattern


def test_expectations():
    check = {c['name']: c for c in C.CHECKS}
    assert check['totals']['expect']([dict(links=10, min_confidence=0.8, versions=1)]) == 'ok'
    assert check['totals']['expect']([dict(links=10, min_confidence=0.5, versions=1)]) == 'warn'
    assert check['totals']['expect']([dict(links=0, min_confidence=None, versions=0)]) == 'fail'
    assert check['self_links']['expect']([dict(n=0)]) == 'ok'
    assert check['self_links']['expect']([dict(n=2)]) == 'fail'
    dangling = check['dangling_entities']['expect']
    assert dangling([dict(links=0, dangling=0, examples=None)]) == 'ok'
    assert dangling([dict(links=1000, dangling=10, examples=[])]) == 'ok'
    assert dangling([dict(links=1000, dangling=11, examples=[])]) == 'fail'
    assert check['duplicate_link_ids']['expect']([]) == 'ok'
    assert check['missing_fields']['expect']([dict(source_kind='x')]) == 'warn'


def test_run_reports_errors_and_goes_on():
    def query(sql):
        if 'state_comptroller_chapters' in sql:
            raise RuntimeError('relation "state_comptroller_chapters" does not exist\nLINE 1')
        if 'row_number()' in sql:
            return [dict(source_doc_id='gov_decisions/a', source_title='s', target_doc_id='org/association/1',
                         target_title='t', relation='funded', method='alias', confidence=0.93, mention='m',
                         excerpt='line\nbreak')]
        return []
    results = C.run(query)
    status = {c['name']: s for c, _, s in results}
    assert status['dangling_audits'] == 'error' and status['duplicate_link_ids'] == 'ok'
    assert status['totals'] == 'fail' and status['by_kind'] == 'info'
    out = io.StringIO()
    C.report(results, out)
    text = out.getvalue()
    assert 'relation "state_comptroller_chapters" does not exist' in text and 'LINE 1' not in text
    assert 'https://next.obudget.org/i/org/association/1' in text and 'line break' in text
