import datetime

from datapackage_pipelines_budgetkey.pipelines.analysis import topics as T
from datapackage_pipelines_budgetkey.pipelines.analysis.history import mirror_code


def candidate(**kw):
    c = dict(slug='road-safety', question_he='מה הממשלה עושה בנושא בטיחות בדרכים?', category='transport-and-infrastructure',
             budget_codes=['40.53', '79.52', '99.99'], temporal=False, verdict=None)
    c.update(kw)
    return c


def test_check_candidate():
    c = candidate()
    assert T.check_candidate(c, set(), {'40.53', '79.52'}) == []
    assert c['budget_codes'] == ['40.53', '79.52'] and c['office_groups'] == ['transport']
    problems = T.check_candidate(candidate(slug='Bad Slug', question_he='למה?', budget_codes=[]), set(), set())
    assert len(problems) == 3


def test_expiry():
    now = datetime.datetime(2026, 10, 2)
    assert T.expires_at(candidate(), now) is None
    assert T.expires_at(candidate(temporal=True, relevance_months=100), now) == datetime.date(2029, 9, 30)
    registry = [dict(slug='a', status='active', expires_at=datetime.date(2026, 1, 1)),
                dict(slug='b', status='active', expires_at=None)]
    T.expire(registry, today=datetime.date(2026, 10, 2))
    assert [t['status'] for t in registry] == ['expired', 'active']


def test_mirror_code():
    assert mirror_code('20.62.01') == '00206201'
