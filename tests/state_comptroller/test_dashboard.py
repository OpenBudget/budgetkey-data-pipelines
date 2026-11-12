"""Offline tests for the State Comptroller taxonomy and dashboard tables."""
import datetime
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).parents[2] / 'datapackage_pipelines_budgetkey/pipelines/state_comptroller'))

import taxonomy as tx  # noqa: E402
import dashboard as db  # noqa: E402


# --- taxonomy ----------------------------------------------------------------------------------------------------

def test_tags_keep_only_known_keys():
    assert tx.tags(dict(domain='health', domain2='none', lenses=['procurement', 'bogus', 'procurement'])) == (
        'health', ['health'], ['procurement'])
    assert tx.tags(dict(domain='health', domain2='health', lenses=[])) == ('health', ['health'], [])
    assert tx.tags(dict(domain='made_up', domain2='welfare', lenses=['hr', 'it_cyber', 'integrity', 'equality_social'])) == (
        None, ['welfare'], ['hr', 'it_cyber', 'integrity'])
    assert tx.tags(None) == (None, [], [])


def test_batches_follow_publications_and_chapter_order(monkeypatch):
    monkeypatch.setattr(tx, 'BATCH', 2)
    audits = [dict(chapter_id=i, pub_id=p, chapter_order=o) for i, p, o in
              ((1, 20, 3), (2, 10, 1), (3, 20, 1), (4, 20, 2), (5, None, 1))]
    assert [[a['chapter_id'] for a in b] for b in tx.batches(audits)] == [[2], [3, 4], [1], [5]]


def test_classify_tags_each_audit_and_its_event(monkeypatch):
    def complete(prompt, **kw):
        return False, dict(items=[dict(id=1, domain='health', domain2='welfare', lenses=['service_rights']),
                                  dict(id=99, domain='defense', domain2='none', lenses=[])])  # 99: not in the batch
    monkeypatch.setattr(tx.llm, 'complete', complete)
    rows = tx.classify([dict(chapter_id=1, pub_id=7, publish_date=datetime.date(2020, 1, 1)),
                        dict(chapter_id=2, pub_id=8, publish_date=None)], {7: '7'})
    assert [(r['chapter_id'], r['domain'], r['domains'], r['lenses'], r['event'], r['year']) for r in rows] == [
        (1, 'health', ['health', 'welfare'], ['service_rights'], '7', 2020), (2, None, [], [], None, None)]


def test_special_reports_about_one_event_share_a_key(monkeypatch):
    def complete(prompt, **kw):
        if 'ערוך כל תיאור' in prompt:
            return False, dict(items=[dict(key='12', description='המלחמה ומה שנבדק בה.')])
        if 'שם קצר: השם המקובל' in prompt:
            return False, dict(items=[dict(key='12', short_name='7 באוקטובר')])
        if 'קבץ' in prompt:
            return False, dict(groups=[dict(name='מלחמת חרבות ברזל', when='אוקטובר 2023', description='המלחמה', ids=[30, 12])])
        title = '"title": "' + prompt.split('"title": "')[1].split('"')[0]
        kind = 'thematic' if 'פנסיה' in title else 'event'
        return False, dict(kind=kind, name=title, when='', description='')
    monkeypatch.setattr(tx.llm, 'complete', complete)
    pubs = [dict(pub_id=12, title='דוח מיוחד - העורף', publication_type='special', publish_date=datetime.date(2025, 1, 1)),
            dict(pub_id=30, title='דוח מיוחד - המילואים', publication_type='special', publish_date=datetime.date(2025, 6, 1)),
            dict(pub_id=40, title='דוח מיוחד - קרנות פנסיה', publication_type='special', publish_date=None),
            dict(pub_id=50, title='דוח מיוחד - האסון', publication_type='special', publish_date=None),
            dict(pub_id=60, title='דוח שנתי 75א', publication_type='annual', publish_date=None),
            dict(pub_id=70, title='דוח מיוחד בלי פרקים', publication_type='special', publish_date=None)]
    audits = [dict(chapter_id=i, pub_id=p, title='פרק') for i, p in ((1, 12), (2, 30), (3, 40), (4, 50), (5, 60))]
    reports = {r['pub_id']: r for r in tx.special_reports(pubs, audits)}
    assert sorted(reports) == [12, 30, 40, 50]  # not the annual report, nor one without analysed chapters
    assert reports[12]['event'] == reports[30]['event'] == '12' and reports[30]['event_name'] == 'מלחמת חרבות ברזל'
    assert (reports[40]['event'], reports[40]['kind']) == ('40', 'thematic')
    assert reports[50]['event'] == '50' and reports[50]['kind'] == 'event'  # left out of the groups: its own
    assert reports[12]['event_short_name'] == reports[30]['event_short_name'] == '7 באוקטובר'
    assert reports[40]['event_short_name'] == reports[40]['event_name']  # not shortened: the full name
    assert reports[30]['event_description'] == 'המלחמה ומה שנבדק בה.'  # copy-edited, for the whole group
    assert reports[40]['event_description'] == ''  # not returned by the model: kept


def test_short_names_fall_back_and_tell_duplicates_apart(monkeypatch):
    monkeypatch.setattr(tx.llm, 'complete', lambda prompt, **kw: (False, dict(items=[
        dict(key='1', short_name='ירושלים'), dict(key='2', short_name='ירושלים'),
        dict(key='3', short_name='שם ארוך מדי ' * 10), dict(key='99', short_name='לא קיים')])))
    reports = [dict(pub_id=p, event=str(p), event_name='שם מלא %d' % p, event_description='', publish_date=d)
               for p, d in ((1, datetime.date(2012, 1, 1)), (2, datetime.date(2022, 1, 1)), (3, None), (4, None))]
    tx.add_short_names(reports)
    assert [r['event_short_name'] for r in reports] == ['ירושלים (2012)', 'ירושלים (2022)', 'שם מלא 3', 'שם מלא 4']


def test_label_rows_link_to_filtered_search():
    rows = {(r['axis'], r['key']): r for r in tx.label_rows()}
    assert len(rows) == len(tx.DOMAINS) + len(tx.LENSES)
    assert rows[('lens', 'integrity')]['search_url'] == 'https://next.obudget.org/s?q=&dd=state_comptroller&lens=integrity'
    assert rows[('domain', 'health')]['field'] == 'domains' and rows[('domain', 'defense')]['display_order'] == 0


# --- dashboard -----------------------------------------------------------------------------------------------------

def test_comptroller_terms():
    assert db.comptroller(datetime.date(2005, 7, 3)) == 'goldberg'
    assert db.comptroller(datetime.date(2005, 7, 4)) == 'lindenstrauss'
    assert db.comptroller(datetime.date(2019, 7, 1)) == 'englman' and db.comptroller(None) is None
    assert db.comptroller(datetime.date(1990, 1, 1)) == 'benporat' and db.comptroller(datetime.date(1998, 6, 30)) == 'benporat'
    assert db.comptroller(datetime.date(1988, 4, 10)) is None  # report 38, before Ben-Porat took office


def test_chi2_sf_matches_known_critical_values():
    for k, x in ((1, 3.841), (2, 5.991), (3, 7.815), (4, 9.488), (5, 11.070)):
        assert abs(db.chi2_sf(x, k) - 0.05) < 1e-3
    assert db.chi2_sf(0, 3) == 1.0 and db.chi2_sf(50, 3) < 1e-9


def test_trend_test_tells_a_trend_from_a_comptroller_shift():
    rng = np.random.default_rng(1)
    years = [y for y in range(2000, 2026) for _ in range(80)]
    periods = [db.comptroller(datetime.date(y, 1, 1)) for y in years]
    trend = [rng.random() < 0.05 + 0.01 * (y - 2000) for y in years]
    odds, p_trend, p_comp = db.trend_test(years, periods, trend)
    assert odds > 1.5 and p_trend < 1e-6 and p_comp > 0.01
    assert db.pattern(odds, p_trend, p_comp) == 'rising'
    shift = [rng.random() < (0.3 if p == 'lindenstrauss' else 0.1) for p in periods]
    odds, p_trend, p_comp = db.trend_test(years, periods, shift)
    assert p_comp < 1e-6 and db.pattern(odds, p_trend, p_comp) == 'comptroller'
    flat = [rng.random() < 0.2 for _ in years]
    assert db.pattern(*db.trend_test(years, periods, flat)) == 'stable'


def audit(chapter_id, year, domains, lenses, pub_id=1, kind='annual'):
    return dict(chapter_id=chapter_id, pub_id=pub_id, title='פרק %d' % chapter_id, publication_type=kind,
                follow_up='none', publish_date=datetime.date(year, 6, 1), domain=domains[0], domains=domains,
                lenses=lenses)


def test_counts_share_and_lift():
    audits = db.audit_rows([audit(1, 2010, ['health'], ['hr']), audit(2, 2010, ['defense'], ['hr']),
                            audit(3, 2020, ['health', 'welfare'], ['it_cyber']), audit(4, 2020, ['health'], [])])
    labels = {(r['axis'], r['key']): r['label'] for r in tx.label_rows()}
    rows = {(r['axis'], r['key'], r['period_kind'], r['period']): r for r in db.count_rows(audits, labels)}
    assert (rows[('domain', 'health', 'all', 'all')]['n'], rows[('domain', 'health', 'all', 'all')]['share']) == (3, 0.75)
    h2010 = rows[('domain', 'health', 'year', '2010')]
    assert (h2010['n'], h2010['total'], h2010['share'], h2010['lift']) == (1, 2, 0.5, 0.667)
    assert rows[('lens', 'hr', 'comptroller', 'lindenstrauss')]['share'] == 1.0
    assert ('lens', 'hr', 'comptroller', 'goldberg') not in rows  # no audits in his term in this fixture
    assert rows[('domain', 'welfare', 'year', '2020')]['search_url'].endswith('&domain=welfare')


def test_events_group_reports_and_count_their_findings():
    audits = db.audit_rows([audit(1, 2024, ['defense'], ['emergency_readiness'], pub_id=12),
                            audit(2, 2025, ['health'], ['emergency_readiness'], pub_id=30),
                            audit(3, 2025, ['education'], ['policy_planning'], pub_id=40)])
    reports = [dict(pub_id=p, title='דוח %d' % p, publish_date=d, page_url='https://m/%d' % p, kind=k, event=e,
                    event_name=n, event_short_name=n[:10], event_when=w, event_description='')
               for p, d, k, e, n, w in (
        (30, '2025-06-01', 'event', '12', 'מלחמת חרבות ברזל', 'אוקטובר 2023'),
        (12, '2024-06-01', 'event', '12', 'מלחמת חרבות ברזל', 'אוקטובר 2023'),
        (40, '2025-06-01', 'thematic', '40', 'קרנות פנסיה', None))]
    findings = [dict(chapter_id=1, severity='high'), dict(chapter_id=2, severity='low'), dict(chapter_id=3)]
    rows = {r['event']: r for r in db.event_rows(reports, audits, findings)}
    war = rows['12']
    assert (war['n_reports'], war['n_chapters'], war['n_findings'], war['n_high_severity']) == (2, 2, 2, 1)
    assert [r['pub_id'] for r in war['reports']] == [12, 30] and war['first_published'] == datetime.date(2024, 6, 1)
    assert war['search_url'].endswith('&event=12') and war['chapters'][0]['url'].endswith('/state_comptroller_audits/1')
    assert war['lenses'] == ['emergency_readiness'] and rows['40']['kind'] == 'thematic'
    assert war['short_name'] == 'מלחמת חרבו'
    # a single audit links straight to its page
    assert rows['40']['search_url'] == 'https://next.obudget.org/i/state_comptroller_audits/3'


def test_chronic_issues_are_unfixed_at_the_last_audit():
    def issue(issue_id, verdicts, first='2005-01-01', n_audits=2):
        timeline = [dict(kind='finding', date=first, chapter_id=1, source='הביקורת הראשונה')]
        timeline += [dict(kind='follow_up', date=d, status=s, chapter_id=c, source='מעקב %d' % c) for d, s, c in verdicts]
        timeline += [dict(kind='response', date='2006-01-01', status='in_progress')]
        return dict(issue_id=issue_id, title=issue_id, first_reported=first, n_audits=n_audits, n_responses=1,
                    timeline=timeline)
    issues = [
        issue('long', [('2010-01-01', 'rectified', 2), ('2020-01-01', 'not_rectified', 3)]),
        issue('long-too', [('2020-01-01', 'partially_rectified', 3)]),  # same chain (1 -> 3) as 'long'
        issue('short', [('2008-01-01', 'not_rectified', 4)]),
        issue('fixed', [('2010-01-01', 'not_rectified', 2), ('2015-01-01', 'rectified', 3)]),
        issue('no-verdict', []),
        issue('one-audit', [('2020-01-01', 'not_rectified', 1)], n_audits=1),
    ]
    by_chapter = {1: dict(domains=['health'], lenses=['hr']), 3: dict(domains=['welfare'], lenses=['hr'])}
    issues[0]['bodies'] = ['המשרד לאיכות הסביבה', 'המשרד להגנת הסביבה']
    rows = db.chronic_rows(issues, by_chapter, {'המשרד לאיכות הסביבה': 'המשרד להגנת הסביבה'})
    assert [(r['issue_id'], r['rank'], r['chain_rank']) for r in rows] == [
        ('long', 1, 1), ('long-too', 2, 2), ('short', 3, 1)]
    long = rows[0]
    assert (long['years_open'], long['latest_verdict_label'], long['government_status']) == (15.0, 'לא תוקן', 'in_progress')
    assert (long['domains'], long['lenses']) == (['health', 'welfare'], ['hr'])
    assert long['first_chapter_title'] == 'הביקורת הראשונה' and long['latest_verdict_chapter_id'] == 3
    assert long['url'] == 'https://next.obudget.org/i/state_comptroller_issues/long'
    assert long['bodies'] == ['המשרד להגנת הסביבה']  # an old and a current name of one body: shown once


def test_normalize_when():
    assert tx.normalize_when('יולי - אוגוסט 2014') == 'יולי–אוגוסט 2014'
    assert tx.normalize_when('2020-2022') == '2020–2022' and tx.normalize_when('2003—2006 ') == '2003–2006'
    assert tx.normalize_when(None) is None and tx.normalize_when('אוקטובר 2023 ואילך') == 'אוקטובר 2023 ואילך'


def test_body_names_merge_only_pairs_judged_the_same_body(monkeypatch):
    old, new = 'המשרד לאיכות הסביבה', 'המשרד להגנת הסביבה'
    lists = [[old, new, 'משרד הבריאות'], ['עיריית חיפה', 'עיריית חדרה'],
             ['הרשות העליונה לאשפוז ובריאות לשעת חירום', 'הרשות העליונה לאשפוז ובריאות בשעת חירום'],
             ['אוניברסיטת בר-אילן', 'אוניברסיטת בר אילן'], [new]]
    pairs = tx.candidate_pairs(lists)
    assert ('המשרד להגנת הסביבה', 'משרד הבריאות') not in pairs
    asked = []

    def complete(prompt, **kw):
        listing = json.loads(prompt[prompt.index('['):])
        asked.extend((p['a'], p['b']) for p in listing)
        current = {new, 'הרשות העליונה לאשפוז ובריאות בשעת חירום'}  # the pairs the model judges the same body
        return False, dict(items=[dict(id=p['id'], same=bool(current & {p['a'], p['b']}),
                                       current='b' if p['b'] in current else 'a') for p in listing])
    monkeypatch.setattr(tx.llm, 'complete', complete)
    counts = {old: 1, new: 3, 'אוניברסיטת בר-אילן': 5, 'אוניברסיטת בר אילן': 2,
              'הרשות העליונה לאשפוז ובריאות לשעת חירום': 1, 'הרשות העליונה לאשפוז ובריאות בשעת חירום': 2}
    mapping = tx.canonical_body_names(lists, counts)
    assert mapping[old] == new  # renamed: the name judged current
    assert mapping['אוניברסיטת בר אילן'] == 'אוניברסיטת בר-אילן'  # spelling only: no call, the more frequent one
    assert 'אוניברסיטת בר-אילן' not in mapping
    assert 'עיריית חיפה' not in mapping and 'עיריית חדרה' not in mapping  # asked, judged different bodies
    assert mapping['הרשות העליונה לאשפוז ובריאות לשעת חירום'] == 'הרשות העליונה לאשפוז ובריאות בשעת חירום'
    assert ('אוניברסיטת בר-אילן', 'אוניברסיטת בר אילן') not in asked and ('אוניברסיטת בר אילן', 'אוניברסיטת בר-אילן') not in asked


def test_canonical_bodies_maps_and_drops_duplicates():
    import sc_common as sc
    mapping = {'המשרד לאיכות הסביבה': 'המשרד להגנת הסביבה'}
    assert sc.canonical_bodies(['המשרד לאיכות הסביבה', 'משרד הבריאות', 'המשרד להגנת הסביבה', ' משרד  הבריאות', ''],
                               mapping) == ['המשרד להגנת הסביבה', 'משרד הבריאות']
    assert sc.canonical_bodies(None, mapping) == []
