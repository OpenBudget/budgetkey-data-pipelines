"""Offline tests for the AI analysis pages as a related-items source: markdown links, site-link resolution, linking."""
import datetime
import json
import os

import dataflows as DF

from datapackage_pipelines_budgetkey.pipelines.related_items import linker as LK
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_site_links as RS
from datapackage_pipelines_budgetkey.pipelines.related_items import sources as S

SITE = 'https://next.obudget.org/i/'
SHORT_BUDGET = RS.short_id('budget/00206101/2026')
SHORT_BUDGET_OLD = RS.short_id('budget/0020610105/2019')  # an older year of the same connected item
SHORT_DECISION = RS.short_id('gov_decisions/abc')
SHORT_UNKNOWN = 'ffffffffffff'

CHART = {'type': 'plotly', 'title': 'תקציב החינוך המיוחד', 'chart': []}
BODY = '\n'.join([
    '## בקצרה',
    'הממשלה מממנת חינוך מיוחד. עיקר הכסף עובר דרך [חינוך מיוחד]({s}{b}) במשרד החינוך. משפט אחר.'.format(
        s=SITE, b=SHORT_BUDGET),
    '',
    '## כמה כסף ולאן',
    '```plotly',
    json.dumps(CHART, ensure_ascii=False),
    '```',
    '',
    '*הסעיפים התקציביים בתרשים:* [חינוך מיוחד]({s}{b}) (20.61.01) · [שעות]({s}{o}) (20.61.01.05) · כונניות'.format(
        s=SITE, b=SHORT_BUDGET, o=SHORT_BUDGET_OLD),
    '',
    '## התקשרויות וספקים',
    '| ספק | מטרה | היקף |',
    '|---|---|---|',
    '| [מסע ישראלי בע״מ]({s}org/company/513918961) | [הסעות]({s}{u}) | 1 מיליון ₪ |'.format(s=SITE, u=SHORT_UNKNOWN),
    '',
    '## החלטות ממשלה',
    '| החלטה | מספר |',
    '|---|---|',
    '| [טיוטת חוק [נוסח חדש]]({s}{d}) | 814 |'.format(s=SITE, d=SHORT_DECISION),
    '',
    '## על הנתונים',
    '**סעיפי התקציב שנכללו בניתוח:** [חינוך מיוחד]({s}{b}) (20.61.01) · [שעות]({s}{o}) (20.61.01.05) · אחר'.format(
        s=SITE, b=SHORT_BUDGET, o=SHORT_BUDGET_OLD),
])
PAGE = dict(slug='special-education', title='חינוך מיוחד', body=BODY, generated_at='2026-10-02T22:05:43')


# --- markdown ------------------------------------------------------------------------------------------------------

def test_markdown_links_places_and_contexts():
    links = S.markdown_links(BODY)
    assert len(links) == 8
    prose, caption, caption_old, org, unknown, decision, scope, scope_old = links
    assert (prose['place'], prose['section']) == ('prose', 'בקצרה')
    # the sentence holding the link, not the whole paragraph
    assert prose['context'] == 'עיקר הכסף עובר דרך חינוך מיוחד במשרד החינוך.'
    assert caption['place'] == 'list' and caption['context'] == 'תרשים "תקציב החינוך המיוחד" - הסעיפים התקציביים בתרשים'
    assert caption_old['url'] == SITE + SHORT_BUDGET_OLD
    # a table row as "header: cell" pairs
    assert org['place'] == 'table'
    assert org['context'] == 'ספק: מסע ישראלי בע״מ · מטרה: הסעות · היקף: 1 מיליון ₪'
    assert unknown['mention'] == 'הסעות'
    # link text with brackets of its own
    assert decision['mention'] == 'טיוטת חוק [נוסח חדש]'
    assert (scope['place'], scope['context'], scope['section']) == ('list', 'סעיפי התקציב שנכללו בניתוח', 'על הנתונים')


def test_markdown_links_cap_long_contexts():
    long_row = '| [פריט]({}{}) | {} |'.format(SITE, SHORT_BUDGET, 'מילה ' * 200)
    links = S.markdown_links('\n'.join(['| שם | תיאור |', '|---|---|', long_row]))
    assert len(links[0]['context']) <= S.EXCERPT_MAX
    sentence = 'פתיחה ' * 100 + '[פריט]({}{}) '.format(SITE, SHORT_BUDGET) + 'סיום ' * 100
    links = S.markdown_links(sentence)
    assert len(links[0]['context']) <= S.EXCERPT_MAX and 'פריט' in links[0]['context']


def test_analysis_documents():
    doc, = S.analysis_documents([PAGE])
    assert (doc['doc_id'], doc['kind'], doc['date']) == ('analysis/special-education', 'analysis',
                                                         datetime.date(2026, 10, 2))
    links = doc['hints']['links']
    # best placed first: prose, then tables, then lists
    assert [link['place'] for link in links] == ['prose'] + ['table'] * 3 + ['list'] * 4
    assert links[0]['excerpt'] == 'בקצרה: עיקר הכסף עובר דרך חינוך מיוחד במשרד החינוך.'
    org = [link for link in links if '/org/' in link['url']][0]
    assert org['relation'] == 'contractor'
    assert {link['relation'] for link in links if '/org/' not in link['url']} == {'mentioned'}


# --- resolution -----------------------------------------------------------------------------------------------------

def test_parse():
    assert RS.parse(SITE + 'abcdef012345') == ('short', 'abcdef012345')
    assert RS.parse(SITE + 'org/company/513918961') == ('org', ('company', '513918961'))
    assert RS.parse(SITE + 'budget/00206101/2026') == ('doc', 'budget/00206101/2026')
    assert RS.parse('https://next.obudget.org/dashboards/x') == (None, None)
    assert RS.parse('https://example.com/i/abcdef012345') == (None, None)


def fake_query(tables):
    """ri_db.query over canned rows: {table name: [row]}, filtered by the short ids in the SQL."""
    def query(sql, columns):
        table = sql.split(' from ')[1].split()[0]
        wanted = set(sql.split(' in (')[-1].rstrip(')').replace("'", '').replace(' ', '').split(','))
        out = []
        for row in tables.get(table, []):
            if table == 'entities':
                if row['id'] in wanted:
                    out.append({c: row.get(c) for c in columns})
                continue
            if 'jsonb_each' in sql:
                for y, cts in (row.get('history') or {}).items():
                    for ct in cts:
                        s = RS.short_id('budget/{}/{}'.format(ct.split(':')[0], y))
                        if s in wanted:
                            out.append(dict(code=row['code'], year=row['year'], y=y, ct=ct, site_short_id=s))
                continue
            spec = [k for k in RS.KINDS if k['table'] == table]
            keys, kind = (spec[0]['keys'], spec[0]['kind']) if spec else (['code', 'year'], 'budget')
            s = RS.short_id('/'.join([kind] + [str(row[k]) for k in keys]))
            if s in wanted:
                out.append(dict(row, site_title=row.get('title') or row.get(spec[0]['title'] if spec else 'title'),
                                site_short_id=s))
        return out
    return query


def test_site_link_index(monkeypatch):
    monkeypatch.setattr(RS.ri_db, 'query', fake_query({
        'budget': [dict(code='00206101', year=2026, title='חינוך מיוחד',
                        history={'2019': ['0020610105:שעות'], '2026': ['00206101:חינוך מיוחד']})],
        'government_decisions': [dict(id='abc', title='החלטה')],
        'entities': [dict(id='513918961', kind='company', name='מסע ישראלי')],
        'supports_by_request_year': [dict(budget_code='0019420216', year_requested=2024, short_id='580099638',
                                          request_type='א3', support_title='תמיכה', recipient='עמותה')],
    }))
    support = 'supports/0019420216/2024/580099638/א3'
    urls = [SITE + SHORT_BUDGET, SITE + SHORT_BUDGET_OLD, SITE + SHORT_DECISION, SITE + SHORT_UNKNOWN,
            SITE + 'org/company/513918961', SITE + 'org/company/1', SITE + 'gov_decisions/abc',
            SITE + RS.short_id(support), 'https://next.obudget.org/dashboards/x']
    idx = RS.SiteLinkIndex.load(urls)
    budget, _ = idx.resolve(SITE + SHORT_BUDGET)
    assert budget == dict(doc_id='budget/00206101/2026', kind='budget', title='חינוך מיוחד', year=2026,
                          code='00206101')
    # an older year resolves to the connected item, keeping that year's code and title
    old, _ = idx.resolve(SITE + SHORT_BUDGET_OLD)
    assert (old['doc_id'], old['year'], old['code'], old['title']) == ('budget/00206101/2026', 2019, '0020610105',
                                                                       'שעות')
    assert idx.resolve(SITE + SHORT_DECISION)[0]['doc_id'] == 'gov_decisions/abc'
    assert idx.resolve(SITE + 'gov_decisions/abc')[0]['doc_id'] == 'gov_decisions/abc'  # a readable doc id
    assert idx.resolve(SITE + 'org/company/513918961')[0] == dict(doc_id='org/company/513918961', kind='entities',
                                                                  title='מסע ישראלי')
    # a doc id built from the row's own keys (supports have a short_id column of their own)
    assert idx.resolve(SITE + RS.short_id(support))[0]['doc_id'] == support
    assert idx.resolve(SITE + SHORT_UNKNOWN) == (None, 'not_found')
    assert idx.resolve(SITE + 'org/company/1') == (None, 'not_found')
    assert idx.resolve('https://next.obudget.org/dashboards/x') == (None, 'not_an_item')


def test_kind_columns_dont_collide():
    for spec in RS.KINDS:
        assert not {'site_title', 'site_short_id'} & set(spec['keys'] + spec.get('extra', []))


# --- linking ------------------------------------------------------------------------------------------------------

def test_linker_site_links():
    budget = dict(doc_id='budget/00206101/2026', kind='budget', title='חינוך מיוחד', year=2026, code='00206101')
    targets = LK.Targets(site_links=RS.SiteLinkIndex({
        SITE + SHORT_BUDGET: budget,
        SITE + SHORT_BUDGET_OLD: dict(budget, year=2019, code='0020610105', title='שעות'),
        SITE + SHORT_DECISION: dict(doc_id='gov_decisions/abc', kind='gov_decisions', title='החלטה'),
        SITE + 'org/company/513918961': dict(doc_id='org/company/513918961', kind='entities', title='מסע ישראלי'),
        SITE + SHORT_UNKNOWN: 'not_found',
    }))
    doc, = S.analysis_documents([PAGE])
    links, mentions = LK.DocLinker(doc, targets, None).run()
    by_target = {link['target_doc_id']: link for link in links}
    # one link per target: both budget URLs (two years of one connected item) and all their occurrences fold
    assert len(links) == 3 and set(by_target) == {'budget/00206101/2026', 'gov_decisions/abc',
                                                  'org/company/513918961'}
    b = by_target['budget/00206101/2026']
    assert (b['method'], b['confidence'], b['relation'], b['source_part_id']) == ('site_link', 1.0, 'mentioned', None)
    assert b['excerpt'] == 'בקצרה: עיקר הכסף עובר דרך חינוך מיוחד במשרד החינוך.'  # the prose occurrence
    assert (b['target_year'], b['target_code']) == (2026, '00206101')
    assert by_target['org/company/513918961']['relation'] == 'contractor'
    # one mention per URL, with every section it's in; the unresolved one is kept
    by_url = {m['details']['url']: m for m in mentions}
    assert len(mentions) == 5
    assert by_url[SITE + SHORT_BUDGET]['details']['sections'] == ['בקצרה', 'כמה כסף ולאן', 'על הנתונים']
    assert by_url[SITE + SHORT_BUDGET]['details']['occurrences'] == 3
    unknown = by_url[SITE + SHORT_UNKNOWN]
    assert (unknown['status'], unknown['mention'], unknown['details']['short_id']) == ('not_found', 'הסעות',
                                                                                     SHORT_UNKNOWN)


# --- merge --------------------------------------------------------------------------------------------------------

def test_merge_skips_a_missing_source(tmp_path):
    from datapackage_pipelines_budgetkey.pipelines.related_items import merge_sources as MS
    from datapackage_pipelines_budgetkey.pipelines.related_items.link_source import output_flow
    link = dict(link_id='1', source_doc_id='gov_decisions/a', source_kind='gov_decisions', source_part_id=None,
                source_title='t', source_date=datetime.date(2026, 1, 1), target_doc_id='org/x/1',
                target_kind='entities', target_title='x', target_year=None, target_code=None, relation='mentioned',
                mention='x', excerpt='e', confidence=0.9, method='alias', linker_version='1')
    output_flow([link], [dict(source_doc_id='gov_decisions/a', source_kind='gov_decisions', source_part_id=None,
                              mention_kind='org', mention='x', status='linked', method='alias',
                              target_doc_id='org/x/1', confidence=0.9, details={}, excerpt='e')],
                str(tmp_path / 'gd')).process()
    out = tmp_path / 'all'
    DF.Flow(MS.flow({'sources': [str(tmp_path / 'gd'), str(tmp_path / 'analysis')], 'out-path': str(out),
                     'dump-to-sql': False})).process()
    assert os.path.exists(out / 'related_items.csv')
    rows = DF.Flow(DF.load(str(out / 'datapackage.json'), resources=['related_items'])).results()[0][0]
    assert [r['link_id'] for r in rows] == ['1']
