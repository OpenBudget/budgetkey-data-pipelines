"""ri_owners: structural links from a ministry or authority to its own budget chapters (relation budget_owner)."""
import datetime
import os

import dataflows as DF

from datapackage_pipelines_budgetkey.pipelines.budgetkey.elasticsearch import add_related_items as A
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_budget as RB
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_owners as O


def body(name, entity_name, entity_id, chapters, kind='government_office', status='ok'):
    return dict(name=name, entity_name=entity_name, entity_id=entity_id, entity_kind=kind,
                budget_chapters=chapters.split('|'), status=status)


HEALTH = body('משרד הבריאות', 'משרד הבריאות/המשרד הראשי', '500100904', '0024|0067')
RAMBAM = body('מרכז רפואי רמב"ם', 'משרד הבריאות/בי"ח הממשלתי רמב"ם', '500107040', '0024|0067|0094')
BNEI_ZION = body('מרכז רפואי בני ציון', 'בית חולים בני ציון', '500107420', '0024|0067|0094')
KNESSET = body('הכנסת', 'הכנסת', '500100029', '0002')
ELECTIONS = body('ועדת הבחירות המרכזית לכנסת', 'ועדת הבחירות המרכזית לכנסת', '500105606', '0014')
AEC = body('הוועדה לאנרגיה אטומית', 'משרד רוה"מ/הועדה לאנרגיה אטומית', '500100102', '0035')
SOREQ = body('משרד רוה"מ/ממ"ג שורק', 'משרד רוה"מ/ממ"ג שורק', '500104799', '0004|0035|0089')
WATER = body('הרשות הממשלתית למים ולביוב', 'הרשות הממשלתית למים ולביוב', '500101753', '0041|0073')
POLICE = body('משטרת ישראל', 'משטרת ישראל-מטה ארצי', '500101944', '0007')
PRISONS = body('שירות בתי הסוהר', 'משרד לבטחון פנים/שרות בתי הסוהר', '500102025', '0007')
HOME_FRONT = body('פיקוד העורף', 'צה"ל/מפקדת פיקוד העורף', '500100755', '0015')
SCIENCE = body('משרד החדשנות, המדע והטכנולוגיה', 'משרד המדע והטכנולוגיה/המשרד הראש', '500101878', '0019')
SCIENCE_OLD = body('משרד המדע והטכנולוגיה', 'משרד המדע והטכנולוגיה/המשרד הראש', '500101878', '0019')
DRAFT = body('טיוטה', 'משרד הטיוטות/המשרד הראשי', '1', '0024', status='draft')
BODIES = [HEALTH, RAMBAM, BNEI_ZION, KNESSET, ELECTIONS, AEC, SOREQ, WATER, POLICE, PRISONS, HOME_FRONT, SCIENCE,
          SCIENCE_OLD, DRAFT]
TITLES = {'0002': 'הכנסת', '0007': 'המשרד לביטחון לאומי', '0014': 'בחירות ומימון מפלגות', '0015': 'משרד הביטחון',
          '0019': 'מדע, תרבות וספורט', '0024': 'משרד הבריאות', '0035': 'הועדה לאנרגיה אטומית',
          '0041': 'רשות ממשלתית למים וביוב', '0067': 'בריאות', '0073': 'מפעלי מים', '0094': 'בתי חולים ממשלתים'}


def names(found):
    return {c: [b['name'] for b in bodies] for c, bodies in found.items()}


def test_owners_exclude_units():
    found = names(O.owners(BODIES, TITLES))
    # the headquarters, not the hospitals sharing its chapters (a draft row is never an owner)
    assert found['0024'] == found['0067'] == ['משרד הבריאות']
    # a chapter only units list has no owner: the hospitals' 0094, a ministry without an HQ entity
    assert '0094' not in found and '0007' not in found and '0015' not in found
    # by title: a top-level body, or a unit that is the chapter's own body (not another unit listing it)
    assert found['0002'] == ['הכנסת'] and found['0035'] == ['הוועדה לאנרגיה אטומית']
    assert found['0041'] == ['הרשות הממשלתית למים ולביוב']
    # the one, top-level body listing a chapter
    assert found['0014'] == ['ועדת הבחירות המרכזית לכנסת']
    # an owner's other chapter (an authority's development chapter)
    assert found['0073'] == ['הרשות הממשלתית למים ולביוב']
    # one body per entity
    assert found['0019'] == ['משרד החדשנות, המדע והטכנולוגיה']
    # chapters not in the budget are skipped (0004, 0089: not in TITLES)
    assert '0004' not in found


def test_reviewed_co_owners():
    culture = body('משרד התרבות והספורט', 'משרד התרבות והספורט', '500106646', '0019')
    found = names(O.owners(BODIES + [culture], TITLES, co_owners={'0019': ['500106646'], '0094': ['500100904'],
                                                                 '0099': ['500100904'], '0024': ['999']}))
    # a second owner beside the rules' one
    assert found['0019'] == ['משרד החדשנות, המדע והטכנולוגיה', 'משרד התרבות והספורט']
    # or the only one; a chapter not in the budget, or an entity not in bodies.csv, adds nothing
    assert found['0094'] == ['משרד הבריאות'] and '0099' not in found and found['0024'] == ['משרד הבריאות']
    # without co-owners, culture isn't an owner of 0019
    assert names(O.owners(BODIES + [culture], TITLES, co_owners={}))['0019'] == ['משרד החדשנות, המדע והטכנולוגיה']


def test_unit_and_headquarters_names():
    assert O.is_headquarters('משרד המדע והטכנולוגיה/המשרד הראש') and O.is_headquarters('ביטוח לאומי המשרד הראשי')
    assert O.is_unit('משרד האוצר/רשות המסים') and not O.is_unit('משרד הבריאות/המשרד הראשי')
    assert not O.is_unit('בית חולים בני ציון') and not O.is_unit('')


def budget_row(code, year, title, history=None):
    return dict(code=code, year=year, title=title, depth=1, hierarchy=[], history=history or {})


def test_owner_links_resolve_to_the_connected_item():
    budget = RB.BudgetIndex([
        budget_row('0024', 2026, 'משרד הבריאות', {'2020': ['0024:משרד הבריאות']}),
        # 0067's items: its own up to 2019, then folded into a later item's history
        budget_row('0067', 2019, 'פיתוח בריאות'),
        budget_row('0068', 2026, 'בריאות', {'2025': ['0067:בריאות'], '2026': ['0068:בריאות']}),
        budget_row('0002', 2026, 'הכנסת'),
    ])
    links = {(l['source_doc_id'], l['target_doc_id']): l for l in O.owner_links([HEALTH, RAMBAM, KNESSET], budget)}
    assert set(links) == {('org/government_office/500100904', 'budget/0024/2026'),
                          ('org/government_office/500100904', 'budget/0068/2026'),
                          ('org/government_office/500100029', 'budget/0002/2026')}
    chapter = links[('org/government_office/500100904', 'budget/0068/2026')]
    # the chapter's latest year, with that year's code and title
    assert (chapter['target_year'], chapter['target_code'], chapter['target_title']) == (2025, '0067', 'בריאות')
    assert chapter['relation'] == 'budget_owner' and chapter['confidence'] == 1.0
    assert chapter['method'] == 'structural:bodies' and chapter['source_kind'] == 'entities'
    assert chapter['source_title'] == 'משרד הבריאות' and chapter['source_date'] is None
    assert chapter['excerpt'] is None and chapter['source_part_id'] is None
    assert len({l['link_id'] for l in links.values()}) == 3


def test_merge_adds_owner_links(tmp_path, monkeypatch):
    from datapackage_pipelines_budgetkey.pipelines.related_items import merge_sources as MS
    from datapackage_pipelines_budgetkey.pipelines.related_items.link_source import output_flow
    link = dict(link_id='1', source_doc_id='gov_decisions/a', source_kind='gov_decisions', source_part_id=None,
                source_title='t', source_date=datetime.date(2026, 1, 1), target_doc_id='budget/0024/2026',
                target_kind='budget', target_title='x', target_year=2026, target_code='0024', relation='funded',
                mention='x', excerpt='e', confidence=0.9, method='code', linker_version='1')
    output_flow([link], [dict(source_doc_id='gov_decisions/a', source_kind='gov_decisions', source_part_id=None,
                              mention_kind='budget', mention='x', status='linked', method='code',
                              target_doc_id='budget/0024/2026', confidence=0.9, details={}, excerpt='e')],
                str(tmp_path / 'gd')).process()
    budget = RB.BudgetIndex([budget_row('0024', 2026, 'משרד הבריאות')])
    monkeypatch.setattr(MS, 'budget_owner_links', lambda: O.owner_links([HEALTH, RAMBAM], budget))
    out = tmp_path / 'all'
    DF.Flow(MS.flow({'sources': [str(tmp_path / 'gd')], 'out-path': str(out), 'dump-to-sql': False,
                     'budget-owners': True})).process()
    assert os.path.exists(out / 'related_items.csv')
    rows = DF.Flow(DF.load(str(out / 'datapackage.json'), resources=['related_items'])).results()[0][0]
    assert [(r['source_doc_id'], r['relation']) for r in rows] == [
        ('gov_decisions/a', 'funded'), ('org/government_office/500100904', 'budget_owner')]


def test_owner_first_in_mentioned_in():
    owner = dict(source_doc_id='org/government_office/500100904', source_kind='entities', source_part_id=None,
                 source_title='משרד הבריאות', source_date=None, target_doc_id='budget/0024/2026', target_kind='budget',
                 target_title='משרד הבריאות', target_year=2026, target_code='0024', relation='budget_owner',
                 excerpt=None, confidence=1.0)
    mentions = [dict(owner, source_doc_id='gov_decisions/{}'.format(k), source_kind='gov_decisions',
                     source_date='2024-01-{:02d}'.format(k + 1), relation='funded', confidence=0.9, excerpt='e')
                for k in range(5)]
    index = A.RelatedItems(mentions + [owner], 'budget/', max_mentioned_in=3)
    fields = index.fields('budget/0024/2026')
    # the owner first though it has no date, and the cap doesn't drop it
    assert [e['doc_id'] for e in fields['mentioned_in']] == [
        'org/government_office/500100904', 'gov_decisions/4', 'gov_decisions/3']
    assert fields['mentioned_in_count'] == 6
    # and the org lists its chapter
    org = A.RelatedItems([owner], 'org/').fields('org/government_office/500100904')
    assert [(e['doc_id'], e['relation']) for e in org['related_items']] == [('budget/0024/2026', 'budget_owner')]
