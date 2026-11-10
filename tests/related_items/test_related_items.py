"""Offline tests for the related-items linker: extractors, resolvers and the linker with canned LLM answers."""
import datetime

import pytest

from datapackage_pipelines_budgetkey.pipelines.related_items import linker as LK
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_budget as RB
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_comptroller as RC
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_entities as E
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_llm as L
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_text as T
from datapackage_pipelines_budgetkey.pipelines.related_items import sources as S
from datapackage_pipelines_budgetkey.pipelines.related_items.ri_decisions import DecisionIndex

D = datetime.date


# --- names -------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize('a,b', [
    ('מ. מ. כוכב יאיר', 'מועצה מקומית כוכב יאיר'),
    ('עירית אריאל', 'עיריית אריאל'),
    ('חברת החשמל לישראל בע"מ', 'חברת החשמל לישראל'),
    ('רשות המיסים בישראל', 'רשות המסים בישראל'),
    ('עמותת אלי"ן (ע"ר)', 'עמותת אלין'),
    ('המשרד להגנת הסביבה', 'המשרד להגנת הסביבה'),
    ('משרד האנרגייה והתשתיות', 'משרד האנרגיה והתשתיות'),
])
def test_normalize_name(a, b):
    assert T.normalize_name(a) == T.normalize_name(b)


def test_normalize_name_keeps_different_bodies_apart():
    assert T.normalize_name('משרד האוצר/רשות המסים') != T.normalize_name('רשות המסים')
    assert T.normalize_name('עיריית חיפה') != T.normalize_name('חיפה')


# --- corporation numbers -----------------------------------------------------------------------------------------

@pytest.mark.parametrize('number,valid', [
    ('500100011', True), ('520033374', True), ('513986042', True), ('500500285', True),
    ('500500286', False), ('123456782', False), ('50010001', False),
])
def test_valid_corporation_number(number, valid):
    assert T.valid_corporation_number(number) == valid


def test_corporation_numbers():
    text = 'רשות הטבע והגנים ח.פ 500500285, החברה (51-398604-2), טלפון 052-1234567, מספר 5205551234 ו-500500286'
    assert [m['number'] for m in T.corporation_numbers(text)] == ['500500285', '513986042']


# --- decision citations ------------------------------------------------------------------------------------------

def cite(text):
    return [(c['number'], c['date'], c['date_precision'], c['government'], c['relation'])
            for c in T.decision_citations(text)]


def test_decision_citation_with_date():
    assert cite("בהמשך להחלטת הממשלה מספר 3200 מיום 4.7.2025 בעניין") == [('3200', D(2025, 7, 4), 'day', None, 'cites')]


def test_decision_citation_hebrew_date_with_gregorian():
    text = 'ולהחלטת ממשלה מס\' 4482 מיום ד\' בחשוון התשפ"ז (15 באוקטובר 2026) שהאריכה'
    assert cite(text) == [('4482', D(2026, 10, 15), 'day', None, 'cites')]


def test_decision_citation_lists():
    assert cite("בהמשך להחלטות הממשלה מס' 1478 מיום 29 בפברואר 2024 ומס' 1479 מיום 1 במרץ 2024 ולאור") == [
        ('1478', D(2024, 2, 29), 'day', None, 'cites'), ('1479', D(2024, 3, 1), 'day', None, 'cites')]
    # A date given once at the end of a list applies to all of it.
    assert cite("בהמשך להחלטות הממשלה מס' 2443 ו-2444 מיום 15.02.2014, הרשות") == [
        ('2443', D(2014, 2, 15), 'day', None, 'cites'), ('2444', D(2014, 2, 15), 'day', None, 'cites')]


def test_decision_citation_relations():
    assert cite("לבטל את החלטת הממשלה מס' 770 מיום 31.1.2021.")[0][4] == 'cancels'
    assert cite("לתקן את החלטת הממשלה מס' 3425 מיום 26.10.2025 שעניינה")[0][4] == 'amends'


def test_decision_citation_government_and_month():
    assert cite("החלטה מס' 2397 של הממשלה ה-31 מיום 3.3.2007") == [('2397', D(2007, 3, 3), 'day', 31, 'cites')]
    assert cite('החלטת ממשלה 1577 מיוני 2014') == [('1577', D(2014, 6, 1), 'month', None, 'cites')]


def test_decision_citation_skips_other_bodies_and_aliases():
    assert cite("מועצת מקרקעי ישראל החלטה מס' 1500") == []
    assert cite('החלטת הממשלה מספטמבר 1993') == []
    # "(להלן – החלטה 1306)" names the decision cited before it.
    text = "לסעיף 15(ג) להחלטת הממשלה מס' 1306 מיום 14.01.2024 (להלן – החלטת ממשלה 1306)"
    assert [c[0] for c in cite(text)] == ['1306']


def decisions():
    rows = [
        dict(id='a', procedure_number_str='3200', government='הממשלה ה- 37', doc_published_date='2025-07-04',
             policy_type='החלטות ממשלה', title='היתר לקבלת תרומה'),
        dict(id='b', procedure_number_str='3200', government='הממשלה ה- 34, בנימין נתניהו',
             doc_published_date='2017-11-19', policy_type='החלטות ממשלה', title='אחרת'),
        # A copy of 'a' (same number, date and title) and a decision of another body with the same number.
        dict(id='c', procedure_number_str='3200', government='הממשלה ה- 37', doc_published_date='2025-07-04',
             policy_type='החלטות ממשלה', title='היתר לקבלת תרומה'),
        dict(id='d', procedure_number_str='3200', government=None, doc_published_date='2025-07-04',
             policy_type='החלטות ועדת פטור', title='עסקה'),
        dict(id='e', procedure_number_str='1383', government='הממשלה ה- 37', doc_published_date='2024-02-12',
             policy_type='החלטות ממשלה', title='תמרוץ'),
        dict(id='f', procedure_number_str='1383', government=None, doc_published_date='2014-02-26',
             policy_type='החלטות ממשלה', title='ישנה'),
    ]
    return DecisionIndex(rows)


def test_decision_resolution():
    idx = decisions()
    c = dict(number='3200', date=D(2025, 7, 4), date_precision='day', government=None)
    entry, confidence, how = idx.resolve(c, D(2026, 1, 1))
    assert entry['id'] == 'a' and how == 'date'
    # The publication date can be a week off the date cited: resolved through the government.
    c = dict(number='1383', date=D(2024, 2, 4), date_precision='day', government=None)
    entry, confidence, how = idx.resolve(c, D(2026, 1, 1))
    assert entry['id'] == 'e' and how == 'date_close'
    # Not in the table (a decision of the 31st government).
    c = dict(number='1383', date=D(2007, 3, 3), date_precision='day', government=None)
    assert idx.resolve(c, D(2026, 1, 1))[0] is None


def test_decision_resolution_without_date():
    idx = decisions()
    c = dict(number='3200', date=None, date_precision=None, government=None)
    # A comptroller report citing a bare number: not enough to tell which government's decision it is.
    assert idx.resolve(c, D(2026, 1, 1)) == (None, 0, 'no_date')
    # A government decision citing its own government's earlier decision.
    entry, confidence, how = idx.resolve(c, D(2026, 1, 1), same_government_default=True)
    assert entry['id'] == 'a' and how == 'same_government'
    # A decision can't cite a later one.
    assert idx.resolve(c, D(2017, 11, 1), same_government_default=True)[0] is None
    assert idx.resolve(c, D(2017, 12, 1), same_government_default=True)[0]["id"] == "b"


# --- budget codes and tenders --------------------------------------------------------------------------------------

def test_budget_codes():
    text = 'בתקנה 20-38-02-01 ובתכנית התקציבית 20.38 בסך 5 מיליון ש"ח, סעיף 19 לחוק, תקנה 25 (1) לתקנות'
    assert [m['code'] for m in T.budget_codes(text)] == ['0020380201', '002038']
    # Two groups of digits look like many other things; they count only in a sentence about money.
    assert T.budget_codes('ראו סעיף 12.04 לנוהל') == []


def test_budget_codes_without_separators():
    text = 'בחלוקה של 210 מלש"ח בתוכנית 062001 במשרד הפנים ו-90 מלש"ח בתוכנית 23012061, בתוכנית פעילות ופרויקטים ' \
           'שמספרה 206701 שעניינה גרעינים תורניים'
    assert [m['code'] for m in T.budget_codes(text)] == ['00062001', '0023012061', '00206701']
    # Years and other numbers are not program numbers.
    assert T.budget_codes('תוכנית 2025 ותכנית 123456789 ותכנית 12345') == []


def test_tender_numbers():
    text = "מכרז מס' 33/2006 ומכרז 2020 ומכרז פומבי 12-2019"
    assert [m['number'] for m in T.tender_numbers(text)] == ['33/2006', '12-2019']


# --- budget index ------------------------------------------------------------------------------------------------

def budget():
    return RB.BudgetIndex([
        dict(code='0020', year=2026, title='משרד החינוך', depth=1, hierarchy='[]',
             history='{"2019": ["0020:משרד החינוך"]}'),
        dict(code='002038', year=2016, title='העברות למוסדות תורניים', depth=2,
             hierarchy='[["0020", "משרד החינוך"]]',
             history='{"2014": ["002038:העברות למוסדות תורניים"], "2012": ["002240:תמיכה בישיבות"]}'),
    ])


def test_budget_connected_items():
    idx = budget()
    item, year = idx.by_code('002240', [2012])
    assert RB.BudgetIndex.doc_id(item) == 'budget/002038/2016' and year == 2012
    assert idx.by_code('002240', [2016]) == (None, None)


def test_budget_history_tie_break():
    """A code in the history of both an item and its parent goes to the item of its own depth, then the latest -
    whichever row loads first."""
    rows = [
        dict(code='0020', year=2026, title='משרד החינוך', depth=1, hierarchy='[]',
             history='{"2015": ["0020:משרד החינוך", "002041:חינוך יסודי"]}'),
        dict(code='002041', year=2018, title='חינוך יסודי', depth=2, hierarchy='[["0020", "משרד החינוך"]]',
             history='{"2015": ["002041:חינוך יסודי"]}'),
        dict(code='002041', year=2023, title='חינוך יסודי', depth=2, hierarchy='[["0020", "משרד החינוך"]]',
             history='{"2015": ["002041:חינוך יסודי"]}'),
        dict(code='002043', year=2023, title='חינוך מיוחד', depth=2, hierarchy='[["0020", "משרד החינוך"]]',
             history='{"2023": ["002041:חינוך יסודי"]}'),
    ]
    for ordered in (rows, rows[::-1]):
        idx = RB.BudgetIndex(ordered)
        assert RB.BudgetIndex.doc_id(idx.by_code('002041', [2015])[0]) == 'budget/002041/2023'
        assert RB.BudgetIndex.doc_id(idx.by_code('002041', [2023])[0]) == 'budget/002041/2023'  # its own
        assert RB.BudgetIndex.doc_id(idx.by_code('0020', [2015])[0]) == 'budget/0020/2026'


@pytest.mark.parametrize('date,start,end,years', [
    (D(2023, 5, 1), None, None, [2022, 2023]),
    (D(2023, 5, 1), 2018, 2021, [2018, 2019, 2020, 2021]),
    (D(2023, 5, 1), 2021, None, [2021]),
    (D(1998, 5, 1), 1990, 1997, [1997]),
])
def test_source_years(date, start, end, years):
    assert RB.source_years(date, start, end) == years


# --- quotes ------------------------------------------------------------------------------------------------------

def test_find_quote():
    text = L._norm_ws('משרד  הבריאות לא פעל ״כנדרש״ בעניין בתי החולים')
    assert L.find_quote(text, 'משרד הבריאות לא פעל') == 0
    assert L.find_quote(text, 'לא פעל "כנדרש" בעניין') > 0
    assert L.find_quote(text, 'משרד החינוך לא פעל') == -1


# --- the linker, with canned LLM answers -------------------------------------------------------------------------

ENTITIES = [
    dict(id='500100029', name='הכנסת', kind='government_office', kind_he='משרד ממשלתי'),
    dict(id='500100904', name='משרד הבריאות/המשרד הראשי', kind='government_office', kind_he='משרד ממשלתי'),
    dict(id='500106919', name='משרד הבריאות/בי"ח איכילוב', kind='government_office', kind_he='משרד ממשלתי'),
    dict(id='520000472', name='חברת החשמל לישראל בע"מ', kind='company', kind_he='חברה ציבורית'),
    dict(id='500500285', name='רשות הטבע והגנים', kind='law_mandated_organization', kind_he='תאגיד סטטוטורי'),
    dict(id='580000001', name='עמותת לתת', kind='association', kind_he='עמותה'),
    dict(id='580000002', name='עמותת לתת', kind='association', kind_he='עמותה'),
]
BODIES = [dict(name='משרד הבריאות', aliases=['משרד הבריאות והרווחה'], entity_id='500100904',
               comptroller_codes=['1137'], budget_chapters=['0024', '0067'], status='ok'),
          dict(name='הכנסת', aliases=[], entity_id='500100029', comptroller_codes=[], budget_chapters=['0002'],
               ubiquitous='yes', status='ok')]


def targets():
    return LK.Targets(entities=E.EntityIndex(ENTITIES, BODIES), decisions=decisions(), budget=budget())


def test_entity_index():
    idx = targets().entities
    assert idx.body_for_code('1137')['entity']['id'] == '500100904'
    assert [b['name'] for b in idx.bodies_for_name('משרד הבריאות והרווחה')] == ['משרד הבריאות']
    assert [e['id'] for e in idx.exact('חברת החשמל לישראל')] == ['520000472']


def test_linker_deterministic():
    doc = dict(
        doc_id='gov_decisions/x', kind='gov_decisions', title='החלטה', date=D(2026, 1, 1),
        text="בהמשך להחלטת הממשלה מס' 3200 מיום 4.7.2025, לרשות הטבע והגנים ח.פ 500500285 יועבר תקציב.",
        hints=dict(llm=False, same_government_default=True),
    )
    links, mentions = LK.DocLinker(doc, targets(), None).run()
    by_target = {l['target_doc_id']: l for l in links}
    assert set(by_target) == {'gov_decisions/a', 'org/law_mandated_organization/500500285'}
    assert by_target['gov_decisions/a']['relation'] == 'cites'
    assert by_target['gov_decisions/a']['method'] == 'regex:decision:date'
    assert '3200' in by_target['gov_decisions/a']['excerpt']


def test_linker_comptroller_bodies_and_llm(monkeypatch):
    def extract(what, source, parts_hint, usage=None):
        orgs = [
            dict(name='משרד הבריאות', org_type='ministry', parent=None, role='responsible',
                 quote='משרד הבריאות לא פעל', parts=['7-1']),
            dict(name='חברת החשמל לישראל', org_type='government_company', parent=None, role='mentioned',
                 quote='חברת החשמל לישראל סיפקה', parts=[]),
            dict(name='עמותת לתת', org_type='ngo', parent=None, role='funded', quote='עמותת לתת קיבלה', parts=[]),
            dict(name='בית החולים איכילוב', org_type='hospital', parent='משרד הבריאות', role='mentioned',
                 quote='לא מופיע בטקסט', parts=[]),
        ]
        for o in orgs:
            o['position'] = L.find_quote(L._norm_ws(source), o['quote'])
            o['verified'] = o['position'] >= 0
        return orgs, []

    asked = []

    def decide(what, title, date, guidance, items, usage=None):
        asked.extend(items)
        # The two namesake associations: the model can't tell, and says so.
        return {n: (item['candidates'][0][0], 'unsure') for n, item in enumerate(items)}

    monkeypatch.setattr(L, 'extract', extract)
    monkeypatch.setattr(L, 'decide', decide)
    doc = next(S.comptroller_documents(
        [dict(chapter_id=7, title='ניהול בתי החולים', publish_date='2024-05-01', text_md='טקסט מלא',
              audited_bodies=[{'code': '1137', 'name': 'משרד הבריאות', 'main': True}], summary='סיכום',
              audit_period_start_year=2021, audit_period_end_year=2023, language='he')],
        [dict(finding_id='7-1', chapter_id=7, seq=1, title='ליקוי', description='משרד הבריאות לא פעל כנדרש. '
              'חברת החשמל לישראל סיפקה חשמל. עמותת לתת קיבלה מענק.', responsible_bodies=['משרד הבריאות'])]))
    links, mentions = LK.DocLinker(doc, targets(), None).run()
    got = {(l['source_part_id'], l['target_doc_id'], l['relation'], l['method']) for l in links}
    assert got == {
        (None, 'org/government_office/500100904', 'audited', 'comptroller_code'),
        ('7-1', 'org/government_office/500100904', 'responsible', 'alias'),
        (None, 'org/company/520000472', 'mentioned', 'exact_name'),
    }
    assert [i['mention'] for i in asked] == ['עמותת לתת']
    statuses = {(m['mention'], m['status']) for m in mentions}
    assert ('עמותת לתת', 'ambiguous') in statuses
    assert ('בית החולים איכילוב', 'unverified') in statuses


def test_comptroller_documents_skip_translations():
    audits = [dict(chapter_id=1, title='Audit', language='en', analysis_status='not-hebrew'),
              dict(chapter_id=2, title='ביקורת', language='he', publish_date='2020-01-01'),
              dict(chapter_id=3, title='ביקורת', language='', analysis_status='no-file'),
              dict(chapter_id=4, title='ביקורת', language='he', analysis_status='partial-text', text_md='טקסט')]
    assert [d['doc_id'] for d in S.comptroller_documents(audits, [])] == [
        'state_comptroller_audits/2', 'state_comptroller_audits/4']


def test_decision_documents():
    rows = [dict(id='u1', title='היתר', text='<p>מחליטים: <strong>להתיר</strong></p>', policy_type='החלטות ממשלה',
                 doc_published_date='2025-07-04T00:00:00', procedure_number_str='3200'),
            dict(id='u2', title='עסקה', text='<p>' + 'א' * 300 + '</p>', policy_type='החלטות ועדת פטור',
                 doc_published_date='2025-07-04T00:00:00')]
    docs = list(S.decision_documents(rows))
    assert docs[0]['text'] == 'היתר\nמחליטים: להתיר' and docs[0]['hints']['same_government_default']
    assert docs[0]['hints']['llm'] is False  # too short to bother
    assert docs[1]['hints']['llm'] is False and not docs[1]['hints']['same_government_default']


def test_ubiquitous_bodies_only_in_specific_roles(monkeypatch):
    def extract(what, source, parts_hint, usage=None):
        orgs = [dict(name='הכנסת', org_type='statutory_body', parent=None, role='mentioned',
                     quote='הצעת החוק תוגש לכנסת', parts=[]),
                dict(name='משרד הבריאות', org_type='ministry', parent=None, role='responsible',
                     quote='משרד הבריאות יפעל', parts=[])]
        for o in orgs:
            o['position'] = L.find_quote(L._norm_ws(source), o['quote'])
            o['verified'] = o['position'] >= 0
        return orgs, []

    monkeypatch.setattr(L, 'extract', extract)
    doc = dict(doc_id='gov_decisions/y', kind='gov_decisions', title='הצעת חוק', date=D(2024, 1, 1),
               text='הצעת החוק תוגש לכנסת. משרד הבריאות יפעל ליישומה.', hints=dict(llm=True))
    links, mentions = LK.DocLinker(doc, targets(), None).run()
    assert [l['target_doc_id'] for l in links] == ['org/government_office/500100904']
    assert ('הכנסת', 'ubiquitous') in {(m['mention'], m['status']) for m in mentions}


# --- earlier comptroller reports -----------------------------------------------------------------------------------

def chapters():
    pubs = [dict(pub_id=1, title='דוח שנתי 67ב', publish_date='2017-05-01', report_code='67ב'),
            dict(pub_id=2, title='דוחות על הביקורת בשלטון המקומי לשנת 2014', publish_date='2014-07-01',
                 report_code=''),
            dict(pub_id=3, title='דוח שנתי 72א', publish_date='2021-10-01', report_code='72א')]
    chs = [dict(chapter_id=10, pub_id=1, title='זיהומי מים בין מדינת ישראל לשטחי יהודה, שומרון ורצועת עזה - המשרד '
                'להגנת הסביבה - מטלה בין-משרדית',
                publish_date='2017-05-01', report_code='67ב'),
           dict(chapter_id=11, pub_id=1, title='ילדים ובני נוער בסיכון בפנימיות של משרד הרווחה',
                publish_date='2017-05-01', report_code='67ב'),
           dict(chapter_id=12, pub_id=1, title='ילדים ובני נוער בסיכון במשרד החינוך', publish_date='2017-05-01',
                report_code='67ב'),
           dict(chapter_id=20, pub_id=2, title='מתקני ספורט ציבוריים ברשויות המקומיות', publish_date='2014-07-01'),
           dict(chapter_id=30, pub_id=3, title='אי מיצוי של זכויות חברתיות: ביקורת מעקב',
                publish_date='2021-10-01', report_code='72א')]
    return RC.ChapterIndex(chs, pubs)


@pytest.mark.parametrize('text,code', [('דוח שנתי 67ב', '67ב'), ("דוח שנתי 72 א'", '72א'), ('דוח שנתי 49', None),
                                       ('דוחות על הביקורת בשלטון המקומי לשנת 2014', None)])
def test_report_code(text, code):
    assert RC.report_code(text) == code


def test_chapter_resolution():
    idx = chapters()
    ref = dict(report='דוח שנתי 67ב', year=2017, title='זיהומי מים בין ישראל לשטחי יהודה, שומרון ורצועת עזה')
    entry, confidence, how = idx.resolve(ref, D(2024, 3, 19))
    assert entry['id'] == '10' and how == 'title'
    # Reports without a code: by the publication's title and year.
    ref = dict(report='דוחות על הביקורת בשלטון המקומי לשנת 2014', year=2014,
               title='מתקני ספורט ציבוריים ברשויות המקומיות')
    assert idx.resolve(ref, D(2023, 5, 2))[0]['id'] == '20'
    # Two similar chapters: the LLM chooses.
    ref = dict(report='דוח שנתי 67ב', year=2017, title='ילדים ובני נוער בסיכון')
    entry, candidates, how = idx.resolve(ref, D(2023, 5, 2))
    assert entry is None and how == 'llm' and {c['id'] for c, _ in candidates} == {'11', '12'}
    # Unknown report, or one published after the citing chapter.
    assert idx.resolve(dict(report='דוח שנתי 53ב', year=2003, title='השירות לציבור'))[2] == 'no_report'
    assert idx.resolve(dict(report='דוח שנתי 72א', year=2021, title='אי מיצוי של זכויות חברתיות'),
                       D(2020, 1, 1))[2] == 'no_report'


def test_chapter_resolution_without_a_report_code():
    pubs = [dict(pub_id=1, title='דוח שנתי 42 לשנת 1991 ולחשבונות שנת הכספים 1990', publish_date='1992-03-25'),
            dict(pub_id=2, title='הקמת מערך המצלמות האלקטרוניות ואכיפת חוקי תעבורה', publish_date='2016-02-24'),
            dict(pub_id=3, title='דוח שנתי 66ב', publish_date='2016-05-01', report_code='66ב')]
    chs = [dict(chapter_id=1, pub_id=1, title='הקצאת קרקעות לחברות ממשלתיות לצורך תכנון ופיתוח; הקצאת קרקע לבנייה '
                'עצמית; שינוי ייעודן של קרקעות חקלאיות ופדיון זכויות בהן', publish_date='1992-03-25'),
           dict(chapter_id=2, pub_id=1, title='חברות משכנות וסדרי הפעלתן', publish_date='1992-03-25'),
           dict(chapter_id=3, pub_id=2, title='הקמת מערך המצלמות האלקטרוניות ואכיפת חוקי תעבורה',
                publish_date='2016-02-24'),
           dict(chapter_id=4, pub_id=3, title='אכיפת חוקי התעבורה בכבישים הבין-עירוניים', publish_date='2016-05-01',
                report_code='66ב')]
    idx = RC.ChapterIndex(chs, pubs)
    # An old annual report by its bare number; the cited topic is one of a chapter's topics.
    entry, confidence, how = idx.resolve(dict(report='דוח מבקר המדינה 42', year=1992,
                                              title='שינוי יעודן של קרקעות חקלאיות ופדיון זכויות בהן'), D(2010, 1, 1))
    assert entry['id'] == '1' and how == 'title'
    # A report named only generically: a clear title match among the year's chapters.
    ref = dict(report='דוח מבקר המדינה', year=2016, title='הקמת מערך המצלמות האלקטרוניות ואכיפת חוקי תעבורה')
    entry, confidence, how = idx.resolve(ref, D(2017, 5, 16))
    assert entry['id'] == '3' and how == 'title_in_year'
    # A report named loosely matches a publication's title, but its chapter is in another one that year.
    ref = dict(report='הקמת מערך', year=2016, title='אכיפת חוקי התעבורה בכבישים הבין-עירוניים')
    entry, confidence, how = idx.resolve(ref, D(2017, 5, 16))
    assert entry['id'] == '4' and how == 'title_in_year'
    # ... but not a vague one, nor one published after the citing chapter.
    assert idx.resolve(dict(report='דוח מבקר המדינה', year=2016, title='אכיפת חוקי תעבורה'),
                       D(2017, 5, 16))[2] == 'no_report'
    assert idx.resolve(ref, D(2015, 1, 1))[2] == 'no_report'


def test_linker_earlier_reports():
    audits = [dict(chapter_id=7824, title='זיהום אוויר', publish_date='2024-03-19', language='he',
                   earlier_reports=[
                       dict(report='דוח שנתי 67ב', year=2017, relation='followed_up',
                            title='זיהומי מים בין מדינת ישראל לשטחי יהודה, שומרון ורצועת עזה'),
                       dict(report='דוח שנתי 53ב', year=2003, title='השירות לציבור', relation='referenced')])]
    doc = next(S.comptroller_documents(audits, []))
    doc['hints']['llm'] = False
    t = targets()
    t.chapters = chapters()
    links, mentions = LK.DocLinker(doc, t, None).run()
    assert [(l['target_doc_id'], l['relation'], l['method']) for l in links] == [
        ('state_comptroller_audits/10', 'follows_up', 'comptroller_ref:title')]
    assert any(m['status'] == 'no_report' and m['details']['year'] == 2003 for m in mentions)


def test_earlier_reports_link_only_chapters_with_a_page(monkeypatch):
    """A cited chapter that isn't an indexed audit (before 2000, a translation) is a mention, not a link - also when
    the LLM chooses it over a similar chapter that has a page."""
    asked = []

    def decide(what, title, date, guidance, items, usage):
        asked.append(items)
        return {n: ('state_comptroller_audits/12', 'certain') for n in range(len(items))}

    monkeypatch.setattr(L, 'decide', decide)
    audits = [dict(chapter_id=7824, title='זיהום אוויר', publish_date='2024-03-19', language='he', earlier_reports=[
        dict(report='דוח שנתי 67ב', year=2017, title='זיהומי מים בין מדינת ישראל לשטחי יהודה, שומרון ורצועת עזה'),
        dict(report='דוח שנתי 67ב', year=2017, title='ילדים ובני נוער בסיכון')])]
    doc = next(S.comptroller_documents(audits, []))
    doc['hints']['llm'] = False
    t = targets()
    t.chapters = chapters()
    t.chapters.indexed = {'state_comptroller_audits/11', 'state_comptroller_audits/30'}
    links, mentions = LK.DocLinker(doc, t, None).run()
    assert links == []
    assert {(m['target_doc_id'], m['status'], m['method']) for m in mentions} == {
        ('state_comptroller_audits/10', 'not_indexed', 'comptroller_ref:title'),
        ('state_comptroller_audits/12', 'not_indexed', 'llm_resolve')}
    # The LLM saw both similar chapters; with a page, its choice is linked.
    assert {c for c, _ in asked[0][0]['candidates']} == {'state_comptroller_audits/11', 'state_comptroller_audits/12'}
    t.chapters.indexed.add('state_comptroller_audits/12')
    links, _ = LK.DocLinker(doc, t, None).run()
    assert [(l['target_doc_id'], l['method']) for l in links] == [('state_comptroller_audits/12', 'llm_resolve')]
    # No candidate with a page: not asked at all.
    t.chapters.indexed = {'state_comptroller_audits/30'}
    asked.clear()
    _, mentions = LK.DocLinker(doc, t, None).run()
    assert not asked and {m['status'] for m in mentions} == {'not_indexed'}


def test_earlier_reports_from_the_older_schema():
    a = dict(follows_up_report='דוח שנתי 67ב', follows_up_year='2017', follows_up_title='זיהומי מים')
    assert S.earlier_reports(a) == [dict(report='דוח שנתי 67ב', year=2017, title='זיהומי מים',
                                         relation='followed_up')]


def test_issue_links():
    from datapackage_pipelines_budgetkey.pipelines.related_items import merge_sources as MS
    base = dict(source_kind='state_comptroller_audits', source_title='פרק', mention='m', excerpt='e')
    rows = [dict(base, link_id='1', source_doc_id='state_comptroller_audits/7', source_part_id='7-1',
                 target_doc_id='org/x/1', relation='responsible', confidence=0.85, method='llm_resolve'),
            dict(base, link_id='2', source_doc_id='state_comptroller_audits/9', source_part_id='9-4',
                 target_doc_id='org/x/1', relation='subject', confidence=0.93, method='alias'),
            dict(base, link_id='3', source_doc_id='state_comptroller_audits/9', source_part_id=None,
                 target_doc_id='org/x/2', relation='audited', confidence=0.95, method='comptroller_code')]
    out = list(MS.issue_links(rows, {'7-1': '7-1', '9-4': '7-1'}, {'7-1': 'ליקוי מתמשך'}))
    derived = out[len(rows):]
    assert len(derived) == 1
    d = derived[0]
    assert (d['source_doc_id'], d['source_kind'], d['source_title']) == \
        ('state_comptroller_issues/7-1', 'state_comptroller_issues', 'ליקוי מתמשך')
    # The strongest finding link wins, and stays traceable to its finding.
    assert (d['relation'], d['confidence'], d['source_part_id'], d['method']) == ('subject', 0.93, '9-4',
                                                                                  'derived:issue')


def test_load_issues_skips_single_member_issues(tmp_path):
    from datapackage_pipelines_budgetkey.pipelines.related_items import merge_sources as MS
    (tmp_path / 'issue_members.csv').write_text(
        'issue_id,member_type,member_id,date\n'
        '7-1,finding,7-1,\n7-1,finding,9-4,\n'      # two audits: an issue page
        '8-2,finding,8-2,\n8-2,response,r/1,\n'     # a finding and a response: an issue page
        '8-3,finding,8-3,\n', encoding='utf8')       # a single finding: no page
    (tmp_path / 'issues.csv').write_text('issue_id,title\n7-1,a\n8-2,b\n8-3,c\n', encoding='utf8')
    members, titles = MS.load_issues(str(tmp_path))
    assert members == {'7-1': '7-1', '9-4': '7-1', '8-2': '8-2'}


# --- tenders -------------------------------------------------------------------------------------------------------

def test_tender_resolution_needs_the_publisher():
    from datapackage_pipelines_budgetkey.pipelines.related_items import ri_procurement as RP
    idx = RP.TenderIndex([
        dict(tender_type='office', publication_id=1, tender_id='205/2014', publisher='משרד הרווחה',
             description='שירותי רווחה', publication_date='2014-03-01'),
        dict(tender_type='office', publication_id=2, tender_id='205-2014', publisher='משרד החינוך',
             description='הסעות', publication_date='2014-05-01'),
    ])
    bodies = [dict(name='משרד הרווחה והביטחון החברתי', aliases=['משרד הרווחה'], entity_id='500101662',
                   comptroller_codes=[], budget_chapters=['0023'], status='ok'),
              dict(name='משרד החינוך', aliases=[], entity_id='', comptroller_codes=[], budget_chapters=['0020'],
                   status='ok')]
    ents = E.EntityIndex([], bodies)
    welfare = ents.bodies_for_name('משרד הרווחה')
    # The number alone is two tenders; the document's body tells them apart.
    assert idx.resolve('205/2014', ents, [], 'מכרז פומבי מס\' 205/2014')[1] == 'no_publisher'
    t, how = idx.resolve('205/2014', ents, welfare, 'מכרז פומבי מס\' 205/2014')
    assert RP.TenderIndex.doc_id(t) == 'tenders/office/1/205/2014'
    # Or the publisher named next to the number.
    t, how = idx.resolve('205/2014', ents, [], 'משרד החינוך פרסם את מכרז 205-2014 להסעות')
    assert t['publication_id'] == '2'
    # Not a tender published after the source.
    assert idx.resolve('205/2014', ents, welfare, '', datetime.date(2013, 1, 1))[0] is None


# --- shared indexes under the linker's threads ---------------------------------------------------------------------

@pytest.mark.parametrize('make', ['budget', 'supports'])
def test_embeddings_are_built_once_across_threads(monkeypatch, make):
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor
    import numpy as np

    calls = []

    def embed_many(texts):
        calls.append(len(texts))
        time.sleep(0.05)  # long enough for the other threads to arrive while the first one builds
        return [[1.0, float(i)] for i in range(len(texts))]

    monkeypatch.setattr(RB, 'embed_many', embed_many)
    if make == 'budget':
        idx = budget()
        ask = lambda: idx.candidates(np.array([1.0, 0.0], dtype=np.float32), ['0020'], [2014])  # noqa: E731
    else:
        idx = RB.SupportProgramIndex([dict(program_key='p1', title='סל הספורט', supporting_ministry='משרד התרבות',
                                           min_year='2015', max_year='2020', budget_codes='[]', all_titles='[]')])
        ask = lambda: idx.candidates(np.array([1.0, 0.0], dtype=np.float32), [], [2016])  # noqa: E731
    barrier = threading.Barrier(8)

    def go(_):
        barrier.wait()
        return ask()

    with ThreadPoolExecutor(8) as pool:
        results = list(pool.map(go, range(8)))
    assert len(calls) == 1
    assert all(r == results[0] for r in results) and results[0]
