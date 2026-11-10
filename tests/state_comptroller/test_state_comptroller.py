"""Offline tests for the State Comptroller pipelines: parsing, text clean-up, stitching and issue building."""
import datetime
import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / 'datapackage_pipelines_budgetkey/pipelines/state_comptroller'))

import sc_common as sc  # noqa: E402
import chapter_text as ct  # noqa: E402
import mevaker_scraper as ms  # noqa: E402
import pm_responses as pm  # noqa: E402
import chapter_analysis as ca  # noqa: E402
import issue_linking as il  # noqa: E402


# --- sc_common -------------------------------------------------------------------------------------------------

@pytest.mark.parametrize('text,code', [
    ('דוח שנתי 65ג לשנת 2014 ולחשבונות שנת הכספים 2013', '65ג'),
    ('הערות ראש הממשלה לדוח 52 א חלק ב', '52א'),
    ('הערות ראש הממשלה לדו"ח מבקר המדינה 72א\'- חלק ב', '72א'),
    ('הערות ראש הממשלה לדו"ח מבקר המדינה 75 סייבר', None),
    ('דוח שנתי של מבקר המדינה - נובמבר 2022', None),
])
def test_report_code(text, code):
    assert sc.report_code(text) == code


def test_report_codes_expands_volumes():
    assert sc.report_codes("מעקב אחר תיקון ליקויים מדוחות 64 א'-ב'-ג' + 65 א'") == ['64א', '64ב', '64ג', '65א']
    assert sc.report_codes('דוח מבקר המדינה 67ב,68א,68ב') == ['67ב', '68א', '68ב']


def test_fuzzy_contains_tolerates_extraction_noise():
    source = sc.normalize_for_match('נמצא כי אין במשרד הפנים - במטה ובמחוזות מערכת ממוחשבת לקליטת הבקשות להיתרי אשראי '
                                    'המאפשרת מעקב אחר הבקשות וסטטוס טיפולן. פועל יוצא הוא שמשרד הפנים לא יכול לאתר.')
    assert sc.fuzzy_contains(source, 'נמצא כי אין במשרד הפנים, במטה ובמחוזות, מערכת ממוחשבת לקליטת הבקשות')
    assert not sc.fuzzy_contains(source, 'משרד הבריאות לא גיבש תוכנית לאומית לטיפול במחלות כרוניות בקהילה')
    assert not sc.fuzzy_contains(source, '')


def test_parse_dotted_date():
    assert sc.parse_dotted_date('תאריך הפרסום: 24.06.2026') == datetime.date(2026, 6, 24)
    assert sc.parse_dotted_date('-') is None


# --- mevaker_scraper --------------------------------------------------------------------------------------------

def test_parse_search_response():
    data = [{'ErrorInfo': None}, 1, {'IsNull': False},
            {'q': {'ResultTables': [{'TotalRows': 2, 'ResultRows': [{'ListItemID': '1'}, {'ListItemID': '2'}]}]}}]
    total, rows = ms.parse_search_response(data)
    assert total == 2 and [r['ListItemID'] for r in rows] == ['1', '2']
    with pytest.raises(RuntimeError):
        ms.parse_search_response([{'ErrorInfo': {'ErrorMessage': 'no'}}])


def test_parse_ms_date_is_in_israel_time():
    # 2026-06-29T21:00:00Z is midnight of June 30 in Israel
    assert ms.parse_ms_date('/Date(1782766800000)/') == datetime.date(2026, 6, 30)
    assert ms.parse_ms_date(None) is None


CHAPTER_PAGE = '''<html><body><script>var x = {"pageItemId":10596,"a":1};</script>
<h1>העברות תקציביות לקראת סוף שנת 2024</h1>
<div class="links-to-files"><div class="file-link"><span class="title-file">הדוח המלא</span>
<div class="file-link-list"><a href="https://library.mevaker.gov.il/sites/DigitalLibrary/Documents/2026/Economic-Report/2026-Economic-Report-103.docx">x</a>
<a href="https://library.mevaker.gov.il/sites/DigitalLibrary/Documents/2026/Economic-Report/2026-Economic-Report-103.pdf">x</a></div></div>
<div class="file-link"><span class="title-file">תקציר</span>
<a href="/sites/DigitalLibrary/Documents/2026/Economic-Report/2026-Economic-Report-103-Taktzir.pdf">x</a></div>
<div class="file-link"><span class="title-file"></span></div></div></body></html>'''

PUBLICATION_PAGE = '''<html><body><h1>דוח בתחום הכלכלי - דוח מיוחד</h1>
<span id="ctl00_PlaceHolderMain_pubDate">24.06.2026</span>
<span id="ctl00_PlaceHolderMain_lblPublicationNum">מספר קטלוגי:</span><span class="text-field"> 2026-S-0018</span>
<span id="ctl00_PlaceHolderMain_lblPublicationISSNnum">מספר ISSN:</span><span class="text-field"> -</span>
<div class="introduction-content">הדוח המונח לפניכם</div>
<div class="links-to-files"><div class="file-link"><span class="title-file">שער ותוכן עניינים</span>
<a href="/sites/DigitalLibrary/Documents/2026/Economic-Report/2026-Economic-Report-HE.pdf">x</a></div></div></body></html>'''


def test_parse_chapter_page():
    page = ms.parse_chapter_page(CHAPTER_PAGE)
    assert page['page_item_id'] == 10596
    assert page['title'] == 'העברות תקציביות לקראת סוף שנת 2024'
    assert [(f['label'], f['ext']) for f in page['files']] == [('הדוח המלא', 'docx'), ('הדוח המלא', 'pdf'), ('תקציר', 'pdf')]
    assert page['files'][2]['url'].startswith('https://library.mevaker.gov.il/sites/DigitalLibrary/Documents/')
    assert ms.pick(page['files'], 'תקציר', 'pdf')['url'].endswith('-Taktzir.pdf')
    assert ms.pick(page['files'], 'תקציר', 'docx') is None


def _files(*spec):
    return [dict(label=label, url='https://x/Documents/' + path, ext=path.rsplit('.', 1)[1], stored_url='s/' + path)
            for label, path in spec]


def test_chapter_files_prefer_hebrew_over_the_english_abstract():
    # chapter 10383: the whole Hebrew chapter is labelled 'תקציר הדוח'; the English abstract was picked before
    row = ms.chapter_file_fields(_files(('תקציר הדוח', '2026/I/2026-I-101.docx'), ('תקציר הדוח', '2026/I/2026-I-101.pdf'),
                                       ('Abstract', '2026/I/EN/2026-I-101-EN.docx'),
                                       ('Abstract', '2026/I/EN/2026-I-101-EN.pdf')))
    assert row['full_docx_url'].endswith('/2026-I-101.docx') and row['full_pdf_url'].endswith('/2026-I-101.pdf')
    assert row['full_docx_stored'] == 's/2026/I/2026-I-101.docx'
    # chapter 8187: only a Hebrew abstract besides the English one
    row = ms.chapter_file_fields(_files(('תקציר', '2025/c-Taktzir.pdf'), ('Abstract', '2025/EN/c-EN.pdf')))
    assert row['full_pdf_url'].endswith('/c-Taktzir.pdf') and row['abstract_pdf_url'].endswith('/c-Taktzir.pdf')
    # the labelled full report wins; an English-only chapter still gets its file
    row = ms.chapter_file_fields(_files(('Abstract', 'EN/a-EN.pdf'), ('הדוח המלא', 'a.pdf')))
    assert row['full_pdf_url'].endswith('/a.pdf')
    assert ms.chapter_file_fields(_files(('Report', 'EN/a-EN.pdf')))['full_pdf_url'].endswith('/a-EN.pdf')
    assert ms.chapter_file_fields(_files(('קובץ', 'old.pdf')))['full_docx_url'] is None


def test_parse_publication_page():
    page = ms.parse_publication_page(PUBLICATION_PAGE)
    assert page['publish_date'] == datetime.date(2026, 6, 24)
    assert page['catalog_number'] == '2026-S-0018'
    assert page['issn'] is None
    assert page['foreword'] == 'הדוח המונח לפניכם'
    assert len(page['files']) == 1


def test_parse_institution_codes():
    assert ms.parse_institution_codes(';29.7#;06116#;1255#') == ['29.7', '06116', '1255']
    assert ms.parse_institution_codes(None) == []


def test_file_object_name_keeps_the_original_file_name():
    url = 'https://library.mevaker.gov.il/sites/DigitalLibrary/Documents/65c/2015-65c-101-Zehuyot.pdf'
    assert ms.file_object_name(297, url) == 'state-comptroller/reports/297/2015-65c-101-Zehuyot.pdf'


# --- chapter_text -----------------------------------------------------------------------------------------------

def test_tidy_markdown_headings_are_cosmetic():
    md = ct.tidy_markdown('***מבוא***\n\n**פרק 1 - רקע**\n\nטקסט **מודגש** בתוך שורה.\n\n12\n\n![](media/image1.png)\n')
    assert md.split('\n\n') == ['## מבוא', '### פרק 1 - רקע', 'טקסט **מודגש** בתוך שורה.\n']


def test_broken_pages():
    good = 'משרד הפנים לא הוציא בשנים האחרונות הנחיות רוחביות לרשויות המקומיות בדבר אפשרויות לחיסכון. ' * 10
    legacy = '‰Ò Î‰ ˙ÁË·‰ ˙·ˆ˜ ÈÂˆÈÓÏ ÌÈÓÒÁ‰ ˙‡ ˙Â‰ÊÏ Ï"Ë· ÏÚ ' * 10
    no_finals = 'גיוס עובדי ואיוש משרות בשירות המדינה נעשה על ידי משרדי הממשלה ועל ידי הנציבות המופקדת כגו על ביצוע ' * 10
    assert not ct.broken_page(good)
    assert ct.broken_page(legacy)
    assert ct.broken_page(no_finals.replace('ם', '').replace('ן', '').replace('ך', ''))
    # Fonts that come out as symbols of other scripts (from the 56א and 71ג response books)
    latin1 = '§£ª££ª¬ ©¥´ ©³² ±¦¡ §£²³ ´¬ ´¢¦¡ ¬°£ ²¡ ±¬¨ ª£¨ ´²±£ £ª££ª¬¦ ' * 10
    ipa = 'ʬʥʧʡ ʤʷʥʶʮʡ ʭʩʬʠʸʹʩʡ ʵʥʧʤ ʣʸʹʮ ʬʥʴʩʨ ʩʸʬʥʱʰʥʷʤ ʺʥʸʩʹʤ ' * 10
    assert ct.broken_page(latin1)
    assert ct.broken_page(ipa)
    # Arabic (the reports' Arabic abstracts) and English pages are fine
    arabic = 'يعرض هذا التقرير نتائج الرقابة على أنشطة الوزارات الحكومية في مجال التخطيط ' * 10
    english = 'The State Comptroller audited the ministry of finance and found deficiencies. ' * 10
    assert not ct.broken_page(arabic)
    assert not ct.broken_page(english)


def test_pdf_pages_to_markdown_joins_lines_and_drops_running_headers():
    words = ['אלף', 'בית', 'גימל', 'דלת', 'הא']

    def line(n, i):
        return ' '.join([words[n]] * 3 + ['שורה ארוכה של טקסט בעברית שממשיכה את הפסקה', 'מספר', words[i]])
    pages = ['דוח שנתי 56ב\n%d\n%s\n%s\nסוף פסקה %s.\n\n%s\n%s' % (n, line(n, 0), line(n, 1), words[n], line(n, 2),
                                                                line(n, 3)) for n in range(5)]
    md = ct.pdf_pages_to_markdown(pages)
    assert 'דוח שנתי 56ב' not in md
    paragraphs = md.strip().split('\n\n')
    assert paragraphs[0] == ' '.join([line(0, 0), line(0, 1), 'סוף פסקה אלף.'])
    # The last paragraph of a page runs on into the first one of the next page
    assert paragraphs[1] == ' '.join([line(0, 2), line(0, 3), line(1, 0), line(1, 1), 'סוף פסקה בית.'])


def test_fix_punctuation():
    assert ct.fix_punctuation('במדינת ישראל ,ובכלל זה השכבות הללו .על יסוד') == 'במדינת ישראל, ובכלל זה השכבות הללו. על יסוד'
    assert ct.fix_punctuation('3.5 מיליון') == '3.5 מיליון'


@pytest.mark.skipif(not shutil.which('pandoc'), reason='pandoc not installed')
def test_docx_to_markdown(tmp_path):
    import subprocess
    src = tmp_path / 'a.md'
    src.write_text('**כותרת**\n\nפסקה ראשונה.\n\n| א | ב |\n|---|---|\n| 1 | 2 |\n', encoding='utf-8')
    docx = tmp_path / 'a.docx'
    subprocess.run(['pandoc', str(src), '-o', str(docx)], check=True)
    md = ct.docx_to_markdown(str(docx))
    assert '### כותרת' in md and 'פסקה ראשונה.' in md and '| 1 ' in md


# --- chapter_analysis -------------------------------------------------------------------------------------------

def test_windows_cover_long_texts_with_overlap():
    md = ('פסקה ' * 2000 + '\n\n') * 40
    spans = ca.windows(md)
    assert spans[0][0] == 0 and spans[-1][1] == len(md)
    for (a, b), (c, d) in zip(spans, spans[1:]):
        assert c < b  # overlapping
    assert ca.windows('קצר') == [(0, 3)]


def test_same_finding():
    a = dict(title='היעדר מערכת ממוחשבת לקליטת בקשות להיתרי מלוות', description='אין במשרד הפנים מערכת ממוחשבת')
    b = dict(title='היעדר מערכת ממוחשבת לקליטת הבקשות להיתרי מלוות', description='תיאור אחר לגמרי')
    c = dict(title='מרווח עלויות עודף בהנפקת אג"ח', description='החברות הממשלתיות משלמות מרווח')
    assert ca.same_finding(a, b)
    assert not ca.same_finding(a, c)


# --- pm_responses -----------------------------------------------------------------------------------------------

def test_parse_content_files_and_kind():
    content = {'contentSub': {'filesToDownload': {'filesGroupItems': [{'items': [
        {'fileName': 'files_73a_221122.pdf', 'extension': 'pdf', 'displayName': 'הערות ראש הממשלה 73 א',
         'url': 'https://www.gov.il/BlobFolder/reports/73a_221122/he/files_73a_221122.pdf'}]}]}}}
    files = pm.parse_content_files(content)
    assert files == [dict(url='https://www.gov.il/BlobFolder/reports/73a_221122/he/files_73a_221122.pdf',
                          file_name='files_73a_221122.pdf', display_name='הערות ראש הממשלה 73 א', ext='pdf')]
    assert pm.parse_content_files({'contentSub': None}) == []
    assert pm.doc_kind(dict(title='מעקב אחר תיקון ליקויים מדוח 60 א')) == 'follow_up'
    assert pm.doc_kind(dict(title='הערות ראש הממשלה לדוח 47')) == 'pm_comments'
    assert pm.doc_kind(dict(title='ביומטריה בתנופה', publication_type='דוחות')) == 'other'


def test_text_windows_overlap_and_cover_the_text():
    text = '\n\n'.join('פסקה מספר %d ' % i * 50 for i in range(200))
    windows = list(pm.text_windows(text))
    assert windows[0]['first'] == 1 and windows[-1]['last'] == len(windows)
    assert all(w['n'] == 1 for w in windows)
    joined = ''.join(w['text'] for w in windows)
    assert len(joined) >= len(text)


def window(first, last, n, hay='', total=10):
    return dict(first=first, last=last, n=n, total=total, what='', hay=sc.normalize_for_match(hay), data=b'')


def test_analyze_windows_stitches_continuations_and_overlaps(monkeypatch):
    finding_a = 'אין במשרד הפנים מערכת ממוחשבת לקליטת הבקשות להיתרי אשראי'
    finding_b = 'משרד הפנים לא הוציא הנחיות רוחביות לרשויות המקומיות בדבר מחזור הלוואות'
    outputs = iter([
        dict(report_code='73א', pages=[dict(page=1, kind='toc'), dict(page=2, kind='items')], items=[
            dict(page=2, continues_from_previous=False, continues_next=True, chapter_title='הלוואות ומקורות אשראי',
                 ministry='משרד הפנים', number='1', title='מערכת ממוחשבת', finding_text=finding_a,
                 responses=[dict(body='משרד הפנים', text='המשרד מסכים', status_normalized='no_status')])]),
        dict(pages=[dict(page=1, kind='items'), dict(page=2, kind='items')], items=[
            # the cut-off item continues
            dict(page=1, continues_from_previous=True, continues_next=False, chapter_title='הלוואות ומקורות אשראי',
                 finding_text='', responses=[dict(body='משרד הפנים', text='ויפעל להקמת מערכת',
                                                  status='בטיפול', status_normalized='in_progress')]),
            dict(page=2, continues_from_previous=False, continues_next=False, chapter_title='הלוואות ומקורות אשראי',
                 number='2', title='הנחיות', finding_text=finding_b, responses=[])]),
    ])
    monkeypatch.setattr(pm, 'ask', lambda w, prompt, doc: next(outputs))
    doc = dict(title='הערות ראש הממשלה 73 א', publish_date=datetime.date(2022, 11, 22), url_name='x')
    result = pm.analyze_windows([window(1, 2, 2, hay=finding_a), window(2, 3, 2, hay=finding_a + ' ' + finding_b)], doc)
    items = result['items']
    assert [i['number'] for i in items] == ['1', '2']
    assert items[0]['responses'][0]['text'] == 'המשרד מסכים ויפעל להקמת מערכת'
    assert items[0]['responses'][0]['status_normalized'] == 'in_progress'
    assert items[0]['pages'] == [2]
    assert all(i['verified'] for i in items)
    assert result['report_code'] == '73א'
    assert result['coverage'] == 1.0
    assert result['status'] == 'ok'


def book_file(name, ext):
    return dict(file_name=name, ext=ext, url='https://src/' + name, stored_url='https://x/' + name)


def test_word_copies_prefers_the_word_copy():
    pdf, word = book_file('files_68A.pdf', 'pdf'), book_file('files_68Aword.docx', 'docx')
    assert pm.word_copies(dict(files=[pdf, word])) == ([word], [pdf])
    # Two PDFs (e.g. two books under one page), or only Word files: all are read
    other = book_file('files_note110324.pdf', 'pdf')
    assert pm.word_copies(dict(files=[pdf, other])) == ([pdf, other], [])
    doc1, doc2 = book_file('files_53 2.doc', 'doc'), book_file('files_53 1 2.doc', 'doc')
    assert pm.word_copies(dict(files=[doc1, doc2])) == ([doc1, doc2], [])
    # A file that wasn't backed up isn't counted as a copy
    assert pm.word_copies(dict(files=[pdf, dict(word, stored_url=None)])) == ([pdf], [])


def test_analyze_falls_back_to_the_pdf_when_the_word_copy_fails(monkeypatch):
    pdf, word = book_file('a.pdf', 'pdf'), book_file('a.doc', 'doc')
    good, bad = book_file('b.pdf', 'pdf'), book_file('b.docx', 'docx')
    item = dict(finding_text='ליקוי', responses=[], verified=True, pages=[1])
    read = []

    def analyze_file(doc, f):
        read.append(f['file_name'])
        if f['file_name'] == 'b.docx':
            return dict(status='analysis-failed', items=[])
        return dict(status='ok', items=[item], coverage=1.0, pages=1)
    monkeypatch.setattr(pm, 'analyze_file', analyze_file)
    monkeypatch.setattr(pm, 'find_duplicates', lambda books, plans: {})
    docs = [dict(url_name=n, title=n, kind='pm_comments', publish_date=None, files=files)
            for n, files in (('a', [pdf, word]), ('b', [good, bad]))]
    _, responses = pm.analyze(docs)
    assert sorted(read) == ['a.doc', 'b.docx', 'b.pdf']
    assert pdf['analysis_status'] == 'word-copy-read'
    assert sorted(r['file_name'] for r in responses) == ['a.doc', 'b.pdf']


def test_reverify_rechecks_cached_items(monkeypatch):
    text = 'בביקורת עלה כי המשרד לא קבע נוהל לטיפול בפניות הציבור ולא מינה ממונה על הפניות'
    monkeypatch.setattr(pm, 'word_text', lambda path, ext: 'כותרת\n\n' + text)
    result = dict(items=[dict(finding_text=text, verified=False, pages=[1]),
                         dict(finding_text='טקסט שאינו מופיע במסמך כלל ועיקר אלא הומצא', verified=True, pages=[1])],
                  coverage=1.0, failed_windows=0, status='low-coverage')
    pm.reverify(result, 'unused.doc', 'doc')
    assert [i['verified'] for i in result['items']] == [True, False]
    assert result['n_verified'] == 1
    assert result['status'] == 'low-coverage'  # 1 of 2 is below LOW_COVERAGE
    result['items'].pop()
    assert pm.reverify(result, 'unused.doc', 'doc')['status'] == 'ok'


# --- issue_linking ----------------------------------------------------------------------------------------------

def test_union_find():
    uf = il.UnionFind()
    uf.union('a', 'b')
    uf.union('c', 'b')
    assert uf.find('a') == uf.find('c')
    assert uf.find('d') == 'd'


def test_build_issues_follows_a_finding_through_response_and_follow_up():
    findings = [
        dict(finding_id='100-1', chapter_id=100, publish_date=datetime.date(2018, 5, 1), title='ליקוי במערכת',
             description='תיאור', responsible_bodies=['משרד הפנים'], severity='medium', followup_status='not_applicable'),
        dict(finding_id='200-3', chapter_id=200, publish_date=datetime.date(2022, 5, 1), title='ליקוי במערכת - מעקב',
             description='תוקן חלקית', responsible_bodies=['משרד הפנים'], severity='high',
             followup_status='partially_rectified'),
        dict(finding_id='300-1', chapter_id=300, publish_date=datetime.date(2020, 1, 1), title='ליקוי אחר',
             description='אחר', responsible_bodies=[], severity='low', followup_status='not_applicable'),
    ]
    responses = [dict(response_id='book/f.pdf/1', publish_date=datetime.date(2018, 5, 1), doc_title='הערות',
                      title='מערכת', status='in_progress', responses=[dict(body='משרד הפנים', text='בטיפול')])]
    links = [
        dict(level='item', from_type='response', from_id='book/f.pdf/1', to_type='finding', to_id='100-1'),
        dict(level='item', from_type='finding', from_id='200-3', to_type='finding', to_id='100-1'),
        dict(level='chapter', from_type='chapter', from_id='200', to_type='chapter', to_id='100'),
    ]
    issues, members = il.build_issues(findings, responses, links)
    by_id = {i['issue_id']: i for i in issues}
    assert set(by_id) == {'100-1', '300-1'}
    issue = by_id['100-1']
    assert issue['n_audits'] == 2 and issue['n_responses'] == 1
    assert issue['first_reported'] == datetime.date(2018, 5, 1)
    assert issue['latest_status'] == 'partially_rectified'
    assert issue['severity'] == 'high'
    assert [e['kind'] for e in issue['timeline']] == ['finding', 'response', 'follow_up']
    assert len([m for m in members if m['issue_id'] == '100-1']) == 3


def test_pace_spaces_requests_per_host(monkeypatch):
    import time
    from concurrent.futures import ThreadPoolExecutor
    monkeypatch.setitem(sc.MIN_INTERVAL, 'pace.test', 0.05)
    times = []

    def hit(_):
        sc.pace('https://pace.test/x')
        times.append(time.monotonic())
    with ThreadPoolExecutor(4) as executor:
        list(executor.map(hit, range(6)))
    times.sort()
    gaps = [b - a for a, b in zip(times, times[1:])]
    assert min(gaps) > 0.04


def test_find_duplicates_across_books(monkeypatch):
    import numpy as np
    a = np.arange(0, 1000, dtype=np.int64)
    prints = {
        'a.doc': (a, False),
        'a-copy.pdf': (np.arange(0, 900, dtype=np.int64), False),   # the same book under another page
        'b.pdf': (np.arange(500, 1600, dtype=np.int64), False),      # a follow-up book, sharing less than half
        'a-scan.pdf': (np.arange(5000, 5100, dtype=np.int64), True),  # unreadable, same title as 'a'
        'c-scan.pdf': (np.arange(6000, 6100, dtype=np.int64), True),  # unreadable, no other book with its title
    }
    monkeypatch.setattr(pm, 'fingerprint', lambda f: prints[f['file_name']])
    books = [dict(url_name=n, title=t, files=[book_file(n, n.split('.')[-1])]) for n, t in (
        ('a.doc', 'הערות ראש הממשלה לדוח 59 ב'), ('a-copy.pdf', 'הערות לדוח 59ב'), ('b.pdf', 'מעקב 59ב'),
        ('a-scan.pdf', 'הערות ראש הממשלה לדוח 59 ב'), ('c-scan.pdf', 'הערות ראש הממשלה לדוח 60 א'))]
    plans = {b['url_name']: (b['files'], []) for b in books}
    assert pm.find_duplicates(books, plans) == {'a-copy.pdf/a-copy.pdf': 'a.doc/a.doc',
                                                'a-scan.pdf/a-scan.pdf': 'a.doc/a.doc'}


def test_find_duplicates_prefers_the_word_copy(monkeypatch):
    import numpy as np
    prints = {'p.pdf': (np.arange(0, 1100, dtype=np.int64), False), 'w.doc': (np.arange(50, 1000, dtype=np.int64), False)}
    monkeypatch.setattr(pm, 'fingerprint', lambda f: prints[f['file_name']])
    books = [dict(url_name=n, title=n, files=[book_file(n, n.split('.')[-1])]) for n in prints]
    plans = {b['url_name']: (b['files'], []) for b in books}
    assert pm.find_duplicates(books, plans) == {'p.pdf/p.pdf': 'w.doc/w.doc'}


def test_uncheckable_items_dont_count():
    items = [dict(verified=True), dict(verified=None), dict(verified=None), dict(verified=None)]
    result = pm.set_status(dict(items=items, coverage=1.0, failed_windows=0))
    assert result['status'] == 'ok'
    assert result['n_uncheckable'] == 3
    result = pm.set_status(dict(items=items + [dict(verified=False)] * 2, coverage=1.0, failed_windows=0))
    assert result['status'] == 'low-coverage'


def test_scanned_pages():
    text = 'משרד הפנים לא הוציא הנחיות רוחביות לרשויות המקומיות. ' * 5
    assert ct.scanned_pages(['', ' \n', '', text]) == [1, 2, 3]
    # A blank page in a document that has text is just blank
    assert ct.scanned_pages([text, '', text, text]) == []


def test_language():
    assert ca.language('דוח ביקורת על משרד האוצר, 2023. Ministry of Finance') == 'he'
    assert ca.language('The State Comptroller audited the Ministry of Finance (משרד האוצר).') == 'en'
    assert ca.language('يعرض هذا التقرير نتائج الرقابة') == 'ar'


def test_transcribe_retries_and_splits(monkeypatch, tmp_path):
    def pdf_slice(path, first, last, dest):
        with open(dest, 'w') as f:
            f.write('%d-%d' % (first, last))
        return dest
    calls = []

    def complete_with_file(prompt, data, mime, **kwargs):
        first, last = map(int, data.decode().split('-'))
        calls.append((first, last))
        if last - first >= 2:  # windows of 3+ pages always come back empty
            raise ValueError('Expecting value: line 1 column 1 (char 0)')
        return False, dict(pages=[dict(page=i + 1, markdown='עמוד %d' % n) for i, n in enumerate(range(first, last + 1))])
    monkeypatch.setattr(ca.sc, 'pdf_slice', pdf_slice)
    monkeypatch.setattr(ca.llm, 'complete_with_file', complete_with_file)
    result = ca.transcribe('x.pdf', [3, 4, 5, 6])
    assert result == {n: 'עמוד %d' % n for n in (3, 4, 5, 6)}
    assert calls[:2] == [(3, 6), (3, 6)]  # tried twice, then in halves
    assert (3, 4) in calls and (5, 6) in calls


def test_transcribe_falls_back_to_an_image(monkeypatch):
    def pdf_slice(path, first, last, dest):
        with open(dest, 'w') as f:
            f.write('pdf')
        return dest
    mimes = []

    def complete_with_file(prompt, data, mime, **kwargs):
        mimes.append(mime)
        if mime == 'application/pdf':
            raise ValueError('Empty response (finish reason None, prompt feedback block_reason=OTHER)')
        return False, dict(pages=[dict(page=1, markdown='עמוד 21')])
    monkeypatch.setattr(ca.sc, 'pdf_slice', pdf_slice)
    monkeypatch.setattr(ca.sc, 'page_image', lambda path, page, tmp: b'png')
    monkeypatch.setattr(ca.llm, 'complete_with_file', complete_with_file)
    assert ca.transcribe('x.pdf', [21]) == {21: 'עמוד 21'}
    assert mimes == ['application/pdf', 'application/pdf', 'image/png']


def test_transcribe_survives_pages_poppler_cant_cut(monkeypatch):
    def pdf_slice(path, first, last, dest):
        if last > 2:  # a 2-page file
            raise RuntimeError('pdfseparate returned non-zero exit status 99')
        with open(dest, 'w') as f:
            f.write('%d-%d' % (first, last))
        return dest

    def page_image(path, page, tmp):
        if page > 2:
            raise RuntimeError('pdftoppm failed')
        return b'png'

    def complete_with_file(prompt, data, mime, **kwargs):
        first, last = (1, 1) if mime == 'image/png' else map(int, data.decode().split('-'))
        return False, dict(pages=[dict(page=i + 1, markdown='עמוד %d' % n) for i, n in enumerate(range(first, last + 1))])
    monkeypatch.setattr(ca.sc, 'pdf_slice', pdf_slice)
    monkeypatch.setattr(ca.sc, 'page_image', page_image)
    monkeypatch.setattr(ca.llm, 'complete_with_file', complete_with_file)
    assert ca.transcribe('x.pdf', [1, 2, 3]) == {1: 'עמוד 1', 2: 'עמוד 2'}


def test_split_pages_drops_the_piece_after_the_last_form_feed():
    assert ct.split_pages('one\ftwo\f') == ['one', 'two']
    assert ct.split_pages('one\f\ftwo\f') == ['one', '', 'two']  # a blank page is still a page
    assert ct.split_pages('no form feeds') == ['no form feeds']
    assert ct.split_pages('') == ['']
    # a 2-page scanned map: both pages textless, and no phantom page 3
    assert ct.scanned_pages(ct.split_pages('\n\f \f')) == [1, 2]


def test_failed_window_is_read_page_by_page(monkeypatch):
    def pdf_slice(path, first, last, dest):
        with open(dest, 'w') as f:
            f.write('%d-%d' % (first, last))
        return dest
    calls = []

    def ask_once(window, prompt, doc, mime_type=None, data=None):
        calls.append((window['first'], window['last'], mime_type))
        if window['n'] > 1 or (window['first'] == 3 and mime_type is None):
            return None  # the whole window fails, and page 3 is blocked as a PDF
        return dict(pages=[dict(page=1, kind='items')], items=[dict(page=1, finding_text='ליקוי %d' % window['first'])])
    monkeypatch.setattr(pm.sc, 'pdf_slice', pdf_slice)
    monkeypatch.setattr(pm.sc, 'page_image', lambda path, page, tmp: b'png')
    monkeypatch.setattr(pm, 'ask_once', ask_once)
    window = dict(first=2, last=4, n=3, total=10, path='x.pdf', data=b'', what='')
    out = pm.ask(window, lambda w: 'prompt', dict(url_name='x'))
    assert [i['finding_text'] for i in out['items']] == ['ליקוי 2', 'ליקוי 3', 'ליקוי 4']
    assert [i['page'] for i in out['items']] == [1, 2, 3]  # relative to the window
    assert (3, 3, 'image/png') in calls


def test_es_documents(tmp_path):
    import dataflows as DF
    import es_documents as ed
    chapters, issues = tmp_path / 'chapters', tmp_path / 'issues'
    audit = dict(chapter_id=1, title='מינהל הרכב', publication_title='דוח שנתי 61ב', publication_type='annual',
                 report_code='61ב', publish_date=datetime.date(2011, 5, 17), page_url='https://x/1', main_body='משרד האוצר',
                 audited_bodies=[dict(code='29', name='משרד האוצר')], llm_audited_bodies=[dict(name='החשב הכללי', role='secondary')],
                 language='he', analysis_status='ok', text_md='טקסט מלא', summary='סיכום', topics=['רכב'],
                 full_pdf_stored='https://s/1.pdf', earlier_reports=[], key_figures=[], n_findings=2)
    english = dict(audit, chapter_id=2, language='en', analysis_status='not-hebrew')
    findings = [dict(finding_id='1-%d' % i, chapter_id=1, seq=i, title='ממצא %d' % i, description='תיאור %d' % i,
                     responsible_bodies=['משרד האוצר'], severity='high', followup_status='not_applicable', verified=True)
                for i in (2, 1)]
    DF.Flow([audit, english], DF.update_resource(-1, name='audits'), [dict(f) for f in findings],
            DF.update_resource(-1, name='findings'), DF.dump_to_path(str(chapters))).process()
    timeline = [dict(date='2011-05-17', kind='finding', id='1-1', chapter_id=1, source='מינהל הרכב', title='ממצא 1'),
                dict(date='2012-01-01', kind='response', id='r/1', source='הערות', title='תגובה')]
    DF.Flow([dict(issue_id='1-1', title='ממצא 1', timeline=timeline, bodies=['משרד האוצר'],
                  first_reported=datetime.date(2011, 5, 17), last_seen=None),
             dict(issue_id='1-2', title='ממצא 2', timeline=timeline[:1], bodies=[])],
            DF.update_resource(-1, name='issues'),
            [dict(issue_id='1-1', member_type='finding', member_id='1-1'),
             dict(issue_id='1-1', member_type='response', member_id='r/1'),
             dict(issue_id='1-2', member_type='finding', member_id='1-2')],
            DF.update_resource(-1, name='issue_members'), DF.dump_to_path(str(issues))).process()

    results, dp, _ = DF.Flow(ed.documents_flow(str(chapters), str(issues))).results()
    by_name = dict(zip([r.name for r in dp.resources], results))
    audits, tracked = by_name[ed.AUDITS], by_name[ed.ISSUES]
    assert [a['chapter_id'] for a in audits] == [1]  # the English chapter isn't indexed
    a = audits[0]
    assert [f['finding_id'] for f in a['findings']] == ['1-1', '1-2']
    assert a['findings'][0]['issue_id'] == '1-1' and a['findings'][0]['issue_size'] == 2
    assert a['audited_bodies'] == ['משרד האוצר', 'החשב הכללי'] and a['audited_body_codes'] == ['29']
    assert a['tracked_issues'] == 1 and a['year'] == 2011 and a['text'] == 'טקסט מלא'
    assert [i['issue_id'] for i in tracked] == ['1-1']  # a single-finding issue is in its audit already
    assert tracked[0]['chapter_ids'] == [1] and tracked[0]['chapter_titles'] == ['מינהל הרכב']
    fields = {f.name: f.descriptor for f in dp.get_resource(ed.AUDITS).schema.fields}
    assert 'text_md' not in fields and fields['title']['es:title'] and fields['findings']['es:index'] is False


def test_incremental_scrape(monkeypatch):
    year = datetime.date.today().year

    def ms_date(y):
        return '/Date(%d)/' % int(datetime.datetime(y, 6, 1).timestamp() * 1000)
    hits = [dict(ListItemID=str(i), PublicationType='דוחות שנתיים', PublicationStartDate=ms_date(y), Path='p%d' % i)
            for i, y in ((1, year), (2, year - 10), (3, year - 20), (4, year - 15))]
    scraped = []

    def scrape_publication(hit):
        scraped.append(int(hit['ListItemID']))
        return dict(pub_id=int(hit['ListItemID']), title='new', publish_date=None), [dict(Order=1)]
    monkeypatch.setattr(ms, 'scrape_publication', scrape_publication)
    monkeypatch.setattr(ms, 'scrape_chapter', lambda task, pub, inst: dict(chapter_id=pub['pub_id'] * 10, pub_id=pub['pub_id']))
    monkeypatch.setattr(ms, 'Institutions', lambda: None)
    previous_pubs = [dict(pub_id=i, title='old') for i in (1, 2, 3, 5)]  # 4 is new to us, 5 no longer listed
    previous_chapters = [dict(chapter_id=i * 10, pub_id=i, title='old') for i in (1, 2, 3, 5)]
    pubs, chapters = ms.scrape(previous=previous_pubs, recent_years=3, hits=hits)
    assert sorted(scraped) == [1, 4]  # the recent one, and the one not seen before
    listed = {int(h['ListItemID']) for h in hits}
    pubs, chapters = ms.merge((previous_pubs, previous_chapters), listed, pubs, chapters)
    assert sorted((p['pub_id'], p['title']) for p in pubs) == [(1, 'new'), (2, 'old'), (3, 'old'), (4, 'new')]
    assert sorted(c['chapter_id'] for c in chapters) == [10, 20, 30, 40]
    # A kept chapter's file fields are recomputed from its files (an old run had picked the English abstract)
    old_files = _files(('תקציר הדוח', 'o/c.docx'), ('Abstract', 'o/EN/c-EN.docx'))
    previous_chapters[1].update(files=old_files, full_docx_url='https://x/Documents/o/EN/c-EN.docx')
    _, chapters = ms.merge((previous_pubs, previous_chapters), listed, [p for p in pubs if p['title'] == 'new'], [])
    kept = {c['chapter_id']: c for c in chapters}
    assert kept[20]['full_docx_url'] == 'https://x/Documents/o/c.docx' and kept[20]['title'] == 'old'
    assert kept[30]['full_docx_url'] is None  # no files at all
    # Without an earlier run, everything is scraped
    scraped.clear()
    ms.scrape(hits=hits)
    assert sorted(scraped) == [1, 2, 3, 4]
