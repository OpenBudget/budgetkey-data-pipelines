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
