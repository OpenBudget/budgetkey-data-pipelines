import importlib.util
import io
from pathlib import Path

import pytest

SCRAPER = Path(__file__).parents[2] / 'datapackage_pipelines_budgetkey/pipelines/budget/municipal/budget_files/budget_files_scraper.py'
spec = importlib.util.spec_from_file_location('budget_files_scraper', SCRAPER)
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class FakeResponse:
    def __init__(self, url, html, status=200):
        self.url, self.status_code = url, status
        self.headers = {'content-type': 'text/html'}
        self.raw = io.BytesIO(html.encode('utf-8'))
        self.raw.read = lambda n=-1, decode_content=True, _read=self.raw.read: _read(n)
        self.text = html

    def close(self):
        pass


class FakeSession:
    def __init__(self, pages):
        self.pages = pages
        self.requested = []

    def get(self, url, **kwargs):
        self.requested.append(url)
        if url not in self.pages:
            return FakeResponse(url, 'not found', status=404)
        return FakeResponse(url, self.pages[url])


@pytest.mark.parametrize('text, expected', [
    ('תקציב 2025', 'budget'),
    ('הצעת התקציב הרגיל 2025', 'budget_proposal'),
    ('תקציב בלתי רגיל לשנת 2026', 'development_budget'),
    ('התקציב הבלתי רגיל לשנת 2026', 'development_budget'),
    ('תקציב פיתוח לשנת 2025', 'development_budget'),
    ('עדכון תקציב מס\' 1 לשנת 2026', 'budget_update'),
    ('דו"ח תקציב מול ביצוע 2017', 'budget_vs_execution'),
    ('תמצית הדוחות הכספיים לשנת 2016', None),
    ('צו ארנונה 2025', None),
    ('דוח מבקר המועצה לשנת 2018', None),
])
def test_classify(text, expected):
    assert m.classify(text, '', '') == expected


def test_classify_uses_heading_only_for_bare_links():
    assert m.classify('2016', 'file.xlsx', 'תקציב רגיל') == 'budget'
    assert m.classify('צו ארנונה', 'file.pdf', 'תקציב שנתי, דוח כספי, צו ארנונה') is None
    assert m.classify('תמצית הדוחות הכספיים לשנת 2022', 'a.pdf', 'עדכון תקציב 2023') is None
    assert m.classify('רבעון ראשון 2021', 'a.pdf', 'תקציב 2021') is None


@pytest.mark.parametrize('args, expected', [
    (('תקציב 2025', 'a.xlsx', ''), (2025, 'link_text')),
    (('חוברת תקציב', 'taktziv__2019.pdf', ''), (2019, 'filename')),
    (('2016', 'x.xlsx', 'תקציב רגיל'), (2016, 'link_text')),
    (('אקסל', '1755517403.1298.xlsx', 'תקציב 2023'), (2023, 'context')),
    (('נתוני ביצוע מבוקר 2016, עדכון תקציב 2017, תקציב 2018', '', ''), (2018, 'link_text')),
    (('תקציב', '', ''), (None, None)),
])
def test_fiscal_year(args, expected):
    assert m.fiscal_year(*args) == expected


def test_page_with_base_href_iframe_and_file_browser():
    session = FakeSession({
        'https://muni.example.il/budget/': '''
            <html><head><base href="https://muni.example.il/"></head><body>
            <h3>תקציב רגיל</h3><a href="uploads/n/1.xlsx">2024</a>
            <a href="uploads/n/2.pdf">צו ארנונה</a>
            <iframe src="./filebrowser/?folder=42"></iframe>
            </body></html>''',
        'https://muni.example.il/filebrowser/?folder=42': '''
            <ul><li><a href="./?folder=42&sub=471">2025</a></li></ul>''',
        'https://muni.example.il/filebrowser/?folder=42&sub=471': '''
            <a href="./?folder=42&sub=42">תיקייה למעלה</a>
            <a href="../uploads/n/3.xlsx">‏‏הצעת התקציב הרגיל</a>''',
    })
    page_url, files = m.collect_files(session, 'https://muni.example.il/budget/')
    by_url = {f['file_url']: f for f in files}
    assert set(by_url) == {'https://muni.example.il/uploads/n/1.xlsx', 'https://muni.example.il/uploads/n/3.xlsx'}
    assert by_url['https://muni.example.il/uploads/n/1.xlsx']['fiscal_year'] == 2024
    proposal = by_url['https://muni.example.il/uploads/n/3.xlsx']
    assert (proposal['fiscal_year'], proposal['fiscal_year_source']) == (2025, 'context')
    assert proposal['doc_type'] == 'budget_proposal'
    assert proposal['link_text'] == 'הצעת התקציב הרגיל'
    assert proposal['page_url'] == 'https://muni.example.il/budget/'


AUTHORITY = dict(authority_code='31', entity_id='500200316', name='אופקים', website='https://muni.example.il/')
FILE = dict(file_url='https://muni.example.il/b.xlsx', format='xlsx', fiscal_year=2025)


def test_known_page_with_files_skips_discovery(monkeypatch):
    monkeypatch.setattr(m, 'check_page', lambda session, url: (url, [dict(FILE)]))
    monkeypatch.setattr(m, 'discover', lambda *args: pytest.fail('should not rediscover'))
    page, files = m.process_authority(AUTHORITY, dict(budget_page_url='https://muni.example.il/budget/',
                                                      page_source='heuristic', discovered_at='2026-01-01'))
    assert (page['status'], page['page_source'], page['discovered_at']) == ('ok', 'heuristic', '2026-01-01')
    assert len(files) == 1 and files[0]['authority_code'] == '31'


@pytest.mark.parametrize('known_page_result', [[], m.FetchError('http-404')])
def test_known_page_gone_or_empty_rediscovers(monkeypatch, known_page_result):
    def check_page(session, url):
        if isinstance(known_page_result, Exception):
            raise known_page_result
        return url, known_page_result
    monkeypatch.setattr(m, 'check_page', check_page)
    monkeypatch.setattr(m, 'discover', lambda *args: ('https://muni.example.il/new/', [dict(FILE)], 'heuristic'))
    page, files = m.process_authority(AUTHORITY, dict(budget_page_url='https://muni.example.il/old/',
                                                      page_source='heuristic'))
    assert (page['status'], page['budget_page_url'], page['n_spreadsheets']) == ('ok', 'https://muni.example.il/new/', 1)
    assert page['error'] is None


def test_override_page_without_visible_files_keeps_its_page_and_listed_files(monkeypatch):
    # Netanya: the budget page fills its file list by script, so the files are listed in budget-files.csv
    monkeypatch.setattr(m, 'check_page', lambda session, url: (url, []))
    monkeypatch.setattr(m, 'discover', lambda *args: pytest.fail('should not rediscover an override page'))
    listed = [dict(file_url='https://muni.example.il/b2026.xlsx', fiscal_year='2026',
                   link_text='ספר תקציב עירוני רגיל לשנת 2026 (excel)'),
              dict(file_url='https://muni.example.il/notes.pdf', fiscal_year='', link_text='דברי הסבר לתקציב 2025')]
    page, files = m.process_authority(AUTHORITY, dict(budget_page_url='https://muni.example.il/budget/',
                                                      page_source='override'), listed)
    assert (page['status'], page['page_source'], page['budget_page_url']) == \
        ('ok', 'override', 'https://muni.example.il/budget/')
    assert [(f['format'], f['fiscal_year'], f['fiscal_year_source'], f['page_url'], f['authority_code']) for f in files] == [
        ('xlsx', 2026, 'manual', 'https://muni.example.il/budget/', '31'),
        ('pdf', 2025, 'link_text', 'https://muni.example.il/budget/', '31')]
    assert page['n_files'] == 2 and page['latest_fiscal_year'] == 2026


def test_listed_files_found_on_the_page_are_not_repeated(monkeypatch):
    monkeypatch.setattr(m, 'check_page', lambda session, url: (url, [dict(FILE, page_url=url)]))
    listed = [dict(file_url=FILE['file_url'], fiscal_year='2025', link_text='תקציב 2025')]
    page, files = m.process_authority(AUTHORITY, dict(budget_page_url='https://muni.example.il/budget/',
                                                      page_source='heuristic'), listed)
    assert len(files) == 1


def test_unreachable_site(monkeypatch):
    def discover(*args):
        raise m.FetchError('blocked', '403')
    monkeypatch.setattr(m, 'discover', discover)
    page, files = m.process_authority(AUTHORITY, None)
    assert page['status'] == 'unreachable-blocked' and files == []


class BrowserResponse:
    """curl_cffi's response when not streamed: content, but no raw."""
    def __init__(self, url, html, status=200):
        self.url, self.status_code, self.headers = url, status, {'content-type': 'text/html'}
        self.content, self.text = html.encode('utf-8'), html

    def close(self):
        pass


def test_a_site_that_blocks_requests_is_fetched_as_a_browser(monkeypatch):
    # Jerusalem's Akamai answers Python's TLS handshake with a 403, whatever its headers
    blocked = FakeSession({})
    blocked.get = lambda url, **kw: blocked.requested.append(url) or FakeResponse(url, 'Access Denied', status=403)
    browser = FakeSession({})
    browser.get = lambda url, **kw: browser.requested.append(url) or BrowserResponse(url, '<a href="b.xlsx">תקציב 2026</a>')
    monkeypatch.setattr(m.browser_requests, 'Session', lambda impersonate: browser)
    _, files = m.collect_files(blocked, 'https://muni.example.il/budget/')
    assert [f['file_url'] for f in files] == ['https://muni.example.il/budget/b.xlsx']
    # The rest of the site goes straight to the browser session
    m.fetch(blocked, 'https://muni.example.il/other/')
    assert blocked.requested == ['https://muni.example.il/budget/']
    assert browser.requested == ['https://muni.example.il/budget/', 'https://muni.example.il/other/']


def test_unreachable_site_keeps_previous_results(monkeypatch):
    def fail(*args):
        raise m.FetchError('blocked', '403')
    monkeypatch.setattr(m, 'check_page', fail)
    monkeypatch.setattr(m, 'discover', fail)
    known = dict(budget_page_url='https://muni.example.il/budget/', page_source='heuristic',
                 discovered_at='2026-01-01', last_checked='2026-02-01', files=[dict(FILE)])
    page, files = m.process_authority(AUTHORITY, known)
    assert (page['status'], page['budget_page_url'], page['last_checked']) == \
        ('stale', 'https://muni.example.il/budget/', '2026-02-01')
    assert page['error'].startswith('unreachable') and len(files) == 1


def test_tel_aviv_sharepoint_tables():
    init = ("tab.init({'dataSource':{'ListId':'c598','ViewId':'8668'},'showTitle':false,'title':'טבלה'},"
            "'False',{'dataSource':{'ListId':null}})")
    page = '<html><body><div ng-init="%s"></div></body></html>' % init.replace('"', '&quot;')
    posted = []

    class Session(FakeSession):
        def post(self, url, json=None, **kwargs):
            posted.append((url, json))
            resp = FakeResponse(url, '')
            resp.json = lambda: {'items': [
                {'Attachments': ['https://www.tel-aviv.gov.il/Transparency/DocLib5/תקציב העירייה הרגיל לשנת 2026.xlsx'],
                 'Fields': [{'Caption': 'שנת תקציב', 'Value': '2026'},
                            {'Caption': 'שם', 'Value': 'תקציב העירייה הרגיל לשנת 2026.xlsx'}]},
                {'Attachments': ['https://www.tel-aviv.gov.il/Transparency/DocLib5/צו ארנונה.pdf'],
                 'Fields': [{'Caption': 'שנת תקציב', 'Value': '2026'}, {'Caption': 'שם', 'Value': 'צו ארנונה.pdf'}]},
            ]}
            return resp

    session = Session({'https://www.tel-aviv.gov.il/Transparency/Pages/CityFunds.aspx': page})
    _, files = m.collect_files(session, 'https://www.tel-aviv.gov.il/Transparency/Pages/CityFunds.aspx')
    assert posted[0][0] == 'https://www.tel-aviv.gov.il/_vti_bin/TlvSP2013PublicSite/TlvList.svc/GetTables'
    assert posted[0][1]['dataSource']['ListId'] == 'c598'
    assert [(f['fiscal_year'], f['format'], f['doc_type']) for f in files] == [(2026, 'xlsx', 'budget')]


def test_section_title_gives_context_to_bare_file_labels():
    # Haifa: the year and the word "תקציב" are only in the accordion's title, and the file's own label is wrong
    html = '''<ul class="accordion"><li class="accordion-item">
        <a class="accordion-title">תקציב רגיל לשנת 2025 – הוצאות (12)</a>
        <div class="accordion-content">
          <div class="tr_item"><div class="col"><span>נושאים שונים</span></div>
            <div class="col"><a href="https://h.example.il/a.xlsx"></a></div></div>
          <div class="tr_item"><div class="col"><span>פירוט הוצאות 2024</span></div>
            <div class="col"><a href="https://h.example.il/%D7%A4%D7%99%D7%A8%D7%95%D7%98.xlsx"></a></div></div>
        </div></li></ul>'''
    doc = m.lxml.html.fromstring(html, base_url='https://h.example.il/budget/')
    files = {f['file_url'].rsplit('/', 1)[-1]: f for f in m.budget_files(doc, 'https://h.example.il/budget/')}
    detail = files['%D7%A4%D7%99%D7%A8%D7%95%D7%98.xlsx']
    assert detail['context'] == 'תקציב רגיל לשנת 2025 – הוצאות (12) פירוט הוצאות 2024'
    assert (detail['doc_type'], detail['fiscal_year']) == ('budget', 2025)
    assert files['a.xlsx']['context'] == 'תקציב רגיל לשנת 2025 – הוצאות (12) נושאים שונים'
