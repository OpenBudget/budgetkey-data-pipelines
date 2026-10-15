import ast
import csv
import json
import os
import re
import time
import logging
import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse, unquote

import requests
import urllib3
import lxml.html
import lxml.etree
import dataflows as DF


HERE = os.path.dirname(os.path.abspath(__file__))
OUT_PATH = '/var/datapackages/budget/municipal/budget_files'
WEBSITES = os.environ.get('MUNICIPAL_WEBSITES_DATAPACKAGE',
                          '/var/datapackages/entities/municipal_websites/datapackage.json')
PREVIOUS = os.environ.get('MUNICIPAL_BUDGET_FILES_PREVIOUS', OUT_PATH + '/datapackage.json')
# dpp re-runs this flow when this file changes, not when the modules it imports do: keep this equal to
# budget_file_analysis.ANALYZER_VERSION, so that a new analyzer re-analyzes the stored files, and bump
# REPORT_REVISION with changes to budget_report.py
ANALYZER_VERSION = 4
REPORT_REVISION = 2

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/126.0 Safari/537.36',
    'Accept-Language': 'he-IL,he;q=0.9,en;q=0.5',
}
MAX_PAGES_PER_SITE = 25
MAX_DEPTH = 2
MAX_HTML = 3_000_000

# Links worth following when looking for the budget page, strongest first
NAV_KEYWORDS = [
    (re.compile(r'תקציב'), 10),
    (re.compile(r'דו"?״?ח(ות)? כספי|מידע (כספי|פיננסי)|כספים'), 5),
    (re.compile(r'שקיפות|מידע לציבור|חופש המידע|מידע ציבורי'), 4),
    (re.compile(r'budget|taktziv|takziv|finance|transparen|shkifut', re.I), 4),
    # Arabic-language sections of Arab local authorities' sites
    (re.compile(r'ميزانية|موازنة'), 10),
    (re.compile(r'تقارير مالية|التقارير المالية|معلومات مالية|المالية'), 5),
    (re.compile(r'الشفافية|حرية المعلومات|معلومات للجمهور'), 4),
    (re.compile(r'אודות|העירייה|המועצה|על העיר|על המועצה'), 1),
]
NAV_EXCLUDE = re.compile(r'facebook|instagram|twitter|youtube|whatsapp|mailto:|tel:|javascript:', re.I)

FILE_FORMATS = {'xlsx': 'xlsx', 'xls': 'xls', 'xlsm': 'xlsx', 'csv': 'csv', 'pdf': 'pdf'}
FILE_EXT = re.compile(r'\.(xlsx|xlsm|xls|csv|pdf)$', re.I)
SPREADSHEETS = {'xlsx', 'xls', 'csv'}

# Order matters: the first matching type wins
DOC_TYPES = [
    ('budget_vs_execution', re.compile(r'ביצוע|bitzua|bitsua|execution|تنفيذ', re.I)),
    ('development_budget', re.compile(r'פיתוח|תב"?״?ר|בלתי[- ]רגיל|tabar|pituach|تطوير|غير عادي', re.I)),
    ('budget_update', re.compile(r'עדכון|תיקון|update|idkun|تعديل|تحديث', re.I)),
    ('budget_proposal', re.compile(r'הצעת|proposal|hatzaat|اقتراح|مقترح', re.I)),
    ('budget', re.compile(r'תקציב|taktziv|takziv|tktzib|budget|ميزانية|موازنة', re.I)),
]
# Not budgets unless the link itself says it's a budget (e.g. a bare "2016" under a mixed heading)
NOT_BUDGET = re.compile(r'מבקר|ביקורת|ארנונה|ارنونا|جمعيات|עמותות|תמיכות|שכר|דו"?״?ח(ות)?\s+ה?כספי|מבוקר|ועדת|רבעון|רבעוני'
                        r'|تقارير مالية|التقارير المالية|البيانات المالية|بيانات مالية|تقرير مالي|مراقب|مدقق', re.I)
# Never budgets, even when they mention one: meeting minutes and protocols
NEVER_BUDGET = re.compile(r'פרוטוקול|פרטיכל|ישיבת (מועצה|מליאה|ועד)|ועדת כספים|محضر|بروتوكول', re.I)
BUDGET_WORD = re.compile(r'תקציב|taktziv|takziv|tktzib|budget|ميزانية|موازنة', re.I)
YEAR = re.compile(r'(?<!\d)(19[9]\d|20[0-4]\d)(?!\d)')

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# --- fetching ---------------------------------------------------------------

class FetchError(Exception):
    def __init__(self, kind, detail=''):
        super().__init__('%s %s' % (kind, detail))
        self.kind = kind


def get(session, url):
    try:
        resp = session.get(url, headers=HEADERS, timeout=30, verify=False, stream=True)
    except requests.exceptions.ConnectionError as e:
        kind = 'dns' if re.search(r'Name or service not known|nodename nor servname|getaddrinfo', str(e)) else 'connection'
        raise FetchError(kind, str(e)[:200])
    except requests.exceptions.Timeout:
        raise FetchError('timeout')
    except requests.exceptions.RequestException as e:
        raise FetchError('error', str(e)[:200])
    if resp.status_code in (401, 403, 451):
        raise FetchError('blocked', str(resp.status_code))
    if resp.status_code >= 400:
        raise FetchError('http-%d' % resp.status_code)
    return resp


def fetch(session, url):
    resp = get(session, url)
    if 'html' not in resp.headers.get('content-type', 'text/html'):
        raise FetchError('not-html', resp.headers.get('content-type'))
    content = resp.raw.read(MAX_HTML, decode_content=True)
    resp.close()
    # Without a declared charset lxml falls back to Latin-1, which garbles Hebrew
    charset = (re.search(r'charset=["\']?([\w-]+)', resp.headers.get('content-type', ''))
               or re.search(rb'charset=["\']?([\w-]+)', content[:4096]))
    encoding = charset.group(1) if charset else 'utf-8'
    encoding = encoding.decode('ascii', 'ignore') if isinstance(encoding, bytes) else encoding
    try:
        parser = lxml.html.HTMLParser(encoding=encoding)
    except LookupError:
        parser = lxml.html.HTMLParser(encoding='utf-8')
    try:
        doc = lxml.html.fromstring(content, base_url=resp.url, parser=parser)
    except Exception as e:
        raise FetchError('parse', str(e)[:200])
    # Relative links resolve against <base href> when the page sets one
    base = doc.find('.//base[@href]')
    if base is not None:
        doc.getroottree().docinfo.URL = urljoin(resp.url, base.get('href'))
    return resp.url, doc


# --- link extraction --------------------------------------------------------

def clean(text):
    # Drop the invisible right-to-left marks some CMSs pad link texts with
    return ' '.join(re.sub(r'[\u200e\u200f\u202a-\u202e]', '', text or '').split())


SECTION_TITLE_TAGS = {'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'summary', 'dt', 'legend', 'button'}
SECTION_TITLE_CLASS = re.compile(r'title|head|toggle|trigger|caption', re.I)


def section_title(a):
    """The title of the collapsible section the link sits in, e.g. Haifa's accordion "תקציב רגיל לשנת 2025 – הוצאות"
    above a file labelled only "פירוט הוצאות"."""
    el = a
    for _ in range(8):
        parent = el.getparent()
        if parent is None:
            break
        title = el.getprevious()
        while title is not None and not isinstance(title.tag, str):
            title = title.getprevious()
        if title is not None and (title.tag in SECTION_TITLE_TAGS or SECTION_TITLE_CLASS.search(title.get('class') or '')):
            text = clean(title.text_content())
            if 0 < len(text) < 150:
                return text
        el = parent
    return ''


def heading_context(a):
    label = nearest_label(a)
    title = section_title(a)
    return label if not title or title in label else clean('%s %s' % (title, label))


def nearest_label(a):
    # The nearest preceding heading-like text, e.g. a bare "2016" link under "תקציב רגיל"
    for el in a.itersiblings(lxml.etree.Element, preceding=True):
        text = clean(el.text_content())
        if el.tag in ('h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'strong', 'b', 'p') and 0 < len(text) < 150:
            return text
    parent = a.getparent()
    for _ in range(4):
        if parent is None:
            break
        for el in parent.itersiblings(lxml.etree.Element, preceding=True):
            text = clean(el.text_content())
            if 0 < len(text) < 150:
                return text
        parent = parent.getparent()
    return ''


def page_links(doc):
    seen = set()
    for a in doc.iter('a'):
        href = a.get('href')
        if not href or NAV_EXCLUDE.search(href):
            continue
        url = urljoin(doc.base_url, href.strip()).split('#')[0]
        if not url.startswith('http') or url in seen:
            continue
        seen.add(url)
        yield a, url, clean(a.text_content()) or clean(a.get('title')) or clean(a.get('aria-label'))


def file_format(url):
    m = FILE_EXT.search(unquote(urlparse(url).path))
    return FILE_FORMATS[m.group(1).lower()] if m else None


def classify(link_text, filename, context):
    own = '%s %s' % (link_text, filename)
    # Use the surrounding heading only when the link itself says nothing, e.g. "2016" or "קובץ אקסל"
    text = own if BUDGET_WORD.search(own) else '%s %s' % (own, context)
    if not BUDGET_WORD.search(text):
        return None
    if NOT_BUDGET.search(own) and not BUDGET_WORD.search(own):
        return None
    if NEVER_BUDGET.search(own):
        return None
    for doc_type, pattern in DOC_TYPES:
        if pattern.search(text):
            return doc_type


def fiscal_year(link_text, filename, context):
    max_year = datetime.date.today().year + 1
    for source, text in (('link_text', link_text), ('filename', filename), ('context', context)):
        years = [int(y) for y in YEAR.findall(text) if int(y) <= max_year]
        if years:
            # "ביצוע 2016, עדכון 2017, תקציב 2018": the budget year is the latest one mentioned
            return max(years), source
    return None, None


def budget_file(page_url, url, text, context):
    """A file row if the link is a budget document, else None."""
    fmt = file_format(url)
    if not fmt:
        return None
    filename = unquote(urlparse(url).path.rsplit('/', 1)[-1])
    doc_type = classify(text, filename, context)
    if not doc_type:
        return None
    year, year_source = fiscal_year(text, filename, context)
    return dict(
        page_url=page_url, file_url=url, link_text=text[:300], context=context[:300],
        format=fmt, doc_type=doc_type, fiscal_year=year, fiscal_year_source=year_source,
    )


def budget_files(doc, page_url, folder=''):
    files = (budget_file(page_url, url, text, clean('%s %s' % (folder, heading_context(a))))
             for a, url, text in page_links(doc))
    return [f for f in files if f]


TLV_TABLES = '/_vti_bin/TlvSP2013PublicSite/TlvList.svc/GetTables'


def sharepoint_tables(session, doc, page_url):
    """Tel Aviv's SharePoint pages fill their document tables in the browser, from TlvList.svc/GetTables.

    Each table widget carries its query in ng-init="tab.init({...})"; replaying it returns the documents
    with their attachment URLs and fields such as "שנת תקציב".
    """
    files = []
    for el in doc.xpath('//*[@ng-init][contains(@ng-init, "tab.init(")]'):
        init = el.get('ng-init')
        try:
            literal = init[init.index('(') + 1:init.rindex(')')]
            literal = literal.replace('true', 'True').replace('false', 'False').replace('null', 'None')
            query = next(a for a in ast.literal_eval('(%s,)' % literal) if isinstance(a, dict))
            resp = session.post(urljoin(page_url, TLV_TABLES), json=query, timeout=60, verify=False,
                                headers=dict(HEADERS, Accept='application/json'))
            items = resp.json().get('items') or []
        except Exception as e:
            logging.info('SharePoint table on %s failed: %s', page_url, e)
            continue
        for item in items:
            fields = {f.get('Caption'): f.get('Value') for f in item.get('Fields') or []}
            text = clean(str(fields.get('שם') or fields.get('כותרת') or ''))
            context = clean(' '.join('%s %s' % (k, v) for k, v in fields.items() if k and 'שנ' in k and v))
            for url in item.get('Attachments') or []:
                f = budget_file(page_url, url, text or unquote(url.rsplit('/', 1)[-1]), context)
                if f:
                    files.append(f)
    return files


MAX_SUBFOLDERS = 40
SUBFOLDER = re.compile(r'[?&]sub=(\d+)')


def iframes(doc):
    for frame in doc.iter('iframe'):
        src = frame.get('src')
        if src and not NAV_EXCLUDE.search(src):
            yield urljoin(doc.base_url, src.strip())


def collect_files(session, url):
    page_url, doc = fetch(session, url)
    return page_url, files_in(session, page_url, doc)


def files_in(session, page_url, doc):
    """Budget files on a page, including ones in same-site iframes and CMS file-browser subfolders."""
    files = budget_files(doc, page_url)
    if doc.xpath('//*[@ng-init][contains(@ng-init, "tab.init(")]'):
        files.extend(sharepoint_tables(session, doc, page_url))
    for frame_url in iframes(doc):
        if not same_site(frame_url, page_url):
            continue
        try:
            _, frame = fetch(session, frame_url)
        except FetchError:
            continue
        for f in budget_files(frame, page_url):
            files.append(f)
        # File browsers list one subfolder per year: "./?folder=42&sub=471" -> "2025"
        subfolders = [(u, t) for _, u, t in page_links(frame)
                      if SUBFOLDER.search(u) and same_site(u, page_url) and u != frame_url]
        visited = {frame_url}
        while subfolders and len(visited) <= MAX_SUBFOLDERS:
            sub_url, sub_name = subfolders.pop(0)
            if sub_url in visited:
                continue
            visited.add(sub_url)
            try:
                _, sub = fetch(session, sub_url)
            except FetchError:
                continue
            for f in budget_files(sub, page_url, folder=sub_name):
                files.append(f)
            subfolders.extend((u, '%s %s' % (sub_name, t)) for _, u, t in page_links(sub)
                              if SUBFOLDER.search(u) and same_site(u, page_url) and u not in visited)
    unique = {}
    for f in files:
        unique.setdefault(f['file_url'], f)
    return list(unique.values())


def nav_score(url, text):
    haystack = '%s %s' % (text, unquote(url))
    return sum(weight for pattern, weight in NAV_KEYWORDS if pattern.search(haystack))


def same_site(url, root):
    host, root_host = urlparse(url).hostname or '', urlparse(root).hostname or ''
    strip = lambda h: h[4:] if h.startswith('www.') else h
    return strip(host) == strip(root_host) or host.endswith('.' + strip(root_host))


def page_score(files):
    return sum(3 if f['format'] in SPREADSHEETS else 1 for f in files)


# --- LLM fallback -----------------------------------------------------------

LLM_SCHEMA = {
    'type': 'object',
    'properties': {'urls': {'type': 'array', 'items': {'type': 'string'}}},
    'required': ['urls'],
}

LLM_PROMPT = '''You are helping find where an Israeli local authority ({name}) publishes its annual budget
(תקציב הרשות) on its website {website}.
Below are links found on the site, one per line, as "link text | URL".
Pick up to 3 URLs of web pages (not files) that most likely list the authority's budget documents
(e.g. pages titled תקציב, תקציב העירייה/המועצה, דוחות כספיים ותקציב, שקיפות, מידע לציבור).
Only return URLs from the list. Return an empty list if none are plausible.

{links}
'''


def llm_available():
    return bool(os.environ.get('GEMINI_API_KEY'))


def llm_pick_pages(name, website, links):
    from datapackage_pipelines_budgetkey.common.llm import complete, MODEL_FAST
    lines = sorted(set('%s | %s' % (text[:80], url) for url, text in links if text))[:400]
    prompt = LLM_PROMPT.format(name=name, website=website, links='\n'.join(lines))
    try:
        _, result = complete(prompt, schema=LLM_SCHEMA, model=MODEL_FAST, temperature=0)
    except Exception:
        logging.exception('LLM failed for %s', name)
        return []
    known = set(url for url, _ in links)
    return [url for url in result.get('urls', []) if url in known][:3]


# --- per-authority logic ----------------------------------------------------

def check_page(session, url):
    return collect_files(session, url)


SITEMAP_PATHS = ['/sitemap.xml', '/sitemap_index.xml', '/wp-sitemap.xml']
SITEMAP_LOC = re.compile(r'<loc>\s*([^<\s]+)\s*</loc>')
BUDGET_URL = re.compile(r'תקציב|budget|taktziv|takziv', re.I)


def sitemap_candidates(session, root, limit=10):
    """URLs from the site's sitemaps that look like budget pages, for homepages rendered by JavaScript."""
    sitemaps = []
    try:
        robots = get(session, urljoin(root, '/robots.txt')).text
        sitemaps = re.findall(r'(?im)^\s*sitemap:\s*(\S+)', robots)
    except FetchError:
        pass
    sitemaps = sitemaps or [urljoin(root, path) for path in SITEMAP_PATHS]
    found, seen = [], set()
    while sitemaps and len(seen) < 20:
        sitemap = sitemaps.pop(0)
        if sitemap in seen:
            continue
        seen.add(sitemap)
        try:
            locs = SITEMAP_LOC.findall(get(session, sitemap).text)
        except FetchError:
            continue
        for loc in locs:
            if loc.endswith('.xml') or '.xml?' in loc:
                sitemaps.append(loc)
            elif BUDGET_URL.search(unquote(loc)) and not file_format(loc):
                found.append(loc)
    # Shorter paths first: "/budget/" before "/budget/2019/some-news-item/"
    return sorted(set(found), key=len)[:limit]


SCRIPT_PATH = re.compile(r'["\'](/[^"\'\s\\<>]{1,200})["\'\\]')


def script_links(doc):
    """Same-site paths mentioned in scripts, for navigation menus rendered by JavaScript (e.g. Next.js data)."""
    text = ' '.join(doc.xpath('//script/text()'))
    for path in set(SCRIPT_PATH.findall(text)):
        if BUDGET_URL.search(unquote(path)) and not file_format(path):
            yield urljoin(doc.base_url, path)


def discover(session, name, website):
    """Crawl from the homepage towards budget pages; returns (page_url, files, method)."""
    final_url, home = fetch(session, website)
    queue = [(nav_score(url, text), 1, url, text) for _, url, text in page_links(home)]
    queue += [(nav_score(url, ''), 1, url, '') for url in script_links(home)]
    all_links = {url: text for _, _, url, text in queue}
    home_files = files_in(session, final_url, home)
    best, best_score = (final_url, home_files, 'heuristic'), page_score(home_files)
    visited = {final_url, website}
    fetched = 0
    while queue and fetched < MAX_PAGES_PER_SITE:
        queue.sort(key=lambda x: -x[0])
        score, depth, url, text = queue.pop(0)
        if score <= 0 or url in visited or file_format(url) or not same_site(url, final_url):
            continue
        visited.add(url)
        fetched += 1
        try:
            page_url, doc = fetch(session, url)
            files = files_in(session, page_url, doc)
        except FetchError:
            continue
        if page_score(files) > best_score:
            best, best_score = (page_url, files, 'heuristic'), page_score(files)
        if depth < MAX_DEPTH:
            for _, child, child_text in page_links(doc):
                all_links.setdefault(child, child_text)
                if child not in visited:
                    # Children of a budget-ish page inherit some of its relevance
                    queue.append((nav_score(child, child_text) + score // 2, depth + 1, child, child_text))
        time.sleep(0.3)

    if best_score == 0:
        for url in sitemap_candidates(session, final_url):
            if url in visited:
                continue
            try:
                page_url, files = check_page(session, url)
            except FetchError:
                continue
            if page_score(files) > best_score:
                best, best_score = (page_url, files, 'sitemap'), page_score(files)

    if best_score == 0 and llm_available():
        for url in llm_pick_pages(name, website, list(all_links.items())):
            try:
                page_url, files = check_page(session, url)
            except FetchError:
                continue
            if page_score(files) > best_score:
                best, best_score = (page_url, files, 'llm'), page_score(files)
    return best


def process_authority(authority, known_page):
    code, name, website = authority['authority_code'], authority['name'], authority['website']
    now = datetime.datetime.now().replace(microsecond=0)
    result = dict(
        authority_code=code, entity_id=authority['entity_id'], name=name, website=website,
        authority_type=authority.get('authority_type'),
        budget_page_url=None, page_source=None, status=None, error=None,
        discovered_at=None, last_checked=now,
    )
    files = []
    session = requests.Session()

    if known_page and known_page.get('budget_page_url'):
        # Only re-discover when the known page is gone or no longer lists budget files
        try:
            page_url, files = check_page(session, known_page['budget_page_url'])
            if files:
                result.update(budget_page_url=page_url, page_source=known_page['page_source'], status='ok',
                              discovered_at=known_page.get('discovered_at') or now)
        except FetchError as e:
            result['error'] = 'known page: %s' % e
            logging.info('%s: known budget page failed: %s', name, e)

    if not files:
        if not website:
            result['status'] = 'no-website'
        else:
            try:
                page_url, files, method = discover(session, name, website)
                if files:
                    result.update(budget_page_url=page_url, page_source=method, status='ok',
                                  discovered_at=now, error=None)
                else:
                    result['status'] = 'no-budget-page'
            except FetchError as e:
                result.update(status='unreachable-%s' % e.kind, error=str(e)[:300])
                if known_page and known_page.get('files'):
                    # A site that is down or blocking us right now keeps what we found last time
                    files = [dict(f) for f in known_page['files']]
                    result.update(budget_page_url=known_page['budget_page_url'], status='stale',
                                  page_source=known_page['page_source'], discovered_at=known_page.get('discovered_at'),
                                  last_checked=known_page.get('last_checked'), error='unreachable: %s' % e)

    for f in files:
        f.update(authority_code=code, entity_id=authority['entity_id'], name=name)
    result.update(
        n_files=len(files),
        n_spreadsheets=sum(1 for f in files if f['format'] in SPREADSHEETS),
        latest_fiscal_year=max((f['fiscal_year'] for f in files if f['fiscal_year']), default=None),
    )
    logging.info('%s: %s %s (%d files)', name, result['status'], result['budget_page_url'] or '', len(files))
    return result, files


# --- inputs -----------------------------------------------------------------

def load_websites():
    return DF.Flow(DF.load(WEBSITES)).results()[0][0]


def load_known_pages():
    pages = {}
    try:
        page_rows, file_rows = DF.Flow(DF.load(PREVIOUS)).results()[0]
        pages = {row['authority_code']: dict(row, files=[]) for row in page_rows if row.get('budget_page_url')}
        for row in file_rows:
            if row['authority_code'] in pages and row['page_url']:
                pages[row['authority_code']]['files'].append({k: row[k] for k in FILE_FIELDS})
    except Exception as e:
        logging.warning('No previous results, discovering all pages (%s)', e)
    with open(os.path.join(HERE, 'budget-pages.csv')) as f:
        for row in csv.DictReader(f):
            # Manually verified pages take precedence over anything discovered
            previous = pages.get(row['authority_code'], {})
            same_page = previous.get('budget_page_url') == row['budget_page_url']
            pages[row['authority_code']] = dict(budget_page_url=row['budget_page_url'], page_source='override',
                                                files=previous.get('files', []) if same_page else [])
    return pages


def scrape(limit=None):
    authorities = load_websites()
    if limit:
        authorities = authorities[:limit]
    known = load_known_pages()
    with ThreadPoolExecutor(8) as executor:
        results = list(executor.map(lambda a: process_authority(a, known.get(a['authority_code'])), authorities))
    pages = [r for r, _ in results]
    files = [f for _, fs in results for f in fs]
    logging.info('Budget pages found for %d/%d authorities (%d more kept from the previous run), %d files',
                 sum(1 for p in pages if p['status'] == 'ok'), len(pages),
                 sum(1 for p in pages if p['status'] == 'stale'), len(files))
    return pages, files


def store_and_analyze(files):
    """Keeps a copy of each file in the object store and checks whether it's ready to process."""
    import budget_file_store
    with ThreadPoolExecutor(8) as executor:
        results = list(executor.map(budget_file_store.process_file, files))
    files = [f for f, _ in results]
    tables = [t for _, ts in results for t in ts]
    logging.info('Analyzed %d files: %d ready to process, %d could not be downloaded', len(files),
                 sum(1 for f in files if f.get('ready_to_process')),
                 sum(1 for f in files if f.get('analysis_status') != 'ok'))
    return files, tables


PAGE_FIELDS = ['authority_code', 'entity_id', 'name', 'authority_type', 'website', 'budget_page_url', 'page_source',
               'status', 'error', 'n_files', 'n_spreadsheets', 'latest_fiscal_year', 'discovered_at', 'last_checked']
FILE_FIELDS = ['authority_code', 'entity_id', 'name', 'fiscal_year', 'doc_type', 'format', 'file_url',
               'link_text', 'context', 'fiscal_year_source', 'page_url']
ANALYZED_FILE_FIELDS = FILE_FIELDS + [
    'stored_url', 'file_size', 'analysis_status', 'table_kind', 'ready_to_process', 'n_tables', 'n_rows',
    'value_columns', 'expected_columns', 'income_sign', 'expense_sign', 'units', 'balance', 'issues']
TABLE_FIELDS = ['authority_code', 'name', 'fiscal_year', 'file_url', 'stored_url', 'format', 'table_index', 'sheet', 'kind',
                'header_row', 'first_col', 'code_col', 'title_col', 'n_rows', 'value_columns', 'income_sign',
                'expense_sign', 'units', 'units_source', 'expected_columns', 'ready', 'issues']


# Kept narrow: there are about a million lines. File details are in budget_files, joined on file_url.
LINE_FIELDS = ['authority_code', 'entity_id', 'name', 'fiscal_year', 'doc_type', 'code', 'chapter', 'direction',
               'title', 'column_kind', 'column_year', 'amount', 'value', 'units', 'sign_normalized',
               'format', 'sheet', 'row', 'file_url']
FORMAT_RANK = {'xlsx': 0, 'xls': 0, 'csv': 0, 'pdf': 1}


def line_sources(files):
    """Ready files, preferring spreadsheets when the same document is also published as a PDF."""
    ready = [f for f in files if f.get('ready_to_process')]
    best = {}
    for f in ready:
        key = (f['authority_code'], f['fiscal_year'], f['doc_type'])
        best[key] = min(best.get(key, 9), FORMAT_RANK[f['format']])
    return [f for f in ready if FORMAT_RANK[f['format']] == best[(f['authority_code'], f['fiscal_year'], f['doc_type'])]]


def column_kinds(f, t):
    """Value column index -> kind. In a regular budget the kind follows the column's year (executed Y-2,
    approved Y-1, proposed Y) and other columns get none; elsewhere it's the header's."""
    import budget_file_analysis
    import budget_file_store
    columns = t['value_columns']
    columns = json.loads(columns) if isinstance(columns, str) else columns
    if f['doc_type'] in budget_file_store.REGULAR_BUDGET:
        return budget_file_analysis.budget_columns(columns, f['fiscal_year'])
    return {n: c['kind'] for n, c in enumerate(columns)}


def budget_lines(files, tables):
    """One row per budget line and value column, in NIS, with incomes and expenses both positive."""
    import budget_file_store
    import budget_file_analysis
    by_file = defaultdict(dict)
    for t in tables:
        by_file[t['file_url']][t['table_index']] = t
    # The same table is often published twice, e.g. on its own and as a sheet of a workbook with all the others
    seen_tables = set()
    for f in sorted(line_sources(files), key=lambda f: f['file_url']):
        file_tables = by_file.get(f['file_url'], {})
        rows_by_table = defaultdict(list)
        for row in budget_file_store.read_rows(f):
            rows_by_table[int(row['table_index'])].append(row)
        unique = set()
        for index, table_rows in rows_by_table.items():
            content = frozenset((r['code'], r['year'], r['value']) for r in table_rows)
            key = (f['authority_code'], f['fiscal_year'], content)
            if key not in seen_tables:
                seen_tables.add(key)
                unique.add(index)
        kinds = {index: column_kinds(f, t) for index, t in file_tables.items() if t['ready']}
        for row in (r for index in sorted(unique) for r in rows_by_table[index]):
            t = file_tables.get(int(row['table_index']))
            if not t or not t['ready'] or int(row['column']) in t['duplicate_columns']:
                continue
            value = float(row['value'])
            direction = budget_file_analysis.chapter_kind(row['code'])
            sign = {'income': t['income_sign'], 'expense': t['expense_sign']}.get(direction)
            amount = value * (1000 if t['units'] == 'thousands' else 1) * (-1 if sign == 'negative' else 1)
            yield dict(
                authority_code=f['authority_code'], entity_id=f['entity_id'], name=f['name'],
                fiscal_year=f['fiscal_year'], doc_type=f['doc_type'], code=row['code'],
                chapter=budget_file_analysis.chapter_of(row['code']),
                direction=direction, title=row['title'],
                column_kind=kinds[int(row['table_index'])].get(int(row['column'])),
                column_year=int(row['year']) if row['year'] else None,
                amount=amount, value=value, units=t['units'], sign_normalized=sign is not None,
                format=f['format'], sheet=row['sheet'], row=row['row'], file_url=f['file_url'],
            )


def with_placeholder(rows, fields):
    # dataflows needs at least one row to infer a schema; the placeholder is filtered out right after
    empty = True
    for row in rows:
        empty = False
        yield row
    if empty:
        yield {k: None for k in fields}


def rows_or_placeholder(rows, fields):
    # dataflows needs at least one row to infer a schema; the placeholder is filtered out right after
    return [{k: r.get(k) for k in fields} for r in rows] or [{k: None for k in fields}]


def output_flow(pages, files, tables):
    return DF.Flow(
        [{k: p.get(k) for k in PAGE_FIELDS} for p in pages],
        DF.update_resource(-1, name='budget_pages', path='budget_pages.csv'),
        DF.set_type('n_files', type='integer', resources='budget_pages'),
        DF.set_type('n_spreadsheets', type='integer', resources='budget_pages'),
        DF.set_type('latest_fiscal_year', type='integer', resources='budget_pages'),
        DF.set_type('discovered_at', type='datetime', resources='budget_pages'),
        DF.set_type('last_checked', type='datetime', resources='budget_pages'),
        DF.set_primary_key(['authority_code'], resources='budget_pages'),
        rows_or_placeholder(files, ANALYZED_FILE_FIELDS),
        DF.update_resource(-1, name='budget_files', path='budget_files.csv'),
        DF.filter_rows(lambda row: row['file_url'] is not None, resources='budget_files'),
        DF.set_type('(fiscal_year|file_size|n_tables|n_rows)', type='integer', resources='budget_files'),
        DF.set_type('(ready_to_process|expected_columns)', type='boolean', resources='budget_files'),
        DF.set_type('balance', type='number', resources='budget_files'),
        rows_or_placeholder(tables, TABLE_FIELDS),
        DF.update_resource(-1, name='budget_file_tables', path='budget_file_tables.csv'),
        DF.filter_rows(lambda row: row['file_url'] is not None, resources='budget_file_tables'),
        DF.set_type('(fiscal_year|header_row|first_col|title_col|n_rows)', type='integer',
                    resources='budget_file_tables'),
        DF.set_type('(ready|expected_columns)', type='boolean', resources='budget_file_tables'),
        with_placeholder(budget_lines(files, tables), LINE_FIELDS),
        DF.update_resource(-1, name='budget_lines', path='budget_lines.csv'),
        DF.filter_rows(lambda row: row['file_url'] is not None, resources='budget_lines'),
        DF.set_type('(fiscal_year|column_year)', type='integer', resources='budget_lines'),
        DF.set_type('(amount|value)', type='number', resources='budget_lines'),
        DF.set_type('sign_normalized', type='boolean', resources='budget_lines'),
        DF.set_type('(code|chapter|row|sheet|title)', type='string', resources='budget_lines'),
    )


def write_report(path, pages, files):
    """Writes index.html next to the datapackage, once all rows have been dumped."""
    def func(package):
        yield package.pkg
        yield from package
        import budget_report
        os.makedirs(path, exist_ok=True)
        with open(os.path.join(path, 'index.html'), 'w', encoding='utf-8') as f:
            f.write(budget_report.render(pages, files))
    return func


def build(limit=None):
    pages, files = scrape(limit)
    files, tables = store_and_analyze(files)
    return pages, files, tables


def flow(*_):
    # MUNICIPAL_BUDGET_FILES_LIMIT runs on the first N authorities only, for testing the pipeline end to end
    pages, files, tables = build(int(os.environ.get('MUNICIPAL_BUDGET_FILES_LIMIT') or 0) or None)
    return DF.Flow(
        output_flow(pages, files, tables),
        DF.update_resource(None, **{'dpp:streaming': True}),
        DF.dump_to_path(OUT_PATH),
        DF.dump_to_sql(dict(
            municipal_budget_pages={'resource-name': 'budget_pages'},
            municipal_budget_files={'resource-name': 'budget_files'},
            municipal_budget_file_tables={'resource-name': 'budget_file_tables'},
            municipal_budget_lines={'resource-name': 'budget_lines'},
        )),
        write_report(OUT_PATH, pages, files),
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    limit = int(os.environ['LIMIT']) if os.environ.get('LIMIT') else None
    out = os.environ.get('OUT', 'municipal_budget_files_out')
    pages, files, tables = build(limit)
    DF.Flow(
        output_flow(pages, files, tables),
        DF.dump_to_path(out),
        write_report(out, pages, files),
    ).process()
