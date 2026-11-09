"""Lists the State Comptroller's annual and special reports and their chapters, and backs their files up.

Source: the comptroller's digital library (library.mevaker.gov.il), a SharePoint site.
- Publications come from its anonymous search (the CSOM ProcessQuery endpoint the publications page itself uses;
  the list REST API is closed to anonymous users).
- Chapters, their main audited body and the other audited bodies come from the site's own JSON service.
- File links (full chapter and abstract, as docx and/or pdf) come from the server-rendered publication and
  chapter pages.

Every file is backed up to budgetkey-files under state-comptroller/reports/<publication item id>/.
"""
import datetime
import json
import logging
import os
import re
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

import requests
import dataflows as DF
from pyquery import PyQuery as pq

import sc_common as sc

BASE = 'https://library.mevaker.gov.il'
SITE = BASE + '/sites/DigitalLibrary'
SVC = SITE + '/_vti_bin/Mevaker.DigitalLibrary.Components/MevakerService.svc'
TYPES = ('דוחות שנתיים', 'דוחות מיוחדים')
TYPE_CODES = {'דוחות שנתיים': 'annual', 'דוחות מיוחדים': 'special'}
OUT_PATH = '/var/datapackages/state_comptroller/reports'

# The publications page's search query, as its search web part sends it. The site only lets anonymous users run
# queries under the page's own safe-query template, and caps each call at 10 rows.
PUBLICATIONS_CONTENT_TYPE = ('0x010100C568DB52D9D0A14D9B2FDCC96666E9F2007948130EC3DB064584E219954237AF39'
                             '00A9336C180C054E07BB233F6286110D0F*')
SOURCE_ID = '{5dc9f503-801e-4ced-8a2c-5d1237132419}'
SEARCH_FIELDS = ['ListItemID', 'Title', 'PublicationName', 'PublicationStartDate', 'PublicationHistoricalDate',
                 'PublicationType', 'Path']


def search_body(start_row, rows=10):
    select = ''.join(
        '<Method Name="Add" Id="%d" ObjectPathId="13"><Parameters><Parameter Type="String">%s</Parameter>'
        '</Parameters></Method>' % (100 + i, f) for i, f in enumerate(SEARCH_FIELDS))
    return (
        '<Request xmlns="http://schemas.microsoft.com/sharepoint/clientquery/2009" SchemaVersion="15.0.0.0" '
        'LibraryVersion="16.0.0.0" ApplicationName="Javascript Library"><Actions>'
        '<ObjectPath Id="1" ObjectPathId="0" />'
        '<SetProperty Id="3" ObjectPathId="0" Name="QueryText"><Parameter Type="String">*</Parameter></SetProperty>'
        '<SetProperty Id="4" ObjectPathId="0" Name="QueryTemplate"><Parameter Type="String">ContentTypeId:%s '
        '{SearchBoxQuery}</Parameter></SetProperty>'
        '<SetProperty Id="10" ObjectPathId="0" Name="RowLimit"><Parameter Type="Number">%d</Parameter></SetProperty>'
        '<SetProperty Id="11" ObjectPathId="0" Name="StartRow"><Parameter Type="Number">%d</Parameter></SetProperty>'
        '<SetProperty Id="12" ObjectPathId="0" Name="SourceId"><Parameter Type="Guid">%s</Parameter></SetProperty>'
        '<ObjectPath Id="14" ObjectPathId="13" />%s'
        '<SetProperty Id="36" ObjectPathId="0" Name="TrimDuplicates"><Parameter Type="Boolean">false</Parameter>'
        '</SetProperty>'
        '<SetProperty Id="51" ObjectPathId="0" Name="SafeQueryPropertiesTemplateUrl"><Parameter Type="String">'
        'querygroup://webroot/Pages/Publications.aspx?groupname=Default</Parameter></SetProperty>'
        '<SetProperty Id="52" ObjectPathId="0" Name="IgnoreSafeQueryPropertiesTemplateUrl">'
        '<Parameter Type="Boolean">false</Parameter></SetProperty>'
        '<ObjectPath Id="55" ObjectPathId="54" />'
        '<Method Name="ExecuteQueries" Id="60" ObjectPathId="54"><Parameters><Parameter Type="Array">'
        '<Object Type="String">q</Object></Parameter><Parameter Type="Array"><Object ObjectPathId="0" />'
        '</Parameter><Parameter Type="Boolean">true</Parameter></Parameters></Method>'
        '</Actions><ObjectPaths><Constructor Id="0" TypeId="{80173281-fffd-47b6-9a49-312e06ff8428}" />'
        '<Property Id="13" ParentId="0" Name="SelectProperties" />'
        '<Constructor Id="54" TypeId="{8d2ac302-db2f-46fe-9015-872b35f15098}" /></ObjectPaths></Request>'
    ) % (PUBLICATIONS_CONTENT_TYPE, rows, start_row, SOURCE_ID, select)


def parse_search_response(data):
    """Returns (total rows, result rows) from a ProcessQuery response."""
    if data[0].get('ErrorInfo'):
        raise RuntimeError('Search failed: %s' % data[0]['ErrorInfo'].get('ErrorMessage'))
    table = data[-1]['q']['ResultTables'][0]
    return table['TotalRows'], table['ResultRows']


def parse_ms_date(s):
    """'/Date(1782823200000)/' -> date, in Israel's time (the site stores local midnights as UTC)."""
    m = re.search(r'-?\d+', s or '')
    if not m:
        return None
    ts = datetime.datetime(1970, 1, 1) + datetime.timedelta(milliseconds=int(m.group()))
    return (ts + datetime.timedelta(hours=3)).date()


def search_publications():
    session = requests.Session()
    session.headers.update(sc.HEADERS)
    digest = session.post(SITE + '/_api/contextinfo', headers={'Accept': 'application/json; odata=nometadata'},
                          timeout=60).json()['FormDigestValue']
    rows, start, total = [], 0, None
    while total is None or start < total:
        for attempt in range(4):
            sc.pace(SITE)
            try:
                resp = session.post(SITE + '/_vti_bin/client.svc/ProcessQuery', data=search_body(start).encode('utf-8'),
                                    headers={'Content-Type': 'text/xml', 'X-RequestDigest': digest,
                                             'X-Requested-With': 'XMLHttpRequest'}, timeout=120)
                total, batch = parse_search_response(resp.json())
                break
            except Exception as e:
                if attempt == 3:
                    raise
                logging.info('Search page at %d failed (%s), retrying', start, e)
                import time
                time.sleep(10 * (attempt + 1))
        if not batch:
            break
        rows.extend(batch)
        start += len(batch)
    logging.info('Found %d publications (search reported %s)', len(rows), total)
    return rows


# --- pages ----------------------------------------------------------------------------------------------------

def absolute(href):
    return urllib.parse.urljoin(BASE + '/', href.strip()) if href else None


def page_files(d):
    """The files a publication or chapter page links to, as [{label, url, ext}]: each `.file-link` block has a
    label ('הדוח המלא', 'תקציר', 'שער ותוכן עניינים', ...) and one link per format."""
    files, seen = [], set()
    for block in d('.links-to-files .file-link').items():
        label = sc.clean_text(block('.title-file').text()).strip()
        for a in block('a'):
            url = absolute(a.attrib.get('href'))
            if not url or '/Documents/' not in url or url in seen:
                continue
            seen.add(url)
            files.append(dict(label=label, url=url, ext=sc.file_ext(url)))
    return files


def field_after(d, label_id):
    value = sc.clean_text(d('#' + label_id).next().text()).strip()
    return None if value in ('', '-') else value


def parse_publication_page(html):
    d = pq(html)
    return dict(
        title=sc.clean_text(d('h1').eq(0).text()).strip() or None,
        publish_date=sc.parse_dotted_date(d('#ctl00_PlaceHolderMain_pubDate').text()),
        catalog_number=field_after(d, 'ctl00_PlaceHolderMain_lblPublicationNum'),
        issn=field_after(d, 'ctl00_PlaceHolderMain_lblPublicationISSNnum'),
        foreword=sc.clean_text(d('.introduction-content').eq(0).text()).strip() or None,
        files=page_files(d),
    )


def parse_chapter_page(html):
    d = pq(html)
    item_id = re.search(r'"pageItemId":(\d+)', html)
    return dict(
        title=sc.clean_text(d('h1').eq(0).text()).strip() or None,
        page_item_id=int(item_id.group(1)) if item_id else None,
        files=page_files(d),
    )


def parse_institution_codes(s):
    """';29.7#;06116#;1255#' -> ['29.7', '06116', '1255']"""
    return [c for c in (part.strip(' #;') for part in (s or '').split(';')) if c]


class Institutions:
    """Audited-body code -> name, looked up once per code."""

    def __init__(self):
        self.names = {}

    def name(self, code):
        if code not in self.names:
            try:
                name = sc.http_json(SVC + '/GetAuditedInstitutionByCode/' + urllib.parse.quote(code))
            except Exception as e:
                logging.info('No name for institution %s: %s', code, e)
                name = None
            self.names[code] = sc.clean_text(name).strip() if isinstance(name, str) and name.strip() else None
        return self.names[code]


def pick(files, label_word, ext):
    for f in files:
        if label_word in f['label'] and f['ext'] == ext:
            return f
    return None


def file_object_name(pub_id, url):
    return '%s/reports/%s/%s' % (sc.PREFIX, pub_id, sc.file_basename(url))


def backup_files(pub_id, files):
    for f in files:
        f['object_name'] = file_object_name(pub_id, f['url'])
        f['stored_url'], f['status'] = sc.backup(f['url'], f['object_name'])
    return files


def scrape_chapter(task, publication, institutions):
    chapter_id = int(task['id'])
    page = parse_chapter_page(sc.http_get(task['url']).text)
    try:
        inst = sc.http_json(SVC + '/ReportInstitutionsByReportId/%d' % chapter_id)
    except Exception as e:
        logging.info('No institutions for chapter %s: %s', chapter_id, e)
        inst = {}
    main_code = (inst.get('mainInstCode') or '').strip() or None
    codes = parse_institution_codes(inst.get('auditInstitutionsCodes'))
    bodies = []
    for code in ([main_code] if main_code else []) + [c for c in codes if c != main_code]:
        bodies.append(dict(code=code, name=institutions.name(code), main=code == main_code))
    files = backup_files(publication['pub_id'], page['files'])
    row = dict(
        chapter_id=chapter_id,
        pub_id=publication['pub_id'],
        chapter_order=task.get('Order'),
        title=page['title'] or sc.clean_text(task.get('Title')).strip(),
        page_url=task['url'],
        publish_date=publication['publish_date'],
        publication_title=publication['title'],
        publication_type=publication['publication_type'],
        report_code=publication['report_code'],
        main_body_code=main_code,
        main_body=(institutions.name(main_code) if main_code else None) or task.get('GroupName'),
        audited_bodies=bodies,
        general_report_id=(inst.get('generalReportId') or None),
        files=files,
    )
    row.update(chapter_file_fields(files))
    return row


def english_file(f):
    """A translation: labelled in English only ('Abstract'), or under an EN folder / with an -EN suffix."""
    label = f.get('label') or ''
    return ((bool(re.search(r'[A-Za-z]', label)) and not re.search(r'[֐-׿]', label))
            or '/EN/' in f['url'] or bool(re.search(r'-EN\.\w+$', f['url'], re.I)))


def full_file(files, ext):
    """The full chapter among `files`: labelled 'הדוח המלא', else (old chapters label their only file differently)
    any file that isn't an abstract."""
    return pick(files, 'המלא', ext) or next((f for f in files if f['ext'] == ext and 'תקציר' not in f['label']), None)


def chapter_file_fields(files):
    """{full,abstract}_{docx,pdf}_{url,stored}. Labels are unreliable (an English file labelled 'הדוח המלא', the
    whole Hebrew chapter labelled 'תקציר הדוח'), so the full text is the first of:
    - the Hebrew full chapter, per format;
    - if there's none in either format, a Hebrew abstract (or a whole chapter labelled as one), but never a
      publication-wide 'ספר התקצירים';
    - an English file - so a chapter with Hebrew text is never analyzed from its English abstract."""
    row = {}
    for ext in ('docx', 'pdf'):
        f = pick(files, 'תקציר', ext)
        row['abstract_%s_url' % ext], row['abstract_%s_stored' % ext] = (f['url'], f.get('stored_url')) if f else (None, None)
    hebrew = [f for f in files if not english_file(f)]
    full = {ext: full_file(hebrew, ext) for ext in ('docx', 'pdf')}
    if not any(full.values()):
        full = {ext: next((f for f in hebrew if f['ext'] == ext and not f['label'].startswith('ספר')), None)
                for ext in ('docx', 'pdf')}
    if not any(full.values()):
        full = {ext: full_file(files, ext) for ext in ('docx', 'pdf')}
    for ext, f in full.items():
        row['full_%s_url' % ext], row['full_%s_stored' % ext] = (f['url'], f.get('stored_url')) if f else (None, None)
    return row


def cover_report_code(files):
    """Recent annual reports are titled by month ('דוח שנתי - נובמבר 2022'); their cover names the volume ('73א')."""
    import subprocess
    import tempfile
    for f in [f for f in files if f['ext'] == 'pdf'][:3]:
        if not f.get('stored_url'):
            continue
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'cover.pdf')
            if not sc.fetch_stored(f['object_name'], f['url'], path):
                continue
            out = subprocess.run(['pdftotext', '-l', '3', '-enc', 'UTF-8', path, '-'], capture_output=True, timeout=120)
            code = sc.report_code(out.stdout.decode('utf-8', 'replace'))
            if code:
                return code
    return None


def scrape_publication(hit):
    pub_id = int(hit['ListItemID'])
    page = parse_publication_page(sc.http_get(hit['Path']).text)
    publish_date = page['publish_date'] or parse_ms_date(hit.get('PublicationStartDate'))
    title = page['title'] or sc.clean_text(hit.get('PublicationName') or hit.get('Title')).strip()
    if title.endswith(' ' + hit['PublicationType']):
        title = title[:-len(hit['PublicationType'])].strip()
    publication = dict(
        pub_id=pub_id,
        page_url=hit['Path'],
        title=title,
        publication_type=TYPE_CODES[hit['PublicationType']],
        publication_type_he=hit['PublicationType'],
        publish_date=publish_date,
        historical_date=parse_ms_date(hit.get('PublicationHistoricalDate')),
        catalog_number=page['catalog_number'],
        issn=page['issn'],
        report_code=sc.report_code(title),
        foreword=page['foreword'],
    )
    publication['files'] = backup_files(pub_id, page['files'])
    if publication['report_code'] is None and publication['publication_type'] == 'annual':
        publication['report_code'] = cover_report_code(publication['files'])
    tasks = sc.http_json(SVC + '/publication/%d/tasks' % pub_id).get('data') or []
    publication['chapter_count'] = len(tasks)
    return publication, tasks


def scrape(limit=None, min_year=None, pub_ids=None, previous=None, recent_years=None, hits=None):
    """Scrapes the publications and their chapters. With `previous` (publication rows of an earlier run) and
    `recent_years`, only publications from the last `recent_years` years, or not in `previous`, are scraped; the
    caller keeps the rest from the earlier run. `hits`: the library's listing, if already fetched."""
    hits = [h for h in (search_publications() if hits is None else hits) if h.get('PublicationType') in TYPES]
    if pub_ids:
        hits = [h for h in hits if int(h['ListItemID']) in pub_ids]
    if previous is not None and recent_years:
        since = datetime.date.today().year - recent_years
        known = {p['pub_id'] for p in previous}
        listed = len(hits)
        hits = [h for h in hits if int(h['ListItemID']) not in known
                or (parse_ms_date(h.get('PublicationStartDate')) or datetime.date.today()).year >= since]
        logging.info('Scraping %d of %d publications: new ones, and those since %d', len(hits), listed, since)
    hits.sort(key=lambda h: parse_ms_date(h.get('PublicationStartDate')) or datetime.date(1900, 1, 1), reverse=True)
    if min_year:
        hits = [h for h in hits if (parse_ms_date(h.get('PublicationStartDate')) or datetime.date.today()).year >= min_year]
    if limit:
        hits = hits[:limit]
    institutions = Institutions()
    publications, chapters = [], []

    def one(hit):
        try:
            publication, tasks = scrape_publication(hit)
        except Exception:
            logging.exception('Failed to scrape publication %s', hit.get('Path'))
            return None, []
        rows = []
        for task in sorted(tasks, key=lambda t: t.get('Order') or 0):
            try:
                rows.append(scrape_chapter(task, publication, institutions))
            except Exception:
                logging.exception('Failed to scrape chapter %s', task.get('url'))
        logging.info('%s: %s, %d chapters', publication['publish_date'], publication['title'], len(rows))
        return publication, rows

    # Requests are paced per host in sc.http_get, so threads only overlap the waiting on responses
    with ThreadPoolExecutor(2) as executor:
        for publication, rows in executor.map(one, hits):
            if publication:
                publications.append(publication)
                chapters.extend(rows)
    if len(publications) < len(hits) * 0.9:
        raise RuntimeError('Only %d of %d publications scraped' % (len(publications), len(hits)))
    return publications, chapters


PUBLICATION_FIELDS = [
    ('pub_id', 'integer'), ('page_url', 'string'), ('title', 'string'), ('publication_type', 'string'),
    ('publication_type_he', 'string'), ('publish_date', 'date'), ('historical_date', 'date'),
    ('catalog_number', 'string'), ('issn', 'string'), ('report_code', 'string'), ('foreword', 'string'),
    ('files', 'array'), ('chapter_count', 'integer'),
]
CHAPTER_FIELDS = [
    ('chapter_id', 'integer'), ('pub_id', 'integer'), ('chapter_order', 'integer'), ('title', 'string'),
    ('page_url', 'string'), ('publish_date', 'date'), ('publication_title', 'string'),
    ('publication_type', 'string'), ('report_code', 'string'), ('main_body_code', 'string'),
    ('main_body', 'string'), ('audited_bodies', 'array'), ('general_report_id', 'string'), ('files', 'array'),
] + [('%s_%s_%s' % (k, e, s), 'string') for k in ('full', 'abstract') for e in ('docx', 'pdf') for s in ('url', 'stored')]


def output_flow(publications, chapters):
    return DF.Flow(
        sc.typed_resource(publications, 'publications', PUBLICATION_FIELDS, ['pub_id']),
        sc.typed_resource(chapters, 'chapters', CHAPTER_FIELDS, ['chapter_id']),
    )


# After the first full scrape, a run re-scrapes only the publications of the last few years (and any it hasn't seen):
# older publications don't change. STATE_COMPTROLLER_FULL=1 scrapes everything again.
RECENT_YEARS = 3


def previous_output(path):
    """(publications, chapters) of the last run's datapackage, or None."""
    try:
        results, dp, _ = DF.Flow(DF.load(os.path.join(path, 'datapackage.json'))).results()
    except Exception:
        logging.info('No earlier run at %s, scraping everything', path)
        return None
    by_name = dict(zip([r.name for r in dp.resources], results))
    if not by_name.get('publications'):
        return None
    return by_name['publications'], by_name.get('chapters') or []


def merge(previous, listed_ids, publications, chapters):
    """The scraped publications and chapters, plus the earlier run's for the publications not scraped again (if the
    library still lists them)."""
    old_pubs, old_chapters = previous
    scraped = {p['pub_id'] for p in publications}
    kept = {p['pub_id'] for p in old_pubs if p['pub_id'] not in scraped and p['pub_id'] in listed_ids}
    publications = publications + [p for p in old_pubs if p['pub_id'] in kept]
    chapters = chapters + [c for c in old_chapters if c['pub_id'] in kept]
    logging.info('Kept %d publications from the last run', len(kept))
    return publications, chapters


def flow(*_):
    limit = int(os.environ.get('STATE_COMPTROLLER_LIMIT') or 0) or None
    previous = None if os.environ.get('STATE_COMPTROLLER_FULL') or limit else previous_output(OUT_PATH)
    hits = search_publications()
    publications, chapters = scrape(limit, previous=previous[0] if previous else None, recent_years=RECENT_YEARS,
                                    hits=hits)
    if previous:
        listed = {int(h['ListItemID']) for h in hits if h.get('PublicationType') in TYPES}
        publications, chapters = merge(previous, listed, publications, chapters)
    publications.sort(key=lambda p: (p.get('publish_date') or datetime.date(1900, 1, 1), p['pub_id']), reverse=True)
    return DF.Flow(
        output_flow(publications, chapters),
        DF.update_resource(None, **{'dpp:streaming': True}),
        DF.dump_to_path(OUT_PATH),
        DF.dump_to_sql(dict(
            state_comptroller_publications={'resource-name': 'publications'},
            state_comptroller_chapters={'resource-name': 'chapters'},
        )),
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    limit = int(os.environ['LIMIT']) if os.environ.get('LIMIT') else None
    min_year = int(os.environ['MIN_YEAR']) if os.environ.get('MIN_YEAR') else None
    pub_ids = {int(p) for p in os.environ['PUB_IDS'].split(',')} if os.environ.get('PUB_IDS') else None
    out = os.environ.get('OUT', 'state_comptroller_reports_out')
    publications, chapters = scrape(limit, min_year, pub_ids)
    DF.Flow(output_flow(publications, chapters), DF.dump_to_path(out)).process()
