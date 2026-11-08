"""The government's responses to the State Comptroller's reports, as published on gov.il.

Source: the publications of אגף המפקח הכללי לענייני ביקורת המדינה (the Prime Minister's Office unit that follows up
on the comptroller's reports): the "הערות ראש הממשלה" books answering each report, and follow-up books on the
rectification of deficiencies (מעקב אחר תיקון ליקויים).
- The list comes from gov.il's PublicationApi, the files of each publication from its content-pages API.
- Files are backed up to budgetkey-files under state-comptroller/pm-responses/<UrlName>/.

The books have no reliable structure: tables of contents, page numbers, column layouts and section boundaries vary
between books and within the same book. So each book is read in overlapping windows of a few pages, sent to Gemini
as PDF, with the context carried over from the previous window (the chapter in force, the item left unfinished).
The items are then stitched across windows, checked against the PDF's text, and each book gets a coverage figure.
"""
import datetime
import difflib
import hashlib
import itertools
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor

import dataflows as DF

import sc_common as sc
import chapter_text as ct

from datapackage_pipelines_budgetkey.common import llm

ANALYZER_VERSION = 1
OFFICE_ID = 'ea0d4b20-fa34-4c7b-b739-ed25291e4e41'
INDEX_URL = 'https://www.gov.il/he/api/PublicationApi/Index?OfficeId={office}&limit={end}&skip={skip}'
CONTENT_URL = 'https://openapi-gc.digital.gov.il/pub/cio/govil/rest/contentpage/v1/api/content-pages/{name}?culture=he'
CONTENT_HEADERS = {'x-client-id': '9KFgciHHGDyNiqz5MdQS0eK2ApeJYMc6YnElUICpN1atirZc', 'Origin': 'https://www.gov.il',
                   'Referer': 'https://www.gov.il/'}
PAGE_URL = 'https://www.gov.il/he/pages/{name}'
OUT_PATH = '/var/datapackages/state_comptroller/pm_responses'
WINDOW = 6
THREADS = int(os.environ.get('STATE_COMPTROLLER_THREADS') or 6)
LOW_COVERAGE = 0.6
# For trying prompts out: read only the first N windows of each file (the partial result isn't cached)
MAX_WINDOWS = int(os.environ.get('STATE_COMPTROLLER_MAX_WINDOWS') or 0) or None

USAGE = llm.Usage()


# --- listing and backup -------------------------------------------------------------------------------------

def fetch_index():
    items, skip, page = [], 0, 50
    while True:
        data = sc.http_json(INDEX_URL.format(office=OFFICE_ID, end=skip + page, skip=skip))
        results = data.get('results') or []
        if not results:
            break
        items.extend(results)
        skip += len(results)
        if skip > 10000:
            break
    logging.info('Found %d gov.il publications', len(items))
    return items


def first(v):
    return v[0] if isinstance(v, list) and v else (v or None)


def parse_index_item(item):
    published = (item.get('DocPublishedDate') or '')[:10]
    row = dict(
        item_id=int(item['ItemId']),
        url_name=item['UrlName'],
        title=sc.clean_text(item.get('Title')).strip(),
        publication_type=first(item.get('PublicationTypeDesc')),
        government=first(item.get('PmoGovernmentDesc')),
        publish_date=datetime.date.fromisoformat(published) if published else None,
        page_url=PAGE_URL.format(name=item['UrlName']),
    )
    row['kind'] = doc_kind(row)
    return row


def parse_content_files(content):
    files = []
    groups = (((content or {}).get('contentSub') or {}).get('filesToDownload') or {}).get('filesGroupItems') or []
    for group in groups:
        for f in group.get('items') or []:
            if f.get('url'):
                files.append(dict(url=f['url'], file_name=f.get('fileName') or sc.file_basename(f['url']),
                                  display_name=sc.clean_text(f.get('displayName')).strip() or None,
                                  ext=(f.get('extension') or sc.file_ext(f['url'])).lower()))
    return files


def object_name(url_name, file_name):
    return '%s/pm-responses/%s/%s' % (sc.PREFIX, url_name, file_name)


def scrape_docs(limit=None, names=None):
    docs = [parse_index_item(i) for i in fetch_index()]
    docs.sort(key=lambda d: d['publish_date'] or datetime.date(1900, 1, 1), reverse=True)
    if names:
        docs = [d for d in docs if d['url_name'] in names]
    if limit:
        docs = docs[:limit]
    for doc in docs:
        try:
            content = sc.http_json(CONTENT_URL.format(name=doc['url_name']), headers=CONTENT_HEADERS)
            doc['files'] = parse_content_files(content)
        except Exception as e:
            logging.warning('No files for %s: %s', doc['url_name'], e)
            doc['files'] = []
        for f in doc['files']:
            f['object_name'] = object_name(doc['url_name'], f['file_name'])
            f['stored_url'], f['status'] = sc.backup(f['url'], f['object_name'])
        doc['report_codes'] = sc.report_codes(doc['title'], *[f.get('display_name') or '' for f in doc['files']])
        doc['report_code'] = doc['report_codes'][0] if doc['report_codes'] else None
    return docs


def doc_kind(doc):
    """pm_comments: the government's comments on a report; follow_up: on the rectification of deficiencies;
    other: guidelines, committee reports and bulletins the same office publishes."""
    title = doc.get('title') or ''
    if 'מעקב' in title:
        return 'follow_up'
    if 'הערות' in title or 'הערות ראש הממשלה' in (doc.get('publication_type') or ''):
        return 'pm_comments'
    return 'other'


# --- analysis -----------------------------------------------------------------------------------------------

STR = {'type': 'string'}
WINDOW_SCHEMA = {
    'type': 'object',
    'properties': {
        'report_code': STR,
        'pages': {'type': 'array', 'items': {
            'type': 'object',
            'properties': {
                'page': {'type': 'integer'},
                'kind': {'type': 'string', 'enum': ['cover', 'toc', 'intro', 'items', 'narrative', 'blank', 'other']},
            },
            'required': ['page', 'kind'],
        }},
        'items': {'type': 'array', 'items': {
            'type': 'object',
            'properties': {
                'page': {'type': 'integer'},
                'continues_from_previous': {'type': 'boolean'},
                'continues_next': {'type': 'boolean'},
                'ministry': STR,
                'chapter_title': STR,
                'number': STR,
                'title': STR,
                'report': STR,
                'finding_text': STR,
                'responses': {'type': 'array', 'items': {
                    'type': 'object',
                    'properties': {
                        'body': STR,
                        'text': STR,
                        'status': STR,
                        'status_normalized': {'type': 'string', 'enum': [
                            'rectified', 'partially_rectified', 'in_progress', 'not_rectified', 'disputed',
                            'no_status']},
                    },
                    'required': ['body', 'text', 'status_normalized'],
                }},
            },
            'required': ['page', 'continues_from_previous', 'continues_next', 'chapter_title', 'finding_text',
                         'responses'],
        }},
    },
    'required': ['pages', 'items'],
}

WINDOW_PROMPT = '''{what} of "{title}", published by the Israeli Prime Minister's Office on {date}: the
government's responses to a report of the State Comptroller (מבקר המדינה), or a follow-up on the rectification of
deficiencies the comptroller found.

These books have no consistent layout - neither between books nor within the same book. A section may be a table
(e.g. columns מספר | כותרת | תוכן הליקוי/ההמלצה | תגובות הגופים המבוקרים, or columns with the status of
rectification), free text, minutes of a ministerial committee's decisions and their execution, or something else,
and the layout may change from one page to the next. Rely on the content, not on the layout.

Context carried over from the previous part:
- The comptroller report chapter in force when the previous part ended: {chapter} (ministry/body: {ministry})
- The last item of the previous part, which may continue at the start of this one: {last_item}
{overlap_note}
For each page (numbered 1-{n} within this part), give its kind. Then extract every item - one finding,
deficiency, recommendation or decision about the comptroller's report, together with the government bodies'
responses to it or the report on its execution - in reading order, all values in Hebrew except enums:
- page: the page (1-{n}) the item starts on.
- continues_from_previous: true if the item is the continuation of the last item of the previous part.
- continues_next: true if the item is cut off at the end of this part.
- ministry and chapter_title: the comptroller report chapter (audit) the item belongs to, as titled in the book,
  and the ministry or body it is listed under.
- number and title: the item's number and short title in the book, if it has them.
- report: the comptroller report the item refers to, if stated (e.g. "דוח 54א").
- finding_text: the comptroller's finding or recommendation (or the decision), as written - keep its wording.
- responses: each body's response or report, as written: body, text, status (the status of rectification as
  stated, if stated, e.g. "תוקן", "תוקן חלקית", "בטיפול"), status_normalized.
Also give report_code: the comptroller report this book answers (e.g. "73א"), if this part says.
Pages that are only a cover, a table of contents or an introduction have no items.'''

PDF_WHAT = 'This PDF holds pages {first}-{last} (of {total})'
TEXT_WHAT = ('Below is part {part} of {parts} (characters {first}-{last} of {total}) of the text, extracted from a '
             'Word file, which counts as a single page (page 1)')
OVERLAP_NOTE = ('- The start of this part overlaps the end of the previous part, already read: do not extract items '
                'from the overlap again, except to complete the last item above.\n')
TEXT_WINDOW = 24_000
TEXT_OVERLAP = 2_000


def page_texts(path):
    """Each page's normalized text, as pdftotext reads it both with and without -layout: with it, the cells of a
    table with narrow columns come out cut up line by line, interleaved with the neighbouring columns; without it,
    some layouts come out out of order. An item's text is looked for in both."""
    def read(*args):
        out = subprocess.run(['pdftotext', '-enc', 'UTF-8', *args, path, '-'], capture_output=True, timeout=600)
        return [sc.normalize_for_match(p) for p in out.stdout.decode('utf-8', 'replace').split('\f')]
    return [' '.join(p) for p in itertools.zip_longest(read('-layout'), read(), fillvalue='')]


def word_text(path, ext):
    if ext == 'doc':
        # antiword is in the pipelines image; macOS's textutil reads .doc too, for local runs
        cmd = ['antiword', '-m', 'UTF-8.txt', '-w', '0', path]
        if not shutil.which('antiword') and shutil.which('textutil'):
            cmd = ['textutil', '-convert', 'txt', '-stdout', path]
        out = subprocess.run(cmd, capture_output=True, timeout=600)
        if out.returncode != 0:
            raise RuntimeError('antiword failed: %s' % out.stderr.decode('utf-8', 'replace')[:300])
        text = out.stdout.decode('utf-8', 'replace').replace('[pic]', '')
    else:
        text = ct.docx_to_markdown(path)
    text = sc.clean_text(text)
    return re.sub(r'\n{3,}', '\n\n', re.sub(r'[ \t]+\n', '\n', text))


def pdf_windows(path):
    """Overlapping windows of WINDOW pages: (first page, last page, data, normalized text of the pages)."""
    total = ct.page_count(path)
    texts = page_texts(path)
    starts = list(range(1, total + 1, WINDOW - 1)) if total and total > WINDOW else [1]
    with tempfile.TemporaryDirectory() as tmp:
        for start in starts:
            end = min(total, start + WINDOW - 1)
            if start > 1 and end <= start:
                break
            with open(sc.pdf_slice(path, start, end, os.path.join(tmp, 'slice.pdf')), 'rb') as f:
                data = f.read()
            yield dict(first=start, last=end, n=end - start + 1, total=total, data=data,
                       what=PDF_WHAT.format(first=start, last=end, total=total),
                       hay=' '.join(texts[max(0, start - 2):end]))


def text_windows(text):
    """Overlapping windows of the text, cut at paragraph breaks; each counts as one 'page' (its index)."""
    spans, start = [], 0
    while start < len(text):
        end = min(len(text), start + TEXT_WINDOW)
        if end < len(text):
            cut = text.rfind('\n\n', start + TEXT_WINDOW // 2, end)
            end = cut if cut > 0 else end
        spans.append((start, end))
        if end >= len(text):
            break
        start = max(end - TEXT_OVERLAP, start + 1)
    for i, (start, end) in enumerate(spans, 1):
        yield dict(first=i, last=i, n=1, total=len(spans), text=text[start:end],
                   what=TEXT_WHAT.format(part=i, parts=len(spans), first=start, last=end, total=len(text)),
                   hay=sc.normalize_for_match(text[max(0, start - TEXT_OVERLAP):end + TEXT_OVERLAP]))


def item_key(item):
    return (sc.normalize_for_match(item.get('chapter_title'))[:60], (item.get('number') or '').strip())


def same_item(a, b):
    if item_key(a) == item_key(b) and item_key(a)[1]:
        return True
    ta = sc.normalize_for_match((a.get('title') or '') + ' ' + (a.get('finding_text') or ''))[:400]
    tb = sc.normalize_for_match((b.get('title') or '') + ' ' + (b.get('finding_text') or ''))[:400]
    return bool(ta) and difflib.SequenceMatcher(None, ta, tb).ratio() > 0.85


def merge_into(target, item):
    """Adds a continuation (or a re-read of the overlap) to an item, without duplicating text."""
    a, b = target.get('finding_text') or '', item.get('finding_text') or ''
    if b and sc.normalize_for_match(b) not in sc.normalize_for_match(a):
        target['finding_text'] = (a + ' ' + b).strip()
    for k in ('number', 'title', 'ministry'):
        target[k] = target.get(k) or item.get(k)
    for r in item.get('responses') or []:
        body = sc.normalize_for_match(r.get('body'))
        same = [t for t in target['responses'] if sc.normalize_for_match(t.get('body')) == body]
        if same:
            t = same[-1]
            if r.get('text') and sc.normalize_for_match(r['text']) not in sc.normalize_for_match(t.get('text')):
                t['text'] = ((t.get('text') or '') + ' ' + r['text']).strip()
            if t.get('status_normalized') in (None, 'no_status'):
                t['status'], t['status_normalized'] = r.get('status'), r.get('status_normalized')
        else:
            target['responses'].append(r)
    target['continues_next'] = item.get('continues_next')
    target['pages'] = sorted(set(target.get('pages', [])) | set(item.get('pages', [])))
    target['hay'] = target.get('hay', '') + ' ' + item.get('hay', '')


def ask(window, prompt, doc):
    for attempt in range(2):
        try:
            if 'data' in window:
                return llm.complete_with_file(prompt, window['data'], 'application/pdf', schema=WINDOW_SCHEMA,
                                              model=llm.MODEL_FAST, temperature=0, usage=USAGE, thinking='low')[1]
            return llm.complete(prompt + '\n\nTEXT:\n' + window['text'], schema=WINDOW_SCHEMA, model=llm.MODEL_FAST,
                                temperature=0, usage=USAGE, thinking='low')[1]
        except Exception as e:
            logging.warning('%s part %s-%s failed (%s)%s', doc['url_name'], window['first'], window['last'], e,
                            ', retrying' if attempt == 0 else '')
    return None


def analyze_windows(windows, doc):
    """Reads a book window by window, carrying the context over, and stitches the items across windows."""
    items, kinds, chapter, ministry, report_code, total, failed = [], {}, None, None, None, 0, 0
    if MAX_WINDOWS:
        windows = itertools.islice(windows, MAX_WINDOWS)
    for window in windows:
        start, end, total = window['first'], window['last'], window['total']
        last = items[-1] if items else None
        last_desc = 'none'
        if last:
            last_desc = '%s %s: %s ... %s' % (last.get('number') or '', last.get('title') or '',
                                              (last.get('finding_text') or '')[:200],
                                              ((last['responses'][-1].get('text') or '')[-200:]
                                               if last.get('responses') else ''))
        prompt = WINDOW_PROMPT.format(
            what=window['what'], title=doc['title'], date=doc['publish_date'], chapter=chapter or 'unknown',
            ministry=ministry or 'unknown', last_item=last_desc, overlap_note=OVERLAP_NOTE if start > 1 else '',
            n=window['n'])
        out = ask(window, prompt, doc)
        if out is None:
            failed += 1
            continue
        report_code = report_code or sc.report_code(out.get('report_code') or '')
        for p in out.get('pages') or []:
            n = start + int(p.get('page') or 1) - 1
            if start <= n <= end and not (n == start and n in kinds):
                kinds[n] = p.get('kind')
        overlap = start > 1 and window['n'] > 1
        for item in out.get('items') or []:
            n = min(max(start + int(item.get('page') or 1) - 1, start), end)
            item['pages'], item['hay'] = [n], window['hay']
            item['responses'] = item.get('responses') or []
            if items and item.get('continues_from_previous'):
                merge_into(items[-1], item)
                continue
            previous = [prev for prev in items[-8:] if same_item(item, prev)]
            if previous and (n == start or not overlap):
                merge_into(previous[-1], item)
                continue
            items.append(item)
        if items:
            chapter, ministry = items[-1].get('chapter_title') or chapter, items[-1].get('ministry') or ministry
    # Validation: an item's finding text must be found in the text of the window(s) it came from
    for item in items:
        item['verified'] = sc.fuzzy_contains(item.pop('hay', ''), item.get('finding_text') or '', min_ratio=0.7)
    content = [n for n, k in kinds.items() if k in ('items', 'narrative')]
    with_items = {p for item in items for p in item['pages']}
    coverage = (len([p for p in content if p in with_items or p - 1 in with_items]) / len(content)) if content else None
    return set_status(dict(items=items, coverage=coverage, pages=total, report_code=report_code,
                           page_kinds={str(k): v for k, v in sorted(kinds.items())}, failed_windows=failed))


def set_status(result):
    items = result['items']
    result['n_verified'] = verified = sum(1 for i in items if i.get('verified'))
    coverage = result.get('coverage')
    result['status'] = 'ok'
    if not items:
        result['status'] = 'no-items'
    elif ((coverage is not None and coverage < LOW_COVERAGE) or verified < len(items) * LOW_COVERAGE
          or result.get('failed_windows')):
        result['status'] = 'low-coverage'
    return result


def reverify(result, path, ext):
    """Checks a cached result's items against the file's text again, so improvements to the checking apply to
    results cached before them without asking Gemini again."""
    if ext == 'pdf':
        texts = page_texts(path)

        def hay(item):
            # The pages of the window(s) the item could have come from, and the one after it
            pages = item.get('pages') or [1]
            return ' '.join(texts[max(0, min(pages) - WINDOW - 1):max(pages) + 1])
    else:
        whole = sc.normalize_for_match(word_text(path, ext))

        def hay(item):
            return whole
    for item in result['items']:
        item['verified'] = sc.fuzzy_contains(hay(item), item.get('finding_text') or '', min_ratio=0.7)
    return set_status(result)


def analyze_file(doc, f):
    if f['ext'] not in ('pdf', 'doc', 'docx') or not f.get('stored_url'):
        return None
    name = f['object_name']
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'file.' + f['ext'])
        if not sc.fetch_stored(name, f['url'], path):
            return dict(status='download-failed', items=[])
        with open(path, 'rb') as fh:
            digest = hashlib.sha256(fh.read()).hexdigest()
        cached = sc.read_json(name + '.analysis.json')
        if cached and cached.get('version') == ANALYZER_VERSION and cached.get('sha256') == digest:
            try:
                return reverify(cached['result'], path, f['ext'])
            except Exception:
                logging.exception('Failed to check %s again, keeping its cached checks', f['url'])
                return cached['result']
        try:
            if f['ext'] == 'pdf':
                result = analyze_windows(pdf_windows(path), doc)
            else:
                text = word_text(path, f['ext'])
                sc.write_text(name + '.md', text, 'md')
                result = analyze_windows(text_windows(text), doc)
                result['units'] = 'text-parts'
        except Exception:
            logging.exception('Failed to analyze %s', f['url'])
            return dict(status='analysis-failed', items=[])
    # A result with windows Gemini failed on isn't cached, so the next run tries again
    if not MAX_WINDOWS and not result.get('failed_windows'):
        sc.write_json(name + '.analysis.json', dict(version=ANALYZER_VERSION, sha256=digest, result=result))
    return result


WORD = ('doc', 'docx')
READ_OK = ('ok', 'low-coverage')


def word_copies(doc):
    """Most books up to 2018 are published twice, as a PDF and as a Word copy of the same text (and where the PDF
    is a short summary of a follow-up book, the Word copy is the full book). Reading both would count every
    response twice, so only the Word copy is read: it's text, cheaper to read and checked exactly against its
    source. The PDFs are read when there's no Word copy, or it couldn't be read."""
    words = [f for f in doc['files'] if f['ext'] in WORD and f.get('stored_url')]
    pdfs = [f for f in doc['files'] if f['ext'] == 'pdf' and f.get('stored_url')]
    return (words, pdfs) if words and pdfs else (words + pdfs, [])


def analyze(docs):
    responses = []
    books = [doc for doc in docs if doc['kind'] != 'other']
    plans = {doc['url_name']: word_copies(doc) for doc in books}

    def one(task):
        doc, f = task
        result = analyze_file(doc, f)
        if result is not None:
            logging.info('%s %s: %s, %d items, coverage %s', doc['url_name'], f['file_name'], result['status'],
                         len(result['items']), result.get('coverage'))
        return task, result

    with ThreadPoolExecutor(THREADS) as executor:
        results = list(executor.map(one, [(doc, f) for doc in books for f in plans[doc['url_name']][0]]))
        read = {doc['url_name'] for (doc, f), r in results if r and r['status'] in READ_OK}
        fallback = [(doc, f) for doc in books if doc['url_name'] not in read for f in plans[doc['url_name']][1]]
        for doc in books:
            if doc['url_name'] in read:
                for f in plans[doc['url_name']][1]:
                    f['analysis_status'] = 'word-copy-read'
        results += list(executor.map(one, fallback))
        for (doc, f), result in results:
            if result is None:
                continue
            f.update(analysis_status=result['status'], n_items=len(result['items']), coverage=result.get('coverage'),
                     pages=result.get('pages'))
            doc['report_code'] = doc.get('report_code') or result.get('report_code')
            for seq, item in enumerate(result['items'], 1):
                responses.append(dict(
                    response_id='%s/%s/%d' % (doc['url_name'], f['file_name'], seq),
                    url_name=doc['url_name'],
                    doc_title=doc['title'],
                    doc_kind=doc['kind'],
                    file_name=f['file_name'],
                    file_url=f['url'],
                    stored_url=f['stored_url'],
                    publish_date=doc['publish_date'],
                    report_code=doc.get('report_code'),
                    seq=seq,
                    pages=item.get('pages'),
                    ministry=item.get('ministry') or None,
                    chapter_title=item.get('chapter_title') or None,
                    number=item.get('number') or None,
                    item_report_code=sc.report_code(item.get('report') or ''),
                    title=item.get('title') or None,
                    finding_text=item.get('finding_text'),
                    responses=item.get('responses'),
                    bodies=sorted({r.get('body') for r in item.get('responses') or [] if r.get('body')}),
                    status=next((r.get('status_normalized') for r in reversed(item.get('responses') or [])
                                 if r.get('status_normalized') not in (None, 'no_status')), None),
                    verified=item.get('verified'),
                ))
    logging.info('Gemini usage: %s', json.dumps(USAGE.by_model))
    return docs, responses


DOC_FIELDS = [
    ('item_id', 'integer'), ('url_name', 'string'), ('title', 'string'), ('publication_type', 'string'),
    ('government', 'string'), ('publish_date', 'date'), ('page_url', 'string'), ('kind', 'string'),
    ('report_code', 'string'), ('report_codes', 'array'), ('files', 'array'),
]
RESPONSE_FIELDS = [
    ('response_id', 'string'), ('url_name', 'string'), ('doc_title', 'string'), ('doc_kind', 'string'),
    ('file_name', 'string'),
    ('file_url', 'string'), ('stored_url', 'string'), ('publish_date', 'date'), ('report_code', 'string'),
    ('seq', 'integer'), ('pages', 'array'), ('ministry', 'string'), ('chapter_title', 'string'),
    ('number', 'string'), ('item_report_code', 'string'), ('title', 'string'), ('finding_text', 'string'),
    ('responses', 'array'),
    ('bodies', 'array'), ('status', 'string'), ('verified', 'boolean'),
]


def output_flow(docs, responses):
    return DF.Flow(
        sc.typed_resource(docs, 'response_docs', DOC_FIELDS, ['url_name']),
        sc.typed_resource(responses, 'responses', RESPONSE_FIELDS, ['response_id']),
    )


def flow(*_):
    from datapackage_pipelines_budgetkey.common.dump_to_sql_atomic import dump_to_sql_atomic
    limit = int(os.environ.get('STATE_COMPTROLLER_LIMIT') or 0) or None
    docs, responses = analyze(scrape_docs(limit))
    return DF.Flow(
        output_flow(docs, responses),
        DF.update_resource(None, **{'dpp:streaming': True}),
        DF.dump_to_path(OUT_PATH),
        dump_to_sql_atomic(dict(
            state_comptroller_response_docs={'resource-name': 'response_docs'},
            state_comptroller_responses={'resource-name': 'responses'},
        )),
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    for noisy in ('httpx', 'httpcore', 'google_genai', 'urllib3'):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    limit = int(os.environ['LIMIT']) if os.environ.get('LIMIT') else None
    names = set(os.environ['NAMES'].split(',')) if os.environ.get('NAMES') else None
    if os.environ.get('DOCS_JSON'):
        # Offline: docs (with their files) listed in a JSON file, e.g. from an earlier run; files are read from the store
        with open(os.environ['DOCS_JSON']) as f:
            docs = [dict(d, publish_date=datetime.date.fromisoformat(d['publish_date']) if d.get('publish_date') else None)
                    for d in json.load(f) if not names or d['url_name'] in names]
    else:
        docs = scrape_docs(limit, names)
    if os.environ.get('NO_ANALYSIS'):
        responses = []
    else:
        docs, responses = analyze(docs)
    DF.Flow(output_flow(docs, responses), DF.dump_to_path(os.environ.get('OUT', 'state_comptroller_pm_out'))).process()
