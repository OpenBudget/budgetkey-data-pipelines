"""Shared helpers for the State Comptroller pipelines: HTTP, the budgetkey-files store, and text utilities.

Every source file is kept once in the object store under state-comptroller/..., and results derived from it
(markdown, LLM analyses) are cached next to it, so later runs neither re-download nor re-analyze unchanged files.
"""
import csv
import datetime
import difflib
import json
import logging
import os
import re
import subprocess
import tempfile
import threading
import time
import urllib.parse

import requests

PREFIX = 'state-comptroller'

# Chapters' full markdown goes in one CSV field of the dumped datapackages, far beyond csv's default 128KB limit
csv.field_size_limit(2 ** 31 - 1)
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/126.0 Safari/537.36',
}
MAX_FILE = 400 * 1024 * 1024
CONTENT_TYPES = {
    'pdf': 'application/pdf',
    'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'doc': 'application/msword',
    'md': 'text/markdown; charset=utf-8',
    'json': 'application/json',
}
MAGIC = {'pdf': [b'%PDF'], 'docx': [b'PK\x03\x04'], 'doc': [b'\xd0\xcf\x11\xe0', b'PK\x03\x04']}

# Prod runs from a whitelisted network and fetches gov.il directly. Locally gov.il's Cloudflare blocks us, so a
# local run may tunnel gov.il requests through a whitelisted host: GOVIL_PROXY=socks5h://localhost:1080 with
# `ssh -D 1080 tzabar.obudget.org`.
PROXIED_HOSTS = ('www.gov.il', 'gov.il')


def proxies_for(url):
    proxy = os.environ.get('GOVIL_PROXY')
    if proxy and urllib.parse.urlparse(url).hostname in PROXIED_HOSTS:
        return {'http': proxy, 'https': proxy}
    return None


# Polite pacing: the minimum number of seconds between two requests to the same host, from all threads together.
# Bursts of requests get us blocked (gov.il's Cloudflare especially).
MIN_INTERVAL = {'www.gov.il': 3.0, 'openapi-gc.digital.gov.il': 3.0, 'library.mevaker.gov.il': 1.0}
DEFAULT_INTERVAL = 1.0
_pace_lock = threading.Lock()
_next_slot = {}


def pace(url):
    """Waits for this host's next free slot. Slots are handed out in order, so threads queue up instead of
    bursting."""
    host = urllib.parse.urlparse(url).hostname
    factor = float(os.environ.get('STATE_COMPTROLLER_PACE') or 1)
    interval = MIN_INTERVAL.get(host, DEFAULT_INTERVAL) * factor
    with _pace_lock:
        now = time.monotonic()
        slot = max(now, _next_slot.get(host, 0))
        _next_slot[host] = slot + interval
    if slot > now:
        time.sleep(slot - now)


def http_get(url, retries=3, stream=False, **kwargs):
    """GET with pacing and retries on transient errors; falls back to a browser TLS fingerprint when turned away
    (403/451). Backs off for longer when rate-limited or blocked."""
    headers = dict(HEADERS, **kwargs.pop('headers', {}))
    timeout = kwargs.pop('timeout', 120)
    delay = 10
    for attempt in range(retries + 1):
        pace(url)
        try:
            resp = requests.get(url, headers=headers, timeout=timeout, stream=stream,
                                proxies=proxies_for(url), **kwargs)
            if resp.status_code in (401, 403, 451):
                resp.close()
                pace(url)
                from curl_cffi import requests as browser_requests
                resp = browser_requests.get(url, headers=headers, impersonate='chrome', timeout=timeout,
                                            stream=stream, proxies=proxies_for(url), **kwargs)
            if resp.status_code >= 500 or resp.status_code in (403, 429):
                raise requests.HTTPError('HTTP %s' % resp.status_code)
            return resp
        except Exception as e:
            if attempt == retries:
                raise
            logging.info('GET %s failed (%s), retrying in %ss', url, e, delay)
            time.sleep(delay)
            delay *= 3


def http_json(url, **kwargs):
    resp = http_get(url, **kwargs)
    resp.raise_for_status()
    return resp.json()


# --- object store ---------------------------------------------------------------------------------------------

def storage():
    from datapackage_pipelines_budgetkey.common.object_storage import object_storage
    return object_storage


def stored_url(name):
    return storage().urlfor(urllib.parse.quote(name))


def read_object(name):
    store = storage()
    if store.s3:
        try:
            return store.s3.get_object(Bucket=store.bucket_name, Key=name)['Body'].read()
        except Exception:
            return None
    path = store.get_path(name)
    if os.path.exists(path):
        with open(path, 'rb') as f:
            return f.read()


def read_json(name):
    data = read_object(name)
    if data:
        try:
            return json.loads(data)
        except ValueError:
            return None


def write_text(name, text, ext):
    return storage().write(name, data=text, content_type=CONTENT_TYPES.get(ext))


def write_json(name, obj):
    return write_text(name, json.dumps(obj, ensure_ascii=False), 'json')


def file_ext(url):
    path = urllib.parse.urlparse(url).path
    return os.path.splitext(path)[1].lower().lstrip('.')


def file_basename(url):
    return urllib.parse.unquote(os.path.basename(urllib.parse.urlparse(url).path))


class NotAFile(Exception):
    pass


def download(url, dest):
    ext = file_ext(url)
    resp = http_get(url, stream=True, timeout=300)
    resp.raise_for_status()
    size = 0
    with open(dest, 'wb') as out:
        for chunk in resp.iter_content(1024 * 1024):
            size += len(chunk)
            if size > MAX_FILE:
                raise NotAFile('too-large')
            out.write(chunk)
    with open(dest, 'rb') as f:
        head = f.read(16)
    # Missing files are often answered with an HTML page and a 200
    if ext in MAGIC and not any(head.startswith(m) for m in MAGIC[ext]):
        raise NotAFile('not-a-%s' % ext)
    return size


def backup(url, name):
    """Makes sure the file at url is in the store as `name`. Returns (stored_url, status)."""
    store = storage()
    if store.exists(name):
        return stored_url(name), 'stored'
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'file')
        try:
            download(url, path)
        except NotAFile as e:
            logging.warning('Not a file at %s: %s', url, e)
            return None, str(e)
        except Exception as e:
            logging.warning('Failed to download %s: %s', url, e)
            return None, 'download-failed'
        store.write(name, file_name=path, content_type=CONTENT_TYPES.get(file_ext(url)))
    return stored_url(name), 'downloaded'


def fetch_stored(name, url, dest):
    """Writes the stored copy of a file to dest, downloading (and storing) it first if needed. Returns True on success."""
    data = read_object(name)
    if data is None:
        if url is None or backup(url, name)[0] is None:
            return False
        data = read_object(name)
        if data is None:
            return False
    with open(dest, 'wb') as f:
        f.write(data)
    return True


# --- text ---------------------------------------------------------------------------------------------------

BIDI_MARKS = re.compile('[\u200e\u200f\u202a-\u202e\u2066-\u2069\ufeff]')


def clean_text(s):
    return BIDI_MARKS.sub('', s or '')


def normalize_for_match(s):
    """Letters and digits only, single spaces: robust to the punctuation, quotes and whitespace that differ
    between pdftotext, pandoc and LLM output."""
    s = clean_text(s)
    s = re.sub(r'[\u0591-\u05c7]', '', s)  # niqqud and cantillation
    s = re.sub(r'[^\w]+', ' ', s, flags=re.UNICODE)
    return re.sub(r'\s+', ' ', s).strip().lower()


def fuzzy_contains(haystack_norm, needle, min_ratio=0.8, probe_words=8):
    """Whether `needle` (an LLM quote) appears in `haystack_norm` (normalize_for_match'ed source text), allowing
    for small extraction differences. Probes with a few word windows of the needle rather than the whole quote,
    since quotes can be long and partly paraphrased."""
    words = normalize_for_match(needle).split()
    if not words:
        return False
    if len(words) <= probe_words:
        probes = [' '.join(words)]
    else:
        step = max(1, (len(words) - probe_words) // 3)
        probes = [' '.join(words[i:i + probe_words]) for i in range(0, len(words) - probe_words + 1, step)][:4]
    hits = 0
    for probe in probes:
        if probe in haystack_norm:
            hits += 1
            continue
        # A near match: look for the probe's rarest-looking word and compare the surrounding window
        anchor = max(probe.split(), key=len)
        idx = haystack_norm.find(anchor)
        found = False
        while idx >= 0 and not found:
            start = max(0, idx - len(probe))
            window = haystack_norm[start:idx + len(probe) * 2]
            sm = difflib.SequenceMatcher(None, probe, window, autojunk=False)
            match = sm.find_longest_match(0, len(probe), 0, len(window))
            if match.size >= len(probe) * min_ratio:
                found = True
            idx = haystack_norm.find(anchor, idx + 1)
        hits += found
    return hits >= max(1, (len(probes) + 1) // 2)


HEBREW_DATE = re.compile(r'(\d{1,2})[./](\d{1,2})[./](\d{4})')


def parse_dotted_date(s):
    import datetime
    m = HEBREW_DATE.search(s or '')
    if m:
        d, mo, y = map(int, m.groups())
        try:
            return datetime.date(y, mo, d)
        except ValueError:
            return None
    return None


# Report codes such as 73א, 65ג, 61 ב, 72א' - the annual-report volume number and part letter.
REPORT_CODE = re.compile(r'(?<!\d)([4-9]\d)\s*([אבגד])(?![א-ת])')


def report_code(*texts):
    for t in texts:
        m = REPORT_CODE.search(clean_text(t or '').replace("'", '').replace('׳', ''))
        if m:
            return m.group(1) + m.group(2)
    return None


def page_image(pdf_path, page, tmp):
    """A PNG of one page of a PDF (Gemini blocks some PDF pages that it reads fine as images)."""
    prefix = os.path.join(tmp, 'page-%d' % page)
    subprocess.run(['pdftoppm', '-f', str(page), '-l', str(page), '-r', '150', '-png', '-singlefile', pdf_path,
                    prefix], check=True, capture_output=True, timeout=300)
    with open(prefix + '.png', 'rb') as f:
        return f.read()


def pdf_slice(path, first, last, dest):
    """Writes pages first..last (1-based, inclusive) of the PDF at path to dest, with poppler's tools."""
    import glob
    import shutil
    import subprocess
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run(['pdfseparate', '-f', str(first), '-l', str(last), path, os.path.join(tmp, 'p-%06d.pdf')],
                       check=True, capture_output=True, timeout=600)
        parts = sorted(glob.glob(os.path.join(tmp, 'p-*.pdf')))
        if len(parts) == 1:
            shutil.copy(parts[0], dest)
        else:
            subprocess.run(['pdfunite'] + parts + [dest], check=True, capture_output=True, timeout=600)
    return dest


def report_codes(*texts):
    """Every report code named in the texts, in order: 'מדוחות 64 א'-ב'-ג' + 65 א'' -> ['64א', '65א']."""
    codes = []
    for t in texts:
        for m in REPORT_CODES.finditer(clean_text(t or '').replace("'", '').replace('׳', '')):
            for letter in re.findall('[אבגד]', m.group(2)):
                code = m.group(1) + letter
                if code not in codes:
                    codes.append(code)
    return codes


REPORT_CODES = re.compile(r'(?<!\d)([4-9]\d)\s*([אבגד](?:\s*[-,+]\s*[אבגד](?![א-ת]))*)(?![א-ת])')


def typed_resource(rows, name, fields, key):
    """A resource with an explicit schema, even when there are no rows (a placeholder row, filtered out, gives
    dataflows something to build the schema from)."""
    import dataflows as DF
    names = [f for f, _ in fields]
    rows = list(rows)

    def gen():
        if not rows:
            yield {f: None for f in names}
        for r in rows:
            yield {f: r.get(f) for f in names}
    steps = [gen(), DF.update_resource(-1, name=name, path=name + '.csv')]
    for f, t in fields:
        steps.append(DF.set_type(f, type=t, resources=name, **({'format': 'any'} if t == 'date' else {})))
    if not rows:
        steps.append(DF.filter_rows(lambda row: False, resources=name))
    steps.append(DF.set_primary_key(key, resources=name))
    return DF.Flow(*steps)


HEBREW_MONTHS = {'ינואר': 1, 'פברואר': 2, 'מרס': 3, 'מרץ': 3, 'אפריל': 4, 'מאי': 5, 'יוני': 6, 'יולי': 7,
                 'אוגוסט': 8, 'ספטמבר': 9, 'אוקטובר': 10, 'נובמבר': 11, 'דצמבר': 12}
_MONTH = '|'.join(sorted(HEBREW_MONTHS, key=len, reverse=True))
_DATE_PATTERNS = [
    # 2008-11-05, 2008-11
    (re.compile(r'(?<!\d)(\d{4})-(\d{1,2})(?:-(\d{1,2}))?(?!\d)'), lambda m: (m[1], m[2], m[3])),
    # 5.11.2008, 5/11/08
    (re.compile(r'(?<!\d)(\d{1,2})[./](\d{1,2})[./](\d{4}|\d{2})(?![\d./])'), lambda m: (m[3], m[2], m[1])),
    # 11.2008, 11/2008
    (re.compile(r'(?<![\d./])(\d{1,2})[./](\d{4})(?!\d)'), lambda m: (m[2], m[1], None)),
    # 5 בנובמבר 2008, נובמבר 2008, מרץ, 2006
    (re.compile(r'(?:(?<!\d)(\d{1,2})\s*)?(?<![א-ת])ב?-?(' + _MONTH + r')(?![א-ת])[\s,]*(\d{4})(?!\d)'),
     lambda m: (m[3], str(HEBREW_MONTHS[m[2]]), m[1])),
    # a year alone
    (re.compile(r'(?<!\d)((?:19|20)\d{2})(?!\d)'), lambda m: (m[1], None, None)),
]


def normalize_date(text):
    """The first date in a free-text date ("מרס 2006", "12 בספטמבר 2006", "5.11.08", "2008-11"), as YYYY-MM-DD,
    YYYY-MM or YYYY (as precise as the text), or None. Text without a year ("לא צוין", "ספטמבר") is None."""
    if not text:
        return None
    text = str(text)
    for pattern, parts in _DATE_PATTERNS:
        for m in pattern.finditer(text):
            year, month, day = parts(m)
            year = int(year)
            if year < 100:
                year += 2000 if year <= 40 else 1900
            if not 1948 <= year <= 2100:
                continue
            if month is None:
                return '%04d' % year
            month = int(month)
            if not 1 <= month <= 12:
                continue
            if day:
                try:
                    return datetime.date(year, month, int(day)).isoformat()
                except ValueError:
                    pass
            return '%04d-%02d' % (year, month)
    return None


def body_key(name):
    """A body's name with spelling noise removed (punctuation, spaces, doubled yod/vav), for matching variants."""
    s = re.sub(r'[^א-תa-zA-Z]', '', name or '')
    return s.replace('יי', 'י').replace('וו', 'ו')


def canonical_bodies(names, mapping):
    """Body names with each one replaced by its canonical name (`mapping`: name -> canonical, from the taxonomy
    step's body_names) and duplicates dropped, keeping the first occurrence's order."""
    out, seen = [], set()
    for n in names or []:
        if not n:
            continue
        c = mapping.get(n.strip(), n.strip())
        k = body_key(c)
        if k and k not in seen:
            seen.add(k)
            out.append(c)
    return out
