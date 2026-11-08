"""Read-only access to the budgetkey DB, for building target indexes.

In the pipelines (DPP_DB_ENGINE set) queries go straight to the DB. Elsewhere they go through the public query API's
CSV export (next.obudget.org/api/download) and are cached on disk under RELATED_ITEMS_CACHE, keyed by the SQL, so
local runs and repeated experiments don't re-download the entity list. Values from the API come back as strings;
index builders coerce what they need.
"""
import base64
import csv
import hashlib
import io
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

API = os.environ.get('BUDGETKEY_API', 'https://next.obudget.org/api')
CACHE_DIR = Path(os.environ.get('RELATED_ITEMS_CACHE') or '/var/related-items-cache')

_engine = None


def _db():
    global _engine
    if _engine is None:
        from sqlalchemy import create_engine
        _engine = create_engine(os.environ['DPP_DB_ENGINE'])
    return _engine


def _via_api(sql, columns):
    url = '{}/download?{}'.format(API, urllib.parse.urlencode(dict(
        query=base64.b64encode(sql.encode('utf8')).decode('ascii'),
        headers=';'.join(columns),
        format='csv',
    )))
    request = urllib.request.Request(url, headers={'User-Agent': 'budgetkey-related-items'})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=1800) as response:
                return response.read().decode('utf8')
        except urllib.error.HTTPError as e:
            # The export names its columns after `headers`, so they must match the SELECT's column names.
            if e.code < 500 or attempt == 2:
                raise
            logging.info('API error %s, retrying', e.code)
            time.sleep(10 * (attempt + 1))


def query(sql, columns):
    """Runs a read-only SELECT and returns a list of dicts keyed by `columns`, which must be the SELECT's column
    names in order (alias computed columns: "count(*) as n")."""
    if os.environ.get('DPP_DB_ENGINE'):
        from sqlalchemy import text
        with _db().connect() as conn:
            return [dict(zip(columns, row)) for row in conn.execute(text(sql))]
    csv.field_size_limit(1 << 30)
    path = CACHE_DIR / 'sql' / (hashlib.sha256(sql.encode('utf8')).hexdigest()[:24] + '.csv')
    if not path.exists():
        logging.info('Downloading %s', sql[:120])
        content = _via_api(sql, columns)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix('.tmp')
        tmp.write_text(content, encoding='utf8')
        tmp.rename(path)
    with path.open(encoding='utf8') as f:
        rows = list(csv.DictReader(io.StringIO(f.read())))
    for row in rows:
        for k, v in row.items():
            if v == '':
                row[k] = None
    return rows
