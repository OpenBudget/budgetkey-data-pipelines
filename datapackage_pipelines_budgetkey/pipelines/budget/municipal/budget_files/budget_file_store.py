"""Keeps a copy of every municipal budget file in the budgetkey-files object store and analyzes it once.

Objects live at municipal-budgets/<authority_code>/<url hash>.<ext>, with the analysis cached next to them as
<object>.analysis.json and the extracted table rows as <object>.rows.csv. A file is only downloaded from the authority's site once; later runs reuse the stored copy,
and re-analyze it only when the analyzer changes.
"""
import csv
import hashlib
import io
import json
import logging
import os
import tempfile

import requests

import budget_file_analysis as analysis

PREFIX = 'municipal-budgets'
MAX_FILE = 150 * 1024 * 1024
CONTENT_TYPES = {
    'pdf': 'application/pdf',
    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'xls': 'application/vnd.ms-excel',
    'csv': 'text/csv',
}
MAGIC = {'pdf': [b'%PDF'], 'xlsx': [b'PK\x03\x04'], 'xls': [b'\xd0\xcf\x11\xe0', b'PK\x03\x04']}
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/126.0 Safari/537.36',
}

ANALYSIS_FIELDS = ['table_kind', 'ready_to_process', 'n_tables', 'n_rows', 'value_columns', 'expected_columns',
                   'income_sign', 'expense_sign', 'units', 'balance', 'issues']


def storage():
    from datapackage_pipelines_budgetkey.common.object_storage import object_storage
    return object_storage


def object_name(f):
    digest = hashlib.sha1(f['file_url'].encode('utf-8')).hexdigest()[:16]
    return '%s/%s/%s.%s' % (PREFIX, f['authority_code'], digest, f['format'])


def read_object(store, name):
    if store.s3:
        try:
            return store.s3.get_object(Bucket=store.bucket_name, Key=name)['Body'].read()
        except Exception:
            return None
    path = store.get_path(name)
    if os.path.exists(path):
        with open(path, 'rb') as f:
            return f.read()


class NotAFile(Exception):
    pass


def download(url, fmt, dest):
    resp = requests.get(url, headers=HEADERS, timeout=120, verify=False, stream=True)
    resp.raise_for_status()
    size = 0
    with open(dest, 'wb') as out:
        for chunk in resp.iter_content(1024 * 1024):
            size += len(chunk)
            if size > MAX_FILE:
                raise NotAFile('too-large')
            out.write(chunk)
    with open(dest, 'rb') as f:
        head = f.read(512)
    # Sites often answer a missing file with an HTML page and a 200
    if fmt in MAGIC and not any(head.startswith(m) for m in MAGIC[fmt]):
        raise NotAFile('not-a-%s' % fmt)
    if fmt == 'csv' and head.lstrip().lower().startswith((b'<!doctype', b'<html')):
        raise NotAFile('not-a-csv')


def process_file(f):
    """Adds storage and analysis fields to a file row; returns (file row, table rows)."""
    store = storage()
    name = object_name(f)
    f.update(stored_url=store.urlfor(name), file_size=None, analysis_status=None,
             **{k: None for k in ANALYSIS_FIELDS})

    cached = read_object(store, name + '.analysis.json')
    if cached:
        cached = json.loads(cached)
        if cached.get('version') == analysis.ANALYZER_VERSION and cached.get('fiscal_year') == f['fiscal_year']:
            return apply(f, cached)

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'file.' + f['format'])
        data = read_object(store, name)
        try:
            if data is not None:
                with open(path, 'wb') as out:
                    out.write(data)
            else:
                download(f['file_url'], f['format'], path)
                store.write(name, file_name=path, content_type=CONTENT_TYPES.get(f['format']))
        except NotAFile as e:
            f['analysis_status'] = str(e)
            return f, []
        except Exception as e:
            f['analysis_status'] = 'download-failed'
            logging.info('Failed to download %s: %s', f['file_url'], e)
            return f, []

        result = dict(version=analysis.ANALYZER_VERSION, fiscal_year=f['fiscal_year'], size=os.path.getsize(path))
        try:
            result['summary'], result['tables'] = analysis.analyze_file(path, f['format'], f['fiscal_year'])
            result['status'] = 'ok'
        except Exception as e:
            logging.warning('Failed to analyze %s: %s', f['file_url'], e)
            result.update(summary={}, tables=[], status='analysis-failed')
    rows = rows_csv(result['tables'])
    result['has_rows'] = rows is not None
    if rows is not None:
        store.write(name + '.rows.csv', data=rows, content_type='text/csv')
    store.write(name + '.analysis.json', data=json.dumps(result, ensure_ascii=False), content_type='application/json')
    return apply(f, result)


ROW_FIELDS = ['table_index', 'sheet', 'row', 'code', 'title', 'column', 'kind', 'year', 'header', 'value']


def rows_csv(tables):
    """The extracted table rows in long format (one line per value), or None when there are none."""
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(ROW_FIELDS)
    n = 0
    for index, t in enumerate(tables):
        for row in t.pop('rows', None) or []:
            for column_no, (column, value) in enumerate(zip(t['value_columns'], row['values'])):
                if value is None:
                    continue
                writer.writerow([index, t['sheet'], row['row'], row['code'], row['title'], column_no,
                                 column['kind'], column['year'], column['header'],
                                 int(value) if float(value).is_integer() else value])
                n += 1
    return out.getvalue() if n else None


def read_rows(f):
    """The extracted rows of a file analyzed earlier, as dicts."""
    data = read_object(storage(), object_name(f) + '.rows.csv')
    if not data:
        return []
    return list(csv.DictReader(io.StringIO(data.decode('utf-8') if isinstance(data, bytes) else data)))


def apply(f, result):
    f.update(file_size=result.get('size'), analysis_status=result['status'],
             **{k: result['summary'].get(k) for k in ANALYSIS_FIELDS})
    tables = []
    for index, t in enumerate(result['tables']):
        if t['kind'] == 'none':
            continue
        tables.append(dict(
            table_index=index, duplicate_columns=[n for n, c in enumerate(t['value_columns']) if c.get('duplicate')],
            authority_code=f['authority_code'], name=f['name'], fiscal_year=f['fiscal_year'],
            file_url=f['file_url'], stored_url=f['stored_url'], format=f['format'],
            sheet=t['sheet'], kind=t['kind'], header_row=t['header_row'], first_col=t['first_col'],
            code_col=None if t['code_col'] is None else str(t['code_col']), title_col=t['title_col'],
            n_rows=t['n_rows'], value_columns=json.dumps(t['value_columns'], ensure_ascii=False),
            income_sign=t['income_sign'], expense_sign=t['expense_sign'], units=t['units'],
            units_source=t['units_source'], expected_columns=t['expected_columns'], ready=t['ready'],
            issues=','.join(t['issues']) or None,
        ))
    return f, tables
