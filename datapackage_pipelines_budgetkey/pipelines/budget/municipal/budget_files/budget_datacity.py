"""Loads our municipal budget lines into datacity's muni_budgets, one pipeline per authority and fiscal year.

For each authority-year with phases nobody else loads (see datacity_export), this:
1. writes a snapshot to the budgetkey-files bucket: municipal-budgets/datacity/<code>/<year>.csv, the rows,
   and <year>.json, where each phase came from;
2. creates or updates the datacity pipeline budgetkey-muni-<code>-<year>, which reads that CSV;
3. starts it when the snapshot or the pipeline changed, or when it has never run.

A snapshot is never emptied because a file disappeared from an authority's site: a phase the previous
snapshot had and this run lacks is carried over, unless another datacity pipeline now loads it. Pipelines
other than budgetkey-muni-* are only read, never changed.

DATACITY_API_KEY enables step 2 and 3; without it the snapshots are still written. DATACITY_DRY_RUN=1
writes nothing anywhere.
"""
import csv
import datetime
import hashlib
import io
import json
import logging
import os
import sys
import time
from collections import Counter, defaultdict

import dataflows as DF
import requests

import datacity_export as X

OUT_PATH = '/var/datapackages/budget/municipal/budget_files_datacity'
SCRAPED = os.environ.get('MUNICIPAL_BUDGET_FILES_DATAPACKAGE',
                         '/var/datapackages/budget/municipal/budget_files/datapackage.json')
LAMAS = os.environ.get('LAMAS_MUNICIPAL_DATAPACKAGE', '/var/datapackages/lamas-municipal-data/datapackage.json')
PREVIOUS = os.environ.get('MUNICIPAL_BUDGET_DATACITY_PREVIOUS', OUT_PATH + '/datapackage.json')
DATACITY_DB = os.environ.get('DATACITY_DB', 'postgresql://readonly:readonly@db.datacity.org.il:5432/datasets')
DATACITY_API = 'https://app.datacity.org.il/api/'
DRY_RUN = bool(os.environ.get('DATACITY_DRY_RUN'))
PREFIX = 'municipal-budgets/datacity'
PIPELINE_PREFIX = 'budgetkey-muni-'
TEMPLATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'datacity_pipeline.json')

csv.field_size_limit(sys.maxsize)


# --- inputs -------------------------------------------------------------------

def resource_rows(datapackage, name):
    with open(datapackage) as f:
        dp = json.load(f)
    path = next(r['path'] for r in dp['resources'] if r['name'] == name)
    with open(os.path.join(os.path.dirname(datapackage), path), newline='') as f:
        yield from csv.DictReader(f)


def lamas_names():
    """CBS symbol -> the name datacity looks the authority up by."""
    rows = DF.Flow(DF.load(LAMAS), DF.select_fields(['symbol_municipality_2015', 'name_municipality'])).results()[0][0]
    return {int(r['symbol_municipality_2015']): r['name_municipality'] for r in rows}


def datacity_coverage():
    """(CBS symbol, year, phase) loaded by datacity pipelines other than ours. Fails loudly: without it,
    we'd load phases twice."""
    from sqlalchemy import create_engine, text
    engine = create_engine(DATACITY_DB)
    covered = defaultdict(set)
    with engine.connect() as conn:
        result = conn.execute(text(
            'select "municipality-code", "fiscal-year", phase, _source from muni_budgets '
            'where _source not like :ours group by 1, 2, 3, 4'), dict(ours=storage().urlfor(PREFIX) + '/%'))
        for code, year, phase, source in result:
            try:
                covered[(int(code), int(year), phase)].add(source)
            except (TypeError, ValueError):
                continue
    if not covered:
        raise RuntimeError('muni_budgets has no rows from datacity\'s own pipelines; not loading anything')
    return covered


def previous_loads():
    try:
        return {(int(r['authority_code']), int(r['fiscal_year'])): r for r in resource_rows(PREVIOUS, 'datacity_loads')}
    except (OSError, StopIteration, KeyError):
        return {}


# --- storage --------------------------------------------------------------------

def storage():
    from datapackage_pipelines_budgetkey.common.object_storage import object_storage
    return object_storage


def read_snapshot(code, year):
    import budget_file_store
    store = storage()
    meta = budget_file_store.read_object(store, '%s/%d/%d.json' % (PREFIX, code, year))
    data = budget_file_store.read_object(store, '%s/%d/%d.csv' % (PREFIX, code, year))
    if not meta or not data:
        return None, []
    rows = list(csv.DictReader(io.StringIO(data.decode('utf-8'))))
    return json.loads(meta), rows


def csv_text(rows):
    out = io.StringIO()
    w = csv.DictWriter(out, X.FIELDS, lineterminator='\n')
    w.writeheader()
    w.writerows(rows)
    return out.getvalue()


# --- datacity -------------------------------------------------------------------

class Datacity:
    def __init__(self, key):
        self.headers = {'Authorization': 'Bearer ' + key}

    def call(self, method, path, **kw):
        resp = requests.request(method, DATACITY_API + path, headers=self.headers, timeout=120, **kw)
        resp.raise_for_status()
        return resp.json()

    def pipelines(self):
        return {p['id']: p for p in self.call('GET', 'pipelines')['result'] if p['id'].startswith(PIPELINE_PREFIX)}

    def save(self, record):
        ret = self.call('POST', 'pipeline', json=record)
        if not ret.get('success'):
            raise RuntimeError('datacity refused to save %s: %s' % (record['id'], str(ret)[:200]))
        return ret.get('created')

    def start(self, pid):
        # A new pipeline's DAG exists only once Airflow re-parses the pipeline list, about a minute later
        try:
            return self.call('POST', 'pipeline/start/' + pid).get('result') is not None
        except requests.HTTPError:
            return False


def pipeline_record(code, year, name, csv_url, json_url):
    with open(TEMPLATE) as f:
        config = json.load(f)
    config['source']['path'] = csv_url
    return dict(
        id='%s%d-%d' % (PIPELINE_PREFIX, code, year),
        name='%s//תקציב//%d (מפתח התקציב)' % (name, year),
        kind='dgp_kind', schedule='manual', private=False,
        description='נטען אוטומטית על ידי מפתח התקציב מקבצי התקציב שבאתר הרשות. המקורות: %s' % json_url,
        params=dict(dgpConfig=config),
    )


# --- the load -------------------------------------------------------------------

def build():
    covered = datacity_coverage()
    names = lamas_names()
    previous = previous_loads()
    files = {f['file_url']: f for f in resource_rows(SCRAPED, 'budget_files')}
    agg = X.Aggregates()
    for line in resource_rows(SCRAPED, 'budget_lines'):
        agg.add(line)
    chosen = X.choose(agg.candidates(), covered)
    logging.info('%d authority-years with phases to load; %d phases loaded by datacity\'s own pipelines',
                 len(chosen), len(covered))

    needed = {(p['file_url'], p['kind'], p['direction']) for phases in chosen.values()
              for parts in phases.values() for p in parts}
    lines = defaultdict(list)
    for line in resource_rows(SCRAPED, 'budget_lines'):
        key = (line['file_url'], line.get('column_kind'), line.get('direction'))
        if key in needed and line.get('doc_type') in X.REGULAR_BUDGET and line.get('amount') not in (None, ''):
            lines[key].append(X.line_of(line))

    loads = []
    for code, year in sorted(set(chosen) | set(previous)):
        symbol = X.cbs_symbol(code)
        name = names.get(symbol)
        if not name:
            logging.warning('Authority %d is not in the LAMAS list datacity looks names up in; skipping', code)
            continue
        rows, phases = [], []
        for phase, parts in sorted(chosen.get((code, year), {}).items(), key=lambda x: X.PHASE_ORDER.index(x[0])):
            r, summary = X.phase_rows(parts, lines, phase, year, name)
            for f in summary['files']:
                f['copy'] = files.get(f['url'], {}).get('stored_url')
            rows += r
            phases.append(dict(summary, source='scraped'))
        old_meta, old_rows = read_snapshot(code, year) if (code, year) in previous else (None, [])
        for old in (old_meta or {}).get('phases', []):
            if old['phase'] in {p['phase'] for p in phases} or (symbol, year, old['phase']) in covered:
                continue
            carried = [dict(r, **{'municipality-name': name}) for r in old_rows if r['phase'] == old['phase']]
            rows += carried
            phases.append(dict(old, source='carried', carried_since=old.get('carried_since') or old_meta.get('updated')))
        loads.append(dict(code=code, year=year, name=name, rows=X.sort_rows(rows), phases=phases,
                          datacity=sorted(p for p in X.PHASE_ORDER if (symbol, year, p) in covered),
                          previous=old_meta))
    return loads


def publish(loads):
    store = storage()
    key = os.environ.get('DATACITY_API_KEY')
    datacity = Datacity(key) if key else None
    existing = datacity.pipelines() if datacity else {}
    if not datacity:
        logging.warning('DATACITY_API_KEY is not set: writing snapshots only, not touching datacity pipelines')
    to_start, out = [], []
    for load in loads:
        code, year = load['code'], load['year']
        body = csv_text(load['rows'])
        digest = hashlib.sha256(body.encode('utf-8')).hexdigest()
        old = load['previous'] or {}
        changed = digest != old.get('csv_sha256')
        csv_name, json_name = '%s/%d/%d.csv' % (PREFIX, code, year), '%s/%d/%d.json' % (PREFIX, code, year)
        csv_url, json_url = store.urlfor(csv_name), store.urlfor(json_name)
        meta = dict(authority_code=code, authority=load['name'], fiscal_year=year, csv_url=csv_url,
                    csv_sha256=digest, rows=len(load['rows']), phases=load['phases'],
                    loaded_by_datacity=load['datacity'],
                    updated=datetime.datetime.utcnow().isoformat(timespec='seconds') if changed else old.get('updated'))
        if changed and not DRY_RUN:
            store.write(csv_name, data=body, content_type='text/csv; charset=utf-8')
            store.write(json_name, data=json.dumps(meta, ensure_ascii=False, indent=1),
                        content_type='application/json')
        action = 'changed' if changed else 'unchanged'
        record = pipeline_record(code, year, load['name'], csv_url, json_url)
        if datacity and (load['rows'] or record['id'] in existing):
            current = existing.get(record['id'])
            stale = current is None or any(current.get(k) != record[k] for k in ('name', 'description', 'params'))
            if stale and not DRY_RUN:
                datacity.save(record)
            if stale or changed or (current or {}).get('status', {}).get('status') == 'didnt-run':
                to_start.append(record['id'])
            action = 'created' if current is None else 'updated' if stale else action
        out.append(dict(
            authority_code=code, name=load['name'], fiscal_year=year, rows=len(load['rows']), action=action,
            phases=[p['phase'] for p in load['phases']],
            carried_phases=[p['phase'] for p in load['phases'] if p['source'] == 'carried'],
            datacity_phases=load['datacity'], csv_url=csv_url, sources_url=json_url,
            pipeline_id=record['id'] if datacity else None,
            pipeline_url='https://app.datacity.org.il/dashboard/pipelines/' + record['id'] if datacity else None,
        ))
    if to_start and not DRY_RUN:
        start(datacity, to_start)
    logging.info('Loads: %s', dict(Counter(o['action'] for o in out)))
    return out


def start(datacity, ids):
    """Airflow runs them one at a time; new DAGs need a parse cycle before they can be triggered, which took
    over 5 minutes for a first batch of 350."""
    pending = list(ids)
    for attempt in range(16):
        if attempt:
            time.sleep(120)
        pending = [pid for pid in pending if not datacity.start(pid)]
        if not pending:
            break
    logging.info('Started %d datacity pipelines; %d could not be started yet and will be on the next run',
                 len(ids) - len(pending), len(pending))


LOAD_FIELDS = ['authority_code', 'name', 'fiscal_year', 'rows', 'action', 'phases', 'carried_phases',
               'datacity_phases', 'csv_url', 'sources_url', 'pipeline_id', 'pipeline_url']


def flow(*_):
    out = publish(build())
    return DF.Flow(
        [{k: o.get(k) for k in LOAD_FIELDS} for o in out] or [{k: None for k in LOAD_FIELDS}],
        DF.update_resource(-1, name='datacity_loads', path='datacity_loads.csv'),
        DF.filter_rows(lambda row: row['authority_code'] is not None),
        DF.set_type('(authority_code|fiscal_year|rows)', type='integer'),
        DF.set_type('(phases|carried_phases|datacity_phases)', type='array'),
        DF.set_primary_key(['authority_code', 'fiscal_year']),
        DF.update_resource(-1, **{'dpp:streaming': True}),
        DF.dump_to_path(OUT_PATH),
        DF.dump_to_sql(dict(municipal_budget_datacity={'resource-name': 'datacity_loads'})),
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    OUT_PATH = os.environ.get('OUT', 'municipal_budget_datacity_out')
    loads = build()
    out = publish(loads)
    DF.Flow([{k: o.get(k) for k in LOAD_FIELDS} for o in out], DF.dump_to_path(OUT_PATH)).process()
