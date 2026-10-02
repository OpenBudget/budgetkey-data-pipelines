"""Runs read-only SQL over the simpledb `*_data` tables (the same ones the MCP serves).

Inside the pipelines the tables are in DPP_DB_ENGINE. Locally, with no engine set, the
public query API is used instead, so the analysis code can be developed against live data.
"""
import os

import requests

API_URL = os.environ.get('BUDGETKEY_QUERY_API', 'https://next.obudget.org/api/query')
PAGE_SIZE = 1000

_engine = None


def _get_engine():
    global _engine
    if _engine is None:
        from sqlalchemy import create_engine
        _engine = create_engine(os.environ['DPP_DB_ENGINE'])
    return _engine


def use_db():
    url = os.environ.get('DPP_DB_ENGINE', '')
    return url.startswith('postgres')


def query(sql, max_rows=10000):
    if use_db():
        from sqlalchemy import text
        with _get_engine().connect() as conn:
            result = conn.execute(text(sql))
            return [dict(r._mapping) for r in result.fetchmany(max_rows)]
    rows = []
    page = 0
    while len(rows) < max_rows:
        resp = requests.get(API_URL, params=dict(query=sql, page_size=PAGE_SIZE, page=page), timeout=120)
        resp.raise_for_status()
        data = resp.json()
        if not data.get('success', True):
            raise RuntimeError('Query failed: {}'.format(data))
        rows.extend(data['rows'])
        page += 1
        if page >= data.get('pages', 1):
            break
    return rows[:max_rows]
