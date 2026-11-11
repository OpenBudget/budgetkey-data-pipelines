"""Indexer step (state_comptroller_audits): the page of each earlier report an audit names.

An audit's `earlier_reports` are the comptroller's own references, `{report, year, title, relation}`. The
related-items linker resolves each one to a chapter and records the result in `related_items_mentions`
(mention_kind 'comptroller_report', with the reference in `details`). This step copies that result back onto the
reference as `doc_id`: the chapter's doc id when it was linked (it then has a page), else null. Not resolved, not
indexed (before 2000, or not in Hebrew) and ambiguous references all get null.

Parameters: `resource`, `doc-id-pattern` (as for add_related_items).

The table is read once per run (`DPP_DB_ENGINE`). If it can't be read, every `doc_id` is null: indexing never fails
over links.
"""
import json
import logging
import os

import dataflows as DF

TABLE = 'related_items_mentions'
FIELDS = ['source_doc_id', 'target_doc_id', 'details']


def ref_key(ref):
    """A reference's identity: the same (report, year, title) the linker was given."""
    year = ref.get('year')
    try:
        year = int(year) if year not in (None, '') else None
    except (TypeError, ValueError):
        year = None
    return (str(ref.get('report') or '').strip(), year, str(ref.get('title') or '').strip())


def _details(v):
    if isinstance(v, str):
        try:
            v = json.loads(v)
        except ValueError:
            return {}
    return v if isinstance(v, dict) else {}


def load_resolved(engine=None):
    """{source doc id: {ref key: target doc id}} for the linked references, or {} if the table can't be read."""
    engine = engine or os.environ.get('DPP_DB_ENGINE')
    if not engine:
        logging.warning('add_earlier_report_ids: DPP_DB_ENGINE is not set, earlier reports get no doc_id')
        return {}
    try:
        from sqlalchemy import create_engine, text
        stmt = text("select {} from {} where mention_kind = 'comptroller_report' and status = 'linked' "
                    "and target_doc_id is not null".format(', '.join(FIELDS), TABLE))
        with create_engine(engine).connect() as conn:
            rows = [dict(zip(FIELDS, r)) for r in conn.execute(stmt)]
    except Exception:
        logging.exception('add_earlier_report_ids: could not read %s, earlier reports get no doc_id', TABLE)
        return {}
    return resolved_index(rows)


def resolved_index(rows):
    out = {}
    for r in rows:
        out.setdefault(r['source_doc_id'], {})[ref_key(_details(r['details']))] = r['target_doc_id']
    return out


def with_doc_ids(refs, resolved):
    out = []
    for ref in refs or []:
        if isinstance(ref, dict):
            ref = dict(ref, doc_id=resolved.get(ref_key(ref)))
        out.append(ref)
    return out


def add_ids(resource, pattern, index):
    def func(rows):
        if rows.res.name != resource:
            yield from rows
            return
        for row in rows:
            try:
                doc_id = pattern.format(**row)
            except (KeyError, IndexError, ValueError):
                doc_id = None
            row['earlier_reports'] = with_doc_ids(row.get('earlier_reports'), index.get(doc_id, {}))
            yield row
    return func


def flow(parameters, *_):
    resource = parameters['resource']
    index = load_resolved(parameters.get('db-engine'))
    return DF.Flow(
        add_ids(resource, parameters['doc-id-pattern'], index),
        DF.update_resource(resource, **{'dpp:streaming': True}),
    )
