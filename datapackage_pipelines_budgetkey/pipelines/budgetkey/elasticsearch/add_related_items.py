"""Indexer step: a doc's links from the `related_items` table (see pipelines/related_items/README.md).

Adds three fields to each row of `resource`:
- `related_items`: the row as a source - what it is about. One entry per target, its links aggregated over the
  document's parts (a comptroller chapter's findings): the strongest relation and confidence, all relations, the parts.
  Sorted by target kind, then confidence, then title.
- `mentioned_in`: the row as a target - the documents about it. One entry per source document, newest first, at most
  `max-mentioned-in`; `mentioned_in_count` counts them all. Structural links (a budget chapter's owning body) come
  first.
- `mentioned_in_count`.

Parameters:
- `resource`: the doc type's resource;
- `doc-id-pattern`: the type's doc-id pattern, as the generator builds it (`org/{kind}/{id}`, `budget/{code}/{year}`);
- `code-field` (budget): also list, in `mentioned_in`, links to other connected items with the row's code - the
  same code in years the item didn't cover - marked `same_item: false` and with their year, after the item's own;
- `max-mentioned-in` (20), `min-confidence` (0).

The table is read once per run (`DPP_DB_ENGINE`), for the links whose source or target has this type's doc-id prefix,
and held as finished entries keyed by doc id, so a row costs a dict lookup. If the table can't be read the fields stay
empty: indexing never fails over links. The output depends only on the table's content (manage-revisions hashes it).
"""
import logging
import os

import dataflows as DF

TABLE = 'related_items'
FIELDS = ['source_doc_id', 'source_kind', 'source_part_id', 'source_title', 'source_date', 'target_doc_id',
          'target_kind', 'target_title', 'target_year', 'target_code', 'relation', 'excerpt', 'confidence']
# Same order as the linker's RELATION_ORDER (kept here so indexing doesn't import the linker).
RELATION_ORDER = ['budget_owner', 'audited', 'subject', 'follows_up', 'amends', 'cancels', 'cites', 'responsible',
                  'funded', 'contractor', 'mentioned']
# Relations that state what a record is rather than where it's discussed (a chapter's owning body): first in
# `mentioned_in`, ahead of the dated mentions, so the cap never drops them.
STRUCTURAL = {'budget_owner'}
KIND_ORDER = ['entities', 'gov_decisions', 'state_comptroller_audits', 'state_comptroller_issues', 'analysis', 'budget',
              'support_programs', 'activities', 'tenders', 'contract-spending', 'supports']

RELATED_SCHEMA = dict(fields=[
    dict(name='doc_id', type='string'),
    dict(name='kind', type='string'),
    dict(name='title', type='string'),
    dict(name='relation', type='string'),
    dict(name='relations', type='array', **{'es:itemType': 'string'}),
    dict(name='confidence', type='number'),
    dict(name='parts', type='array', **{'es:itemType': 'string'}),
    dict(name='year', type='integer'),
    dict(name='code', type='string'),
])
MENTIONED_SCHEMA = dict(fields=[
    dict(name='doc_id', type='string'),
    dict(name='kind', type='string'),
    dict(name='title', type='string'),
    dict(name='date', type='string'),
    dict(name='relation', type='string'),
    dict(name='confidence', type='number'),
    dict(name='excerpt', type='string'),
    dict(name='part_id', type='string'),
    dict(name='parts', type='array', **{'es:itemType': 'string'}),
    dict(name='year', type='integer'),
    dict(name='same_item', type='boolean'),
])


def doc_id_prefix(pattern):
    """The literal start of a doc-id pattern: 'org/' for 'org/{kind}/{id}'."""
    prefix = pattern.split('{', 1)[0]
    assert prefix.endswith('/'), 'doc-id-pattern should start with "<type>/": {}'.format(pattern)
    return prefix


def load_links(prefix, engine=None):
    """The table's rows whose source or target doc id starts with prefix, or None if they can't be read."""
    engine = engine or os.environ.get('DPP_DB_ENGINE')
    if not engine:
        logging.warning('add_related_items: DPP_DB_ENGINE is not set, related items are left empty')
        return None
    try:
        from sqlalchemy import create_engine, text
        stmt = text('select {} from {} where source_doc_id like :p or target_doc_id like :p'.format(
            ', '.join(FIELDS), TABLE))
        with create_engine(engine).connect() as conn:
            # ('_' in the prefix matches any character; rows are filtered by prefix again in RelatedItems.)
            return [dict(zip(FIELDS, r)) for r in conn.execute(stmt, dict(p=prefix + '%'))]
    except Exception:
        logging.exception('add_related_items: could not read %s, related items are left empty', TABLE)
        return None


def _str(v):
    return None if v is None or v == '' else str(v)


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _date(v):
    return str(v)[:10] if v not in (None, '') else None


def _confidence(v):
    try:
        return round(float(v), 3)
    except (TypeError, ValueError):
        return 0.0


def _relation_rank(r):
    return RELATION_ORDER.index(r) if r in RELATION_ORDER else len(RELATION_ORDER)


def _kind_rank(k):
    return (KIND_ORDER.index(k), '') if k in KIND_ORDER else (len(KIND_ORDER), k or '')


def _stronger(a, b):
    """Whether link a beats link b as the one an aggregated entry shows."""
    return (-a['confidence'], _relation_rank(a['relation']), a['source_part_id'] or '') < \
        (-b['confidence'], _relation_rank(b['relation']), b['source_part_id'] or '')


def _clean(link):
    return dict(link, confidence=_confidence(link['confidence']), source_part_id=_str(link['source_part_id']),
                target_year=_int(link['target_year']), source_date=_date(link['source_date']),
                relation=link['relation'] or 'mentioned')


def _group(links, key):
    """{key: (best link, [links])}"""
    groups = {}
    for link in links:
        k = link[key]
        if k in groups:
            best, members = groups[k]
            members.append(link)
            if _stronger(link, best):
                groups[k] = (link, members)
        else:
            groups[k] = (link, [link])
    return groups


def _parts(members):
    return sorted({m['source_part_id'] for m in members if m['source_part_id']})


def related_entries(links):
    """[entry] for one source's links: one per target, in display order."""
    entries = []
    for target, (best, members) in _group(links, 'target_doc_id').items():
        entries.append(dict(
            doc_id=target, kind=best['target_kind'], title=best['target_title'], relation=best['relation'],
            relations=sorted({m['relation'] for m in members}, key=_relation_rank), confidence=best['confidence'],
            parts=_parts(members), year=best['target_year'], code=_str(best['target_code'])))
    entries.sort(key=lambda e: (_kind_rank(e['kind']), -e['confidence'], e['title'] or '', e['doc_id']))
    return entries


def mentioned_entries(links, same_item=True):
    """[entry] for one target's links: one per source document, newest first."""
    entries = []
    for source, (best, members) in _group(links, 'source_doc_id').items():
        entries.append(dict(
            doc_id=source, kind=best['source_kind'], title=best['source_title'], date=best['source_date'],
            relation=best['relation'], confidence=best['confidence'], excerpt=best['excerpt'],
            part_id=best['source_part_id'], parts=_parts(members), year=best['target_year'], same_item=same_item))
    entries.sort(key=lambda e: e['doc_id'])
    entries.sort(key=lambda e: e['date'] or '', reverse=True)  # stable: same-date entries stay by doc id
    entries.sort(key=lambda e: e['relation'] not in STRUCTURAL)
    return entries


class RelatedItems:
    def __init__(self, links, prefix, min_confidence=0, max_mentioned_in=20, fold_codes=False):
        self.max = max_mentioned_in
        by_source, by_target, by_code = {}, {}, {}
        for link in links or []:
            link = _clean(link)
            if link['confidence'] < min_confidence or link['source_doc_id'] == link['target_doc_id']:
                continue
            if link['source_doc_id'].startswith(prefix):
                by_source.setdefault(link['source_doc_id'], []).append(link)
            if link['target_doc_id'].startswith(prefix):
                by_target.setdefault(link['target_doc_id'], []).append(link)
                if fold_codes and link['target_code']:
                    by_code.setdefault(link['target_code'], []).append(link)
        self.related = {k: related_entries(v) for k, v in by_source.items()}
        self.mentioned = {k: mentioned_entries(v) for k, v in by_target.items()}
        self.by_code = by_code
        logging.info('add_related_items: %d links, %d sources, %d targets', len(links or []), len(self.related),
                     len(self.mentioned))

    def fields(self, doc_id, code=None):
        mentioned = self.mentioned.get(doc_id, [])
        if code and code in self.by_code:
            others = [link for link in self.by_code[code] if link['target_doc_id'] != doc_id]
            if others:
                own = {e['doc_id'] for e in mentioned}
                mentioned = mentioned + [e for e in mentioned_entries(others, same_item=False)
                                         if e['doc_id'] not in own]
        return dict(related_items=self.related.get(doc_id, []), mentioned_in=mentioned[:self.max],
                    mentioned_in_count=len(mentioned))


def add_fields(resource, pattern, index, code_field=None):
    def func(rows):
        if rows.res.name != resource:
            yield from rows
            return
        for row in rows:
            try:
                doc_id = pattern.format(**row)
            except (KeyError, IndexError, ValueError):
                doc_id = None
            row.update(index.fields(doc_id, row.get(code_field) if code_field else None))
            yield row
    return func


def flow(parameters, *_):
    resource = parameters['resource']
    pattern = parameters['doc-id-pattern']
    code_field = parameters.get('code-field')
    prefix = doc_id_prefix(pattern)
    index = RelatedItems(load_links(prefix, parameters.get('db-engine')), prefix,
                         min_confidence=parameters.get('min-confidence', 0),
                         max_mentioned_in=parameters.get('max-mentioned-in', 20), fold_codes=bool(code_field))
    display = {'es:itemType': 'object', 'es:index': False}
    return DF.Flow(
        DF.add_field('related_items', 'array', resources=resource, **display, **{'es:schema': RELATED_SCHEMA}),
        DF.add_field('mentioned_in', 'array', resources=resource, **display, **{'es:schema': MENTIONED_SCHEMA}),
        DF.add_field('mentioned_in_count', 'integer', resources=resource, **{'es:index': False}),
        add_fields(resource, pattern, index, code_field),
        DF.update_resource(resource, **{'dpp:streaming': True}),
    )
