"""Links to budgetkey's own item pages (https://next.obudget.org/i/...), resolved to target doc ids.

A source that links to the site explicitly (the AI analysis pages) needs no text matching: each link names a record.
A link has one of two forms:
- a readable doc id: `/i/org/<kind>/<id>`, `/i/budget/<code>/<year>`, `/i/gov_decisions/<id>`, ...;
- a short id: `/i/<md5(doc id)[:12]>` (common/short_doc_id.py), which is what the `item_url` columns of the simpledb
  tables hold. The site resolves it through a redirect doc; here it is resolved by hashing the doc ids of the kinds
  below in SQL and keeping those that match.

A readable doc id other than an organization's is resolved through its short id too, so both forms go through the
same tables. An organization link is checked against `entities` by its id.

Budget links name one year's item (`budget/{code}/{year}`); they're resolved to the connected item that year belongs
to (the `budget` row whose code and year, or whose history, has it; of the same depth when an item and its parent both
list it), keeping the year and that year's code and title, as the linker does for budget codes in text.
"""
import hashlib
import logging
import os
import re

from datapackage_pipelines_budgetkey.pipelines.related_items import ri_db

SITE_LINK = re.compile(r'^https?://(?:next\.)?obudget\.org/i/([^?#\s]+?)/?(?:[?#].*)?$')
SHORT_ID = re.compile(r'^[0-9a-f]{12}$')

# Doc kinds that short ids are resolved against: the `item_url` kinds of the simpledb tables, with the table holding
# their docs. A doc id is `<kind>/<key 1>/<key 2>...`, as the indexers build it.
KINDS = [
    dict(kind='entities', table='entities', keys=['id'], title='name', extra=['kind'],
         doc_id=lambda r: 'org/{}/{}'.format(r['kind'], r['id'])),
    dict(kind='gov_decisions', table='government_decisions', keys=['id'], title='title'),
    dict(kind='support_programs', table='support_programs', keys=['program_key'], title='title'),
    dict(kind='contract-spending', table='contract_spending', keys=['publisher_key', 'order_id', 'budget_code'],
         title='purpose'),
    dict(kind='supports', table='supports_by_request_year',
         keys=['budget_code', 'year_requested', 'short_id', 'request_type'], title='support_title',
         extra=['recipient'], title_of=lambda r: ' - '.join(x for x in (r['site_title'], r['recipient']) if x)),
    dict(kind='activities', table='activities', keys=['kind', 'id'], title='name'),
]
# Short ids per query: the API passes the SQL in the URL.
BATCH = 2000 if os.environ.get('DPP_DB_ENGINE') else 150


def short_id(doc_id):
    return hashlib.md5(doc_id.encode('utf8')).hexdigest()[:12]


def parse(url):
    """('short', id), ('org', (kind, id)), ('doc', doc_id) for a site item link, or (None, None)."""
    m = SITE_LINK.match((url or '').strip())
    if not m:
        return None, None
    path = m.group(1)
    if SHORT_ID.match(path):
        return 'short', path
    parts = path.split('/')
    if parts[0] == 'org' and len(parts) == 3:
        return 'org', (parts[1], parts[2])
    return 'doc', path


def _hash_sql(kind, keys):
    # Python's str() of a missing key is 'None', which is what the item_url of such a row hashes.
    parts = " || '/' || ".join("coalesce({}::text, 'None')".format(k) for k in keys)
    return "left(md5('{}/' || {}), 12)".format(kind, parts)


def _in(values):
    return ', '.join("'{}'".format(v.replace("'", "''")) for v in values)


def _batches(values):
    values = sorted(values)
    for i in range(0, len(values), BATCH):
        yield values[i:i + BATCH]


class SiteLinkIndex:
    """{url: target} for a known set of URLs, resolved once (load) so the linker's threads only look up."""

    def __init__(self, resolved=None):
        # url -> dict(doc_id, kind, title, year, code), or a status string ('not_found', 'not_an_item')
        self.resolved = resolved or {}

    @classmethod
    def load(cls, urls):
        urls = set(urls)
        shorts, orgs = {}, {}
        resolved = {}
        for url in urls:
            form, value = parse(url)
            if form == 'short':
                shorts.setdefault(value, []).append(url)
            elif form == 'doc':
                shorts.setdefault(short_id(value), []).append(url)
            elif form == 'org':
                orgs.setdefault(value[1], []).append(url)
            else:
                resolved[url] = 'not_an_item'
        targets = cls.resolve_short_ids(set(shorts))
        for s, us in shorts.items():
            for url in us:
                resolved[url] = targets.get(s, 'not_found')
        entities = cls.entities_by_id(set(orgs))
        for entity_id, us in orgs.items():
            for url in us:
                resolved[url] = entities.get(entity_id, 'not_found')
        found = sum(1 for v in resolved.values() if isinstance(v, dict))
        logging.info('Site links: %d urls, %d resolved', len(resolved), found)
        return cls(resolved)

    def resolve(self, url):
        """(target, None) or (None, status)."""
        r = self.resolved.get(url, 'not_found')
        return (r, None) if isinstance(r, dict) else (None, r)

    # --- queries ---

    @staticmethod
    def entities_by_id(ids):
        out = {}
        for batch in _batches(ids):
            for r in ri_db.query('select id, kind, name from entities where id in ({})'.format(_in(batch)),
                                 ['id', 'kind', 'name']):
                out[r['id']] = dict(doc_id='org/{}/{}'.format(r['kind'], r['id']), kind='entities', title=r['name'])
        return out

    @classmethod
    def resolve_short_ids(cls, shorts):
        out = {}
        if not shorts:
            return out
        out.update(cls.budget(shorts))
        for spec in KINDS:
            left = shorts - set(out)
            if not left:
                break
            out.update(cls.by_kind(spec, left))
        return out

    @staticmethod
    def by_kind(spec, shorts):
        h = _hash_sql(spec['kind'], spec['keys'])
        # Aliased apart from the tables' own columns (supports have a `short_id` key of their own).
        columns = spec['keys'] + spec.get('extra', []) + ['site_title', 'site_short_id']
        select = spec['keys'] + spec.get('extra', []) + ['{} as site_title'.format(spec['title']),
                                                         h + ' as site_short_id']
        out = {}
        for batch in _batches(shorts):
            sql = 'select {} from {} where {} in ({})'.format(', '.join(select), spec['table'], h, _in(batch))
            for r in ri_db.query(sql, columns):
                doc_id = spec['doc_id'](r) if 'doc_id' in spec else \
                    '/'.join([spec['kind']] + [str(r[k]) for k in spec['keys']])
                title = spec['title_of'](r) if 'title_of' in spec else r['site_title']
                out.setdefault(r['site_short_id'], dict(doc_id=doc_id, kind=spec['kind'], title=title))
        return out

    @staticmethod
    def budget(shorts):
        """Budget items by the short id of any (code, year) they had: their own, or one in their history."""
        own = _hash_sql('budget', ['code', 'year'])
        hist = "left(md5('budget/' || split_part(ct, ':', 1) || '/' || h.y), 12)"
        out, from_history = {}, {}
        for batch in _batches(shorts):
            ids = _in(batch)
            for r in ri_db.query(
                    'select code, year, title, {} as site_short_id from budget where {} in ({})'.format(own, own, ids),
                    ['code', 'year', 'title', 'site_short_id']):
                out[r['site_short_id']] = dict(doc_id='budget/{}/{}'.format(r['code'], r['year']), kind='budget',
                                          title=r['title'], year=int(r['year']), code=r['code'])
            sql = ("select b.code, b.year, h.y, ct, {hist} as site_short_id from budget b, "
                   "jsonb_each(case when jsonb_typeof(b.history) = 'object' then b.history else '{{}}'::jsonb end) "
                   "as h(y, v), jsonb_array_elements_text(case when jsonb_typeof(h.v->'code_titles') = 'array' "
                   "then h.v->'code_titles' else '[]'::jsonb end) as ct "
                   "where {hist} in ({ids}) "
                   # A code can be in the history of an item and of its parent (a domain with one program): the
                   # item of the code's own depth (same length) is the one, then the latest.
                   "order by (length(b.code) = length(split_part(ct, ':', 1))) desc, b.year desc, b.code"
                   ).format(hist=hist, ids=ids)
            for r in ri_db.query(sql, ['code', 'year', 'y', 'ct', 'site_short_id']):
                code, _, title = r['ct'].partition(':')
                from_history.setdefault(r['site_short_id'], dict(
                    doc_id='budget/{}/{}'.format(r['code'], r['year']), kind='budget', title=title,
                    year=int(r['y']), code=code))
        # A (code, year) that is an item's own wins over one listed in another item's history.
        for s, target in from_history.items():
            out.setdefault(s, target)
        return out
