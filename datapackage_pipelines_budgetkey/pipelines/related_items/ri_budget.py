"""Budget items: the state budget's chapters, programs and lines (budgetkey's `budget`, one doc per connected item).

A budget doc (`budget/{code}/{year}`) stands for an item across the years it kept its meaning; its `history` lists
the codes and titles it had each year, and every such (code, year) redirects to it. So a link always targets the
connected item's doc, with `target_year` and the year's own code kept on the link.

Matching:
- explicit codes in the text ("תקנה 20-38-02-01") -> the item with that code in the source's years;
- program mentions (from the LLM) -> candidates within the budget chapters of the bodies involved, in the
  relevant years, ranked by embedding similarity of "chapter > domain > program" paths; the LLM decides.
"""
import json
import threading

import numpy as np

from datapackage_pipelines_budgetkey.common.cached_openai import embed_many

from datapackage_pipelines_budgetkey.pipelines.related_items import ri_db

# depth 1 = chapter (4-digit code "0020"), 2 = domain ("002038"), 3 = program ("00203801"), 4 = line ("0020380101")
MIN_YEAR, MAX_YEAR = 1997, 2026


class BudgetIndex:
    KIND = 'budget'

    def __init__(self, rows):
        self.items = []
        self.by_code_year = {}
        for row in rows:
            depth = int(row['depth']) if row.get('depth') not in (None, '') else None
            if depth is None or depth < 1:
                continue
            year = int(row['year'])
            history = row.get('history') or {}
            if isinstance(history, str):
                history = json.loads(history) if history else {}
            hierarchy = row.get('hierarchy') or []
            if isinstance(hierarchy, str):
                hierarchy = json.loads(hierarchy) if hierarchy else []
            item = dict(code=row['code'], year=year, title=row['title'], depth=depth,
                        path=[t for _, t in hierarchy] + [row['title']], years={year: (row['code'], row['title'])})
            for y, code_titles in history.items():
                for code_title in code_titles or []:
                    code, _, title = code_title.partition(':')
                    item['years'].setdefault(int(y), (code, title))
                    self.by_code_year.setdefault((code, int(y)), item)
            self.by_code_year[(row['code'], year)] = item
            self.items.append(item)
        self._vectors = None
        self._lock = threading.Lock()

    @classmethod
    def load(cls):
        rows = ri_db.query(
            "select code, year, title, depth, hierarchy::text, (select jsonb_object_agg(k, v->'code_titles') "
            "from jsonb_each(case when jsonb_typeof(history)='object' then history else '{}'::jsonb end) "
            "as h(k,v))::text as history from budget where depth between 1 and 4",
            ['code', 'year', 'title', 'depth', 'hierarchy', 'history'])
        return cls(rows)

    @staticmethod
    def doc_id(item):
        return 'budget/{}/{}'.format(item['code'], item['year'])

    def by_code(self, code, years):
        """(item, year) for the first of `years` in which `code` existed, or (None, None)."""
        for y in years:
            item = self.by_code_year.get((code, y))
            if item:
                return item, y
        return None, None

    @staticmethod
    def label(item, year=None):
        code, title = item['years'].get(year, (item['code'], item['title'])) if year else (item['code'], item['title'])
        return '{} ({})'.format(' > '.join(item['path'][:-1] + [title]), code)

    def _embedded(self, depths=(2, 3)):
        """Unit vectors of the "chapter > domain > program" paths of all items at `depths`, computed once - also when
        the linker's threads ask for them together (call prepare() before starting them to pay it up front)."""
        vectors = self._vectors
        if vectors is None or vectors[0] != depths:
            with self._lock:
                vectors = self._vectors
                if vectors is None or vectors[0] != depths:
                    items = [i for i in self.items if i['depth'] in depths]
                    vectors = (depths, items, unit_rows(embed_many([' > '.join(i['path']) for i in items])))
                    self._vectors = vectors
        return vectors[1], vectors[2]

    def prepare(self):
        self._embedded((2, 3))

    def candidates(self, query_vector, chapters, years, depths=(2, 3), limit=10):
        """Items under the given chapters (4-digit codes) that existed in any of `years`, most similar first."""
        items, m = self._embedded(tuple(depths))
        chapters, years = set(chapters), set(years)
        keep = [k for k, i in enumerate(items) if i['code'][:4] in chapters and years & set(i['years'])]
        if not keep:
            return []
        sims = m[keep] @ query_vector
        order = np.argsort(-sims)[:limit]
        return [(items[keep[k]], float(sims[k])) for k in order]


def source_years(date, start=None, end=None):
    """The budget years a source is about: its stated period (audit period, years of an allocation), else the
    year before it and its own year (an audit published in year N mostly covers N-1)."""
    years = []
    if start or end:
        start, end = int(start or end), int(end or start)
        if end < start:
            start, end = end, start
        years = list(range(max(start, MIN_YEAR), min(end, MAX_YEAR) + 1))
    if not years and date:
        years = [y for y in (date.year - 1, date.year) if MIN_YEAR <= y <= MAX_YEAR]
    return years


class SupportProgramIndex:
    KIND = 'support_programs'

    def __init__(self, rows):
        self.programs = []
        for row in rows:
            codes = row.get('budget_codes') or []
            if isinstance(codes, str):
                codes = json.loads(codes) if codes else []
            titles = row.get('all_titles') or []
            if isinstance(titles, str):
                titles = json.loads(titles) if titles else []
            self.programs.append(dict(
                key=row['program_key'], title=row['title'], ministry=row.get('supporting_ministry') or '',
                min_year=int(row['min_year']) if row.get('min_year') else None,
                max_year=int(row['max_year']) if row.get('max_year') else None,
                chapters={c[1][:4] for c in codes if len(c) > 1 and c[1]},
                titles=sorted({t[2] for t in titles if len(t) > 2 and t[2]} | {row['title']}),
            ))
        self._vectors = None
        self._lock = threading.Lock()

    @classmethod
    def load(cls):
        return cls(ri_db.query(
            'select program_key, title, supporting_ministry, min_year, max_year, budget_codes::text, '
            'all_titles::text from support_programs',
            ['program_key', 'title', 'supporting_ministry', 'min_year', 'max_year', 'budget_codes', 'all_titles']))

    @staticmethod
    def doc_id(program):
        return 'support_programs/' + program['key']

    @staticmethod
    def label(program):
        return '{} - {} ({}-{})'.format(' / '.join(program['titles'][:3]), program['ministry'],
                                        program['min_year'], program['max_year'])

    def _embedded(self):
        """Unit vectors of all programs' titles, computed once (thread-safe, like BudgetIndex._embedded)."""
        vectors = self._vectors
        if vectors is None:
            with self._lock:
                vectors = self._vectors
                if vectors is None:
                    vectors = unit_rows(embed_many(['{} - {}'.format(' / '.join(p['titles'][:3]), p['ministry'])
                                                    for p in self.programs]))
                    self._vectors = vectors
        return vectors

    def prepare(self):
        self._embedded()

    def candidates(self, query_vector, chapters, years, limit=8):
        keep = [k for k, p in enumerate(self.programs)
                if (not chapters or p['chapters'] & set(chapters))
                and (not years or p['min_year'] is None
                     or any(p['min_year'] - 1 <= y <= (p['max_year'] or p['min_year']) + 1 for y in years))]
        if not keep:
            return []
        sims = self._embedded()[keep] @ query_vector
        order = np.argsort(-sims)[:limit]
        return [(self.programs[keep[k]], float(sims[k])) for k in order]


def unit_rows(vectors):
    m = np.array(vectors, dtype=np.float32)
    return m / np.maximum(np.linalg.norm(m, axis=1, keepdims=True), 1e-9)


def embed_queries(texts):
    if not texts:
        return []
    return list(unit_rows(embed_many(texts)))


def chapters_of(bodies):
    out = set()
    for body in bodies:
        out.update(body.get('budget_chapters') or [])
    return sorted(out)

