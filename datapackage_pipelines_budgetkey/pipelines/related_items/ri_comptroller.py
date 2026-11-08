"""State Comptroller chapters as targets: an audit citing or following up on an earlier one.

Comptroller chapters name earlier reports as (report, year, title) - "דוח שנתי 67ב", 2017, "זיהומי מים בין מדינת
ישראל לשטחי יהודה, שומרון ורצועת עזה" (the chapters pipeline's `earlier_reports`). A reference resolves to a chapter:
- the report: by its code (67ב) when it has one, else by the publication's title and year ("דוחות על הביקורת בשלטון
  המקומי לשנת 2014");
- the chapter in it: by title similarity. A clear best match is linked; close calls go to the LLM; a report with a
  single chapter is that chapter when the titles agree.
Only chapters published before the citing one are candidates.
"""
import csv
import datetime
import os
import re
from collections import defaultdict

from datapackage_pipelines_budgetkey.pipelines.related_items import ri_text as T

REPORTS_PATH = '/var/datapackages/state_comptroller/reports'
# Same shape as the chapters pipeline's report codes: an annual report's number and part letter.
REPORT_CODE = re.compile(r'(?<!\d)([4-9]\d)\s*([אבגד])(?![א-ת])')
CLEAR = 88       # title similarity (0-100) for a match without the LLM
MARGIN = 10      # ... and its lead over the next chapter of the same report
CANDIDATE = 55   # below this a chapter is not offered at all


def report_code(text):
    m = REPORT_CODE.search(T.clean_quotes(text or '').replace("'", ''))
    return m.group(1) + m.group(2) if m else None


def _norm(s):
    s = T.normalize_name(s or '')
    return re.sub(r'\b(?:ביקורת מעקב|מעקב|דוח|דו ח|פרק)\b', ' ', s).strip()


def _variants(title):
    """A chapter title, and the same without the audited body or report part appended after a dash
    ("זיהומי מים ... - המשרד להגנת הסביבה - מטלה בין-משרדית"), which references usually leave out."""
    parts = re.split(r'\s+[-–]\s+', T.clean_quotes(title or ''))
    out = [_norm(title)]
    for k in range(1, len(parts)):
        out.append(_norm(' - '.join(parts[:k])))
    return [v for v in out if v]


def _date(s):
    try:
        return datetime.date.fromisoformat(str(s)[:10])
    except ValueError:
        return None


class ChapterIndex:
    KIND = 'state_comptroller_audits'

    def __init__(self, chapters, publications):
        pubs = {str(p['pub_id']): p for p in publications}
        self.chapters = []
        self.by_code = defaultdict(list)
        self.by_pub = defaultdict(list)
        for c in chapters:
            pub = pubs.get(str(c['pub_id']), {})
            code = c.get('report_code') or pub.get('report_code') or report_code(c.get('publication_title'))
            entry = dict(id=str(c['chapter_id']), title=c.get('title') or '', pub_id=str(c['pub_id']),
                         publication_title=c.get('publication_title') or pub.get('title') or '',
                         date=_date(c.get('publish_date') or pub.get('publish_date')), code=code)
            self.chapters.append(entry)
            self.by_pub[entry['pub_id']].append(entry)
            if code:
                self.by_code[code].append(entry)
        self.publications = {}
        for pub_id, cs in self.by_pub.items():
            self.publications[pub_id] = dict(title=cs[0]['publication_title'], date=cs[0]['date'], chapters=cs)

    @classmethod
    def load(cls, path=None):
        path = path or os.environ.get('REPORTS') or REPORTS_PATH
        csv.field_size_limit(1 << 30)

        def rows(name):
            with open(os.path.join(path, name + '.csv'), encoding='utf8') as f:
                return list(csv.DictReader(f))
        return cls(rows('chapters'), rows('publications'))

    @staticmethod
    def doc_id(entry):
        return 'state_comptroller_audits/' + entry['id']

    @staticmethod
    def label(entry):
        return '"{}" in "{}" ({})'.format(entry['title'], entry['publication_title'], entry['date'])

    def report_chapters(self, report, year, before=None):
        """The chapters of the report a reference names, or [] if the report can't be told."""
        code = report_code(report)
        if code:
            found = self.by_code.get(code, [])
        else:
            norm = _norm(report)
            found = []
            if norm and year:
                from rapidfuzz import fuzz
                for pub in self.publications.values():
                    if pub['date'] and abs(pub['date'].year - int(year)) <= 1 and \
                            fuzz.token_set_ratio(norm, _norm(pub['title'])) >= 90:
                        found.extend(pub['chapters'])
        if before:
            found = [c for c in found if c['date'] is None or c['date'] <= before]
        return found

    def resolve(self, ref, before=None):
        """(entry, confidence, how) for a clear match; (None, candidates, 'llm') when the LLM should choose among
        candidates [(entry, score)]; (None, 0, reason) otherwise."""
        from rapidfuzz import fuzz
        chapters = self.report_chapters(ref.get('report'), ref.get('year'), before)
        if not chapters:
            return None, 0, 'no_report'
        title = _norm(ref.get('title'))
        if not title:
            return None, 0, 'no_title'
        scored = sorted(((c, max(max(fuzz.token_sort_ratio(title, v), fuzz.ratio(title, v))
                                 for v in _variants(c['title'])))
                         for c in chapters), key=lambda x: -x[1])
        best, score = scored[0]
        second = scored[1][1] if len(scored) > 1 else 0
        if score >= CLEAR and score - second >= MARGIN:
            return best, 0.95, 'title'
        candidates = [(c, s) for c, s in scored[:6] if s >= CANDIDATE]
        if not candidates:
            return None, 0, 'no_title_match'
        return None, candidates, 'llm'
