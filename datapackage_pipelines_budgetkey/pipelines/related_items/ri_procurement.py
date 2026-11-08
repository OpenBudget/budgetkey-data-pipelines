"""Procurement: government tenders (office and central) cited by number.

Tender numbers repeat across publishers ("5-2021" is a tender of dozens of offices), so a number resolves only together
with its publisher: the tender's publisher must be one of the public bodies the document is about, or be named next to
the number, through bodies.csv (which lists the tender publishers' names as aliases). Exemptions from tender and
individual contracts are not linked: sources rarely identify a single one of them.
"""
import datetime
import re
from collections import defaultdict

from datapackage_pipelines_budgetkey.pipelines.related_items import ri_db
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_text as T

TYPES = ('office', 'central')


def normalize_tender_id(s):
    return re.sub(r'[^0-9A-Za-zא-ת]', '', s or '').lower()


def _date(s):
    try:
        return datetime.date.fromisoformat(str(s)[:10])
    except ValueError:
        return None


class TenderIndex:
    KIND = 'tenders'

    def __init__(self, rows):
        self.by_number = defaultdict(list)
        for r in rows:
            number = normalize_tender_id(r.get('tender_id'))
            if len(number) < 3 or number == 'none':
                continue
            self.by_number[number].append(dict(
                tender_type=r['tender_type'], publication_id=str(r['publication_id']), tender_id=r['tender_id'],
                publisher=r.get('publisher') or '', title=r.get('description') or r.get('page_title') or '',
                date=_date(r.get('publication_date')) or _date(r.get('start_date'))))

    @classmethod
    def load(cls):
        columns = ['tender_type', 'publication_id', 'tender_id', 'publisher', 'description', 'publication_date',
                   'start_date']
        return cls(ri_db.query('select {} from procurement_tenders_processed where tender_type in ({})'.format(
            ', '.join(columns), ', '.join("'{}'".format(t) for t in TYPES)), columns))

    @staticmethod
    def doc_id(t):
        return 'tenders/{}/{}/{}'.format(t['tender_type'], t['publication_id'], t['tender_id'])

    def resolve(self, number, entities, doc_bodies, context, before=None):
        """The tender with this number whose publisher is one of doc_bodies or is named in context, if exactly one.
        entities: the EntityIndex (for bodies.csv lookups). Returns (tender, how) or (None, reason)."""
        candidates = self.by_number.get(normalize_tender_id(number), [])
        if before:
            candidates = [t for t in candidates if t['date'] is None or t['date'] <= before]
        if not candidates:
            return None, 'not_found'
        context_norm = T.normalize_name(context)
        body_names = {id(b) for b in doc_bodies}
        matched = []
        for t in candidates:
            bodies = [b for b in entities.bodies_for_name(t['publisher']) if b.get('status') == 'ok']
            if not bodies:
                continue
            body = bodies[0]
            names = [body['name']] + body['aliases']
            if id(body) in body_names or any(len(T.normalize_name(n)) >= 6 and T.normalize_name(n) in context_norm
                                             for n in names):
                matched.append(t)
        if len(matched) == 1:
            return matched[0], 'publisher'
        return None, 'ambiguous' if matched else 'no_publisher'
