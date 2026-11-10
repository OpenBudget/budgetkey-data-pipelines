"""Resolves government decision citations to budgetkey's gov_decisions records.

Decision numbers restart with each government (and the table holds a few copies of some decisions), so a number
alone is not an identifier. A citation resolves when exactly one decision - after merging copies - has its number
and agrees with what the citation says:
- a citation with a date: the decision dated within a few days of it (or in that month, for "מיוני 2014");
- with a government number: the decision of that government;
- with neither: only when the source allows the same-government default (government decisions citing the
  government's own earlier decisions do so without a date), and then the latest decision with that number that
  precedes the source in the source's own government.
Anything else stays an unresolved mention, keeping the number, date and government for a later load of older
decisions.
"""
import datetime
from collections import defaultdict

from datapackage_pipelines_budgetkey.pipelines.related_items import ri_db
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_text as T

DECISION_TYPE = 'החלטות ממשלה'
DATE_TOLERANCE = datetime.timedelta(days=3)
CLOSE_DATE = datetime.timedelta(days=31)
NEAR_TRANSITION = datetime.timedelta(days=45)


def parse_date(s):
    if not s:
        return None
    if isinstance(s, datetime.datetime):
        return s.date()
    if isinstance(s, datetime.date):
        return s
    try:
        return datetime.date.fromisoformat(str(s)[:10])
    except ValueError:
        return None


def decision_date(row):
    # publish_date is when the record was scraped for some older decisions; doc_published_date is the decision's.
    return parse_date(row.get('doc_published_date')) or parse_date(row.get('publish_date'))


class DecisionIndex:
    KIND = 'gov_decisions'

    def __init__(self, rows):
        self.by_number = defaultdict(list)
        seen = {}
        for row in rows:
            number = (row.get('procedure_number_str') or '').strip()
            if not number.isdigit() or row.get('policy_type') != DECISION_TYPE:
                continue
            date = decision_date(row)
            gov = T.government_number(row.get('government')) or T.government_at(date)
            # Copies of one decision: same number, date and title. Keep one (the smallest id, for stability).
            key = (number, date, (row.get('title') or '').strip())
            if key in seen:
                if row['id'] < seen[key]['id']:
                    seen[key].update(id=row['id'])
                continue
            entry = dict(id=row['id'], number=number, date=date, government=gov,
                         title=row.get('title') or 'החלטת ממשלה {}'.format(number))
            seen[key] = entry
            self.by_number[str(int(number))].append(entry)

    @classmethod
    def load(cls):
        columns = ['id', 'procedure_number_str', 'government', 'doc_published_date', 'publish_date', 'policy_type',
                   'title']
        return cls(ri_db.query(
            "select {} from government_decisions where policy_type='{}'".format(', '.join(columns), DECISION_TYPE),
            columns))

    @staticmethod
    def doc_id(entry):
        return 'gov_decisions/' + entry['id']

    def resolve(self, citation, source_date=None, same_government_default=False, exclude_id=None):
        """Returns (entry, confidence, how) or (None, 0, reason)."""
        candidates = [c for c in self.by_number.get(str(int(citation['number'])), []) if c['id'] != exclude_id]
        if source_date:
            # A decision can't cite a later one (a day's slack for same-day decisions).
            candidates = [c for c in candidates if c['date'] is None or c['date'] <= source_date + datetime.timedelta(1)]
        if not candidates:
            return None, 0, 'not_found'
        date, precision, gov = citation.get('date'), citation.get('date_precision'), citation.get('government')
        if date:
            if gov:
                candidates = [c for c in candidates if c['government'] in (None, gov)]
            if precision == 'day':
                matched = [c for c in candidates if c['date'] and abs(c['date'] - date) <= DATE_TOLERANCE]
                if matched:
                    return self._one(matched, 0.98, 'date')
            else:
                matched = [c for c in candidates
                           if c['date'] and (c['date'].year, c['date'].month) == (date.year, date.month)]
                if matched:
                    return self._one(matched, 0.9, 'month')
            # Numbers are unique within a government, so a date that is off (the publication date differs from
            # the decision's, or the citation has a typo) still identifies the decision through its government -
            # unless the date is close to a change of government.
            cited_gov = gov or T.government_at(date)
            if T.government_at(date - NEAR_TRANSITION) != T.government_at(date + NEAR_TRANSITION):
                return None, 0, 'not_found'
            matched = [c for c in candidates if c['government'] == cited_gov]
            close = [c for c in matched if c['date'] and abs(c['date'] - date) <= CLOSE_DATE]
            if close:
                return self._one(close, 0.93, 'date_close')
            return self._one(matched, 0.85, 'date_government')
        if gov:
            return self._one([c for c in candidates if c['government'] == gov], 0.9, 'government')
        if same_government_default and source_date:
            own = T.government_at(source_date)
            matched = sorted((c for c in candidates if c['government'] == own and c['date']), key=lambda c: c['date'])
            if matched:
                return matched[-1], 0.8, 'same_government'
            return None, 0, 'not_found'
        return None, 0, 'no_date'

    @staticmethod
    def _one(matched, confidence, how):
        if len(matched) == 1:
            return matched[0], confidence, how
        if not matched:
            return None, 0, 'not_found'
        return None, 0, 'ambiguous'
