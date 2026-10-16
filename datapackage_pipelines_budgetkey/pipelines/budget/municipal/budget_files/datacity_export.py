"""Turns our municipal budget lines into datacity muni-budgets loads: one CSV per authority and fiscal year.

A fiscal year Y of an authority has three phases, each read from a different regular budget file:
the proposed budget (מקורי) from Y's own file, the approved one (מאושר) from Y+1's and execution (ביצוע)
from Y+2's. For each phase:

- A phase that a datacity pipeline of someone else already loads is theirs. Ours only fill the gaps.
- Spreadsheets are preferred over PDFs.
- Expenses and income are chosen separately, since some authorities publish them as two files. Each side
  starts from its file with the most lines. A further file is added only when it shares no account codes
  with the ones already chosen and brings that side closer to the other one: Karmiel's salaries and other
  expenses are two files that only add up to its income together, while Tel Aviv's summary file would
  unbalance a budget that already balances. A file that repeats the codes is a copy or another version.
- Two sheets of one file that repeat the same codes are copies or versions of one budget (Arad's PIVOT
  sheet, Ramla's two versions): the sheet with fewer lines is left out.

Rows hold the 6 fields a muni-budgets pipeline maps: expenses positive and income negative, as in most
of muni_budgets, and one row per card code and phase (the pipelines' primary key).
"""
import collections
import re

KIND_PHASE = {'proposed': ('מקורי', 0), 'approved': ('מאושר', -1), 'executed': ('ביצוע', -2)}
PHASE_ORDER = ['מקורי', 'מאושר', 'ביצוע']
REGULAR_BUDGET = ('budget', 'budget_proposal')
SPREADSHEETS = ('xlsx', 'xls', 'csv')
DIRECTIONS = ('expense', 'income')
FIELDS = ['card-code', 'card-name', 'municipality-name', 'phase', 'fiscal-year', 'value']
MIN_SIDE = 1e6   # smaller sides are ratios, staff counts or summaries, not a budget
# A phase this many times off the authority's complete budgets is a summary table or a misread unit
MAX_DEVIATION = 5
MIN_ROWS = 50   # fewer lines are a chapter summary
MIN_COMPLETE_ROWS = 200
MAX_ANCHOR_DEVIATION = 4   # complete budgets of 2007-2026 are 0.4-2.8 times their authority's 2015 expenses
TLV = re.compile(r'(\d{5})/(\d{3})/(\d)')
JERUSALEM = re.compile(r'(\d{4})(\d{3})\d')
CARD = re.compile(r'[0-9]{10,15}')   # datacity drops any other card code
# What a row needs of a budget line; a million of them are held at once
Line = collections.namedtuple('Line', 'sheet code title amount')


def line_of(row):
    return Line(row.get('sheet') or '', row['code'], row['title'], float(row['amount']))


def cbs_symbol(authority_code):
    """The CBS symbol as LAMAS and datacity write it. Regional councils are 55xx at the Interior Ministry and
    "xx" (two digits) at the CBS; the others share one code, in four digits. Ofakim (31) is "0031" and Nahal
    Sorek (5531) is "31": as numbers they would collide."""
    code = int(authority_code)
    return '%02d' % (code - 5500) if 5500 < code < 5600 else '%04d' % code


def card_code(code):
    """Tel Aviv's "81111/110/5" (function, economic class, serial) becomes 1 + function + serial + economic
    class: datacity reads the function from digits 2-5 and the economic class from the last 3. Jerusalem's
    "61011119" (function, economic class, check digit) becomes 1 + function + 00 + economic class. Both keep
    their city's own numbering, which differs from the Interior Ministry's: the fetcher names their chapters."""
    m = TLV.fullmatch(code or '')
    if m:
        return '1' + m[1] + m[3] + m[2]
    m = JERUSALEM.fullmatch(code or '')
    return '1' + m[1] + '00' + m[2] if m else code


class Aggregates:
    """Lines, amounts and account codes per (file, kind, direction) and sheet: the first pass over the lines."""

    def __init__(self):
        self.sheets = collections.defaultdict(lambda: collections.defaultdict(lambda: dict(n=0, amount=0.0)))
        self.codes = collections.defaultdict(lambda: collections.defaultdict(set))
        self.files = {}

    def add(self, line):
        kind, direction = line.get('column_kind'), line.get('direction')
        if line.get('doc_type') not in REGULAR_BUDGET or kind not in KIND_PHASE or direction not in DIRECTIONS \
                or line.get('amount') in (None, ''):
            return
        key = (line['file_url'], kind, direction)
        s = self.sheets[key][line.get('sheet') or '']
        s['n'] += 1
        s['amount'] += float(line['amount'])
        self.codes[key][line.get('sheet') or ''].add(line['code'])
        self.files.setdefault(line['file_url'], dict(
            authority_code=int(line['authority_code']), fiscal_year=int(line['fiscal_year']), format=line['format']))

    def dropped_sheets(self, key):
        by_sheet = self.codes[key]
        names = sorted(by_sheet)
        dropped = set()
        for i, x in enumerate(names):
            for y in names[i + 1:]:
                if len(by_sheet[x] & by_sheet[y]) >= .5 * min(len(by_sheet[x]), len(by_sheet[y])):
                    dropped.add(min((x, y), key=lambda s: (self.sheets[key][s]['n'], s)))
        return dropped

    def candidates(self):
        """(authority, year, phase, direction) -> candidate files, each with its kept sheets' totals."""
        out = collections.defaultdict(list)
        for key, sheets in self.sheets.items():
            file_url, kind, direction = key
            f = self.files[file_url]
            dropped = self.dropped_sheets(key)
            kept = [s for name, s in sheets.items() if name not in dropped]
            codes = set().union(*(c for name, c in self.codes[key].items() if name not in dropped))
            phase, offset = KIND_PHASE[kind]
            out[(f['authority_code'], f['fiscal_year'] + offset, phase, direction)].append(dict(
                file_url=file_url, kind=kind, direction=direction, format=f['format'], file_year=f['fiscal_year'],
                lines=sum(s['n'] for s in kept), amount=sum(s['amount'] for s in kept),
                dropped_sheets=sorted(dropped), codes=codes))
        return out


def best(candidates, key, spreadsheet):
    """The files for one phase (key = authority, year, phase), from spreadsheets or from PDFs, or None."""
    side, extra = {}, {}
    for direction in DIRECTIONS:
        cands = sorted((c for c in candidates.get(key + (direction,), [])
                        if (c['format'] in SPREADSHEETS) == spreadsheet and abs(c['amount']) > MIN_SIDE),
                       key=lambda c: (-c['lines'], c['file_url']))
        if cands:
            side[direction], extra[direction] = [cands[0]], []
            seen = set(cands[0]['codes'])
            for c in cands[1:]:
                if not c['codes'] & seen:
                    extra[direction].append(c)
                    seen |= c['codes']
    if 'expense' not in side:
        return None

    def total(d):
        return sum(c['amount'] for c in side.get(d, []))

    for d, other in (('expense', 'income'), ('income', 'expense')):
        if extra.get(d) and other in side:
            more = abs(total(d) + sum(c['amount'] for c in extra[d]))
            if abs(more - abs(total(other))) < abs(abs(total(d)) - abs(total(other))):
                side[d] += extra[d]
    return [c for d in DIRECTIONS for c in side.get(d, [])]


def choose(candidates, covered):
    """(authority, year) -> {phase: parts} for the phases nobody else loads."""
    chosen = collections.defaultdict(dict)
    for key in sorted({k[:3] for k in candidates}):
        code, year, phase = key
        if (cbs_symbol(code), year, phase) in covered:
            continue
        parts = best(candidates, key, True) or best(candidates, key, False)
        if parts:
            chosen[(code, year)][phase] = parts
    return chosen


def phase_rows(parts, lines_by_part, phase, year, name):
    """The rows of one phase, and a summary of what went into them."""
    by_card = collections.OrderedDict()
    sides, merged, invalid = collections.Counter(), 0, 0
    for part in parts:
        lines = lines_by_part.get((part['file_url'], part['kind'], part['direction']), [])
        lines = [ln for ln in lines if ln.sheet not in part['dropped_sheets']]
        total = sum(ln.amount for ln in lines)
        # A side the file stores as negative is flipped; then expenses are positive and income negative
        sign = (-1 if total < 0 else 1) * (-1 if part['direction'] == 'income' else 1)
        for ln in lines:
            cc = card_code(ln.code)
            if not CARD.fullmatch(cc or ''):
                invalid += 1
                continue
            value = sign * ln.amount
            if cc in by_card:
                by_card[cc]['value'] += value
                merged += 1
            else:
                by_card[cc] = dict(title=ln.title, value=value)
            sides[part['direction']] += value
    rows = [{'card-code': cc, 'card-name': x['title'], 'municipality-name': name, 'phase': phase,
             'fiscal-year': year, 'value': round(x['value'], 2)} for cc, x in by_card.items()]
    summary = dict(phase=phase, rows=len(rows), expense=round(sides['expense']), income=round(sides['income']),
                   merged_lines=merged, invalid_codes=invalid,
                   files=[dict(direction=p['direction'], url=p['file_url'], format=p['format'],
                               file_year=p['file_year'], lines=p['lines'], sheets_left_out=p['dropped_sheets'])
                          for p in parts])
    return rows, summary


def size(summary):
    return summary['expense'] or -summary['income']


def complete(summary):
    """A whole budget: hundreds of lines, and income within 10% of the expenses."""
    expense, income = summary['expense'], -summary['income']
    return summary['rows'] >= MIN_COMPLETE_ROWS and expense > 0 and abs(expense - income) <= 0.1 * expense


def implausible(summaries, anchor=None):
    """{(year, phase): reason} for those of one authority's phase summaries that are not a line-item budget: too
    few lines (a chapter summary), or a size more than MAX_DEVIATION times off its typical budget. That is the
    median of its complete budgets which agree with the anchor (its 2015 expenses at the CBS); failing those, of
    its complete budgets if there are two or more (a fast-growing town outgrows its 2015 budget); failing that,
    the anchor; and failing that, its one complete budget."""
    whole = sorted(size(s) for s in summaries.values() if complete(s))
    anchored = [x for x in whole if anchor and anchor / MAX_ANCHOR_DEVIATION <= x <= anchor * MAX_ANCHOR_DEVIATION]
    reference = anchored or (whole if len(whole) > 1 else None) or ([anchor] if anchor else whole)
    median = reference[len(reference) // 2] if reference else None
    off = {}
    for key, s in summaries.items():
        if s['rows'] < MIN_ROWS:
            off[key] = '%d lines' % s['rows']
        elif median and not median / MAX_DEVIATION <= size(s) <= median * MAX_DEVIATION:
            off[key] = '%.4g times the typical budget' % (size(s) / median)
    return off


def sort_rows(rows):
    return sorted(rows, key=lambda r: (PHASE_ORDER.index(r['phase']), r['card-code']))
