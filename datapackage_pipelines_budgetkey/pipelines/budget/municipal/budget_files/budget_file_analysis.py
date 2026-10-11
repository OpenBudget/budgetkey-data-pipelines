"""Checks whether a municipal budget file holds a machine-readable budget table.

For every sheet (or a PDF's text) we look for the budget table: where it starts, its code and title columns, its value
columns (executed / approved / proposed / revised, with years), whether incomes are positive or negative, and whether
figures are in NIS or thousands of NIS.

Municipal budget codes follow the Interior Ministry's chart of accounts: 10 digits, with digits 2-3 being the chapter
(פרק). Chapters 10-59 are incomes and 60-99 are expenses, e.g. 1111000100 ארנונה (income), 1741000752 (expense).
"""
import csv
import io
import re
import statistics
import subprocess
import tempfile
from collections import Counter

ANALYZER_VERSION = 1

MAX_ROWS = 20000
HEADER_SEARCH = 12
MIN_CODE_ROWS = 5

CODE = re.compile(r'^[1-9]\d{9}$')
YEAR = re.compile(r'(?<!\d)(19[9]\d|20[0-4]\d)(?!\d)')
THOUSANDS = re.compile(r'אלפי|באלפים|אלפים|אלש"?״?ח|בא?לפי|K ?₪|thousands|بالآلاف|آلاف', re.I)
NIS = re.compile(r'ש"?״?ח|₪|שקל|NIS|شيكل', re.I)
NOT_VALUE_HEADER = re.compile(r'פרק|סעיף|סוג|מס\'|מספר|קוד|כרטיס|משרות|תקנים|אחוז|%|שורה|מזהה', re.I)
TEXT_HEADER = re.compile(r'שם|תיאור|פירוט|הסבר|title|name|اسم|بيان', re.I)

VALUE_KINDS = [
    ('executed', re.compile(r'ביצוע|בצוע|בפועל|actual|تنفيذ|فعلي', re.I)),
    ('revised', re.compile(r'עדכון|מעודכן|מתוקן|תוספת|הפחתה|שינוי|تعديل', re.I)),
    ('proposed', re.compile(r'הצעה|הצעת|מוצע|מקורי|اقتراح|مقترح', re.I)),
    ('approved', re.compile(r'תקציב|מאושר|ميزانية|موازنة|budget', re.I)),
]
RTL_MARKS = re.compile(r'[‎‏‪-‮⁦-⁩]')


# --- cell helpers ------------------------------------------------------------

def text(cell):
    if cell is None:
        return ''
    if isinstance(cell, float) and cell.is_integer():
        cell = int(cell)
    return RTL_MARKS.sub('', str(cell)).strip()


def number(cell):
    """A cell's numeric value, or None. Handles "1,234", "(1,234)", "1,234-" and "-1,234"."""
    if isinstance(cell, bool):
        return None
    if isinstance(cell, (int, float)):
        return float(cell)
    s = text(cell).replace(',', '').replace('₪', '').strip()
    if not s:
        return None
    negative = False
    if s.startswith('(') and s.endswith(')'):
        s, negative = s[1:-1], True
    if s.endswith('-'):
        s, negative = s[:-1], True
    try:
        value = float(s)
    except ValueError:
        return None
    return -value if negative else value


def code_of(cell):
    if isinstance(cell, float) and not cell.is_integer():
        return None
    s = re.sub(r'[\s\-]', '', text(cell))
    return s if CODE.match(s) else None


def chapter_kind(code):
    """'income' or 'expense' from the chapter digits of a 10-digit code."""
    if len(code) != 10:
        return None
    chapter = int(code[1:3])
    if 10 <= chapter <= 59:
        return 'income'
    if 60 <= chapter <= 99:
        return 'expense'


# --- value columns -----------------------------------------------------------

def describe_value_column(header):
    years = [int(y) for y in YEAR.findall(header)]
    kind = next((k for k, pattern in VALUE_KINDS if pattern.search(header)), None)
    if kind is None and years:
        kind = 'approved'
    return dict(header=header, kind=kind, year=max(years) if years else None)


def expected_columns(columns, fiscal_year):
    """The usual trio for budget year Y: executed (before Y), approved for Y-1 and the budget for Y itself."""
    if not fiscal_year:
        return False
    executed = any(c['kind'] == 'executed' and c['year'] and c['year'] < fiscal_year for c in columns)
    current = any(c['kind'] in ('approved', 'revised') and c['year'] == fiscal_year - 1 for c in columns)
    upcoming = any(c['kind'] in ('approved', 'proposed') and c['year'] == fiscal_year for c in columns)
    return executed and current and upcoming


# --- signs and units ---------------------------------------------------------

def signs(rows_by_kind):
    """Sign of incomes and expenses, judged on the sum of each group's main value column."""
    result = {}
    for kind in ('income', 'expense'):
        values = rows_by_kind.get(kind, [])
        total = sum(values)
        result[kind] = None if not values or total == 0 else ('positive' if total > 0 else 'negative')
    return result['income'], result['expense']


def units(header_text, values):
    if THOUSANDS.search(header_text):
        return 'thousands', 'header'
    nonzero = [abs(v) for v in values if v]
    if len(nonzero) >= 5:
        # Typical line items are tens of thousands of NIS and up; in thousands they are mostly 2-3 digit numbers
        if statistics.median(nonzero) < 5000:
            return 'thousands', 'magnitude'
        return 'nis', 'magnitude' if not NIS.search(header_text) else 'header'
    if NIS.search(header_text):
        return 'nis', 'header'
    return None, None


# --- sheet analysis ----------------------------------------------------------

def nonempty(row):
    return [i for i, c in enumerate(row) if text(c)]


def find_code_column(rows):
    counts = Counter()
    for row in rows:
        for i, cell in enumerate(row):
            if code_of(cell):
                counts[i] += 1
    if not counts:
        return None, 0
    col, count = counts.most_common(1)[0]
    return (col, count) if count >= MIN_CODE_ROWS else (None, count)


def composite_code_columns(rows):
    """Some authorities split the code into ספר (book), פרק (3-digit chapter) and סעיף (item) columns."""
    for r, row in enumerate(rows[:HEADER_SEARCH * 2]):
        headers = {text(c): i for i, c in enumerate(row) if text(c)}
        chapter = next((i for h, i in headers.items() if re.fullmatch(r'פרק', h)), None)
        item = next((i for h, i in headers.items() if re.fullmatch(r'סעיף|סעיף תקציבי|מס\' סעיף', h)), None)
        book = next((i for h, i in headers.items() if re.fullmatch(r'ספר', h)), None)
        if chapter is not None and item is not None:
            return r, book, chapter, item
    return None


def composite_code(row, book, chapter, item):
    parts = [number(row[c]) if c is not None else 1 for c in (book, chapter, item)]
    if any(p is None or p < 0 or not float(p).is_integer() for p in parts):
        return None
    book_, chapter_, item_ = (int(p) for p in parts)
    if not (0 < book_ < 10 and 100 <= chapter_ <= 999 and item_ < 10 ** 6):
        return None
    return '%d%03d%06d' % (book_, chapter_, item_)


def header_rows(rows, first_data):
    """The header row just above the data, plus the row above it when headers span two rows."""
    for r in range(first_data - 1, max(-1, first_data - 1 - HEADER_SEARCH), -1):
        cells = nonempty(rows[r])
        textual = [i for i in cells if number(rows[r][i]) is None or YEAR.fullmatch(text(rows[r][i]))]
        if len(textual) >= 2:
            above = r - 1 if r > 0 and len(nonempty(rows[r - 1])) >= 2 else None
            return r, above
    return None, None


def column_header(rows, header, above, col):
    parts = [text(rows[r][col]) for r in (above, header) if r is not None and col < len(rows[r])]
    return ' '.join(p for p in parts if p)


def analyze_rows(rows, name='', fiscal_year=None):
    """Analyze one sheet given as a list of rows (lists of cells)."""
    rows = [list(r) for r in rows[:MAX_ROWS]]
    width = max((len(r) for r in rows), default=0)
    rows = [r + [None] * (width - len(r)) for r in rows]
    result = dict(sheet=name, kind='none', header_row=None, first_col=None, code_col=None, title_col=None,
                  n_rows=0, value_columns=[], income_sign=None, expense_sign=None, units=None, units_source=None,
                  expected_columns=False, ready=False, issues=[])
    if not rows:
        return result

    code_col, _ = find_code_column(rows)
    composite = None
    if code_col is None:
        composite = composite_code_columns(rows)
        if composite is None:
            return analyze_summary(rows, result)
        # Append the assembled code as an extra column and analyze as usual
        _, book, chapter, item = composite
        rows = [row + [composite_code(row, book, chapter, item) if r > composite[0] else None]
                for r, row in enumerate(rows)]
        width += 1
        code_col = width - 1
        if sum(1 for row in rows if row[code_col]) < MIN_CODE_ROWS:
            return analyze_summary([row[:-1] for row in rows], result)

    data = [i for i, row in enumerate(rows) if code_of(row[code_col])]
    header, above = header_rows(rows, data[0])
    result.update(kind='detailed', code_col=code_col, n_rows=len(data), header_row=header)
    if header is None:
        result['issues'].append('no-header-row')

    # Title: a mostly-textual column; line-item names are the most varied ("שם כרטיס", not "שם פרק")
    textual, distinct = Counter(), {}
    for i in data:
        for c, cell in enumerate(rows[i]):
            if c != code_col and text(cell) and number(cell) is None:
                textual[c] += 1
                distinct.setdefault(c, set()).add(text(cell))
    title_candidates = [c for c, n in textual.items() if n >= len(data) / 2]
    if title_candidates:
        headers = {c: column_header(rows, header, above, c) if header is not None else '' for c in title_candidates}
        result['title_col'] = min(title_candidates, key=lambda c: (
            not TEXT_HEADER.search(headers[c]), -len(distinct[c]), abs(c - code_col)))
    else:
        result['issues'].append('no-title-column')

    # Value columns: mostly numeric in the data rows, and not another code/count column
    value_cols = []
    for c in range(width):
        if c in (code_col, result['title_col']):
            continue
        numeric = sum(1 for i in data if number(rows[i][c]) is not None)
        if numeric < len(data) * 0.3:
            continue
        head = column_header(rows, header, above, c) if header is not None else ''
        if NOT_VALUE_HEADER.search(head) and not YEAR.search(head):
            continue
        if all(abs(number(rows[i][c]) or 0) < 1000 and float(number(rows[i][c]) or 0).is_integer() for i in data) \
                and not YEAR.search(head):
            continue  # small integers without a year in the header: chapter numbers and the like
        value_cols.append(c)
    result['value_columns'] = [dict(describe_value_column(column_header(rows, header, above, c)
                                                          if header is not None else ''), col=c)
                               for c in value_cols]
    if not value_cols:
        result['issues'].append('no-value-columns')

    used = [c for c in [code_col, result['title_col']] + value_cols if c is not None]
    if composite:
        used = [c for c in used if c != code_col] + [c for c in composite[1:] if c is not None]
    result['first_col'] = min(used)

    # Signs and units are judged on the column for the file's year, else the last value column
    if value_cols:
        main = next((v['col'] for v in result['value_columns'] if fiscal_year and v['year'] == fiscal_year),
                    value_cols[-1])
        by_kind = {}
        for i in data:
            kind, value = chapter_kind(code_of(rows[i][code_col])), number(rows[i][main])
            if kind and value is not None:
                by_kind.setdefault(kind, []).append(value)
        result['income_sign'], result['expense_sign'] = signs(by_kind)
        # Units: the main column's own header and the title rows above the table, not other columns' headers
        header_text = ' '.join([name, column_header(rows, header, above, main) if header is not None else '']
                               + [text(c) for r in rows[:header or 0] for c in r])
        result['units'], result['units_source'] = units(header_text, by_kind.get('income', []) + by_kind.get('expense', []))
        if result['income_sign'] is None and result['expense_sign'] is None:
            result['issues'].append('sign-unknown')
        if result['units'] is None:
            result['issues'].append('units-unknown')
        if not any(v['year'] for v in result['value_columns']):
            result['issues'].append('no-years-in-headers')

    if composite:
        result['code_col'] = 'composite:' + '+'.join(str(c) for c in composite[1:] if c is not None)
        result['issues'].append('composite-code')
    result['expected_columns'] = expected_columns(result['value_columns'], fiscal_year)
    result['ready'] = bool(result['title_col'] is not None and value_cols and result['units']
                           and (result['income_sign'] or result['expense_sign'])
                           and any(v['year'] for v in result['value_columns']))
    return result


def analyze_summary(rows, result):
    """Sheets without budget codes: chapter-level summaries, or not a budget table at all."""
    numeric_rows = [i for i, row in enumerate(rows)
                    if sum(1 for c in row if number(c) is not None) >= 2 and any(text(c) and number(c) is None for c in row)]
    if len(numeric_rows) >= 5:
        header, _ = header_rows(rows, numeric_rows[0])
        result.update(kind='summary', header_row=header, n_rows=len(numeric_rows),
                      first_col=min(nonempty(rows[header])) if header is not None else None)
        result['issues'].append('no-budget-codes')
    return result


# --- file formats ------------------------------------------------------------

def excel_sheets(path):
    with open(path, 'rb') as f:
        magic = f.read(4)
    if magic == b'PK\x03\x04':
        import openpyxl
        workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
        for sheet in workbook.worksheets:
            rows = []
            for row in sheet.iter_rows(values_only=True):
                rows.append(list(row))
                if len(rows) >= MAX_ROWS:
                    break
            yield sheet.title, rows
        workbook.close()
    elif magic == b'\xd0\xcf\x11\xe0':
        import xlrd
        workbook = xlrd.open_workbook(path, on_demand=True)
        for sheet in workbook.sheets():
            yield sheet.name, [sheet.row_values(i) for i in range(min(sheet.nrows, MAX_ROWS))]
    else:
        raise ValueError('not an Excel file')


def csv_rows(path):
    with open(path, 'rb') as f:
        raw = f.read()
    for encoding in ('utf-8-sig', 'cp1255', 'latin-1'):
        try:
            content = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    try:
        dialect = csv.Sniffer().sniff(content[:10000], delimiters=',;\t|')
    except csv.Error:
        dialect = csv.excel
    return list(csv.reader(io.StringIO(content), dialect))


def pdf_text(path, max_pages=300):
    with tempfile.NamedTemporaryFile(suffix='.txt') as out:
        subprocess.run(['pdftotext', '-layout', '-l', str(max_pages), path, out.name],
                       check=True, timeout=300, capture_output=True)
        with open(out.name, encoding='utf-8', errors='replace') as f:
            return RTL_MARKS.sub('', f.read())


def pdf_pages(path):
    try:
        info = subprocess.run(['pdfinfo', path], timeout=60, capture_output=True, text=True).stdout
        return int(re.search(r'Pages:\s+(\d+)', info).group(1))
    except Exception:
        return None


def analyze_pdf_text(content, pages, fiscal_year=None):
    """Turn a PDF's layout text into rows and analyze it like a sheet."""
    result = dict(sheet='pdf', kind='none', header_row=None, first_col=None, code_col=None, title_col=None,
                  n_rows=0, value_columns=[], income_sign=None, expense_sign=None, units=None, units_source=None,
                  expected_columns=False, ready=False, issues=[])
    if pages and len(content.strip()) < 100 * pages:
        result['kind'] = 'scanned'
        result['issues'].append('no-text-layer')
        return result

    lines = content.splitlines()
    code_lines = [i for i, line in enumerate(lines) if re.search(r'(?<!\d)[1-9]\d{9}(?!\d)', line)]
    if len(code_lines) < MIN_CODE_ROWS:
        summary_lines = [l for l in lines if len(re.findall(r'\d{1,3}(?:,\d{3})+', l)) >= 2]
        if len(summary_lines) >= 5:
            result.update(kind='summary', n_rows=len(summary_lines))
            result['issues'].append('no-budget-codes')
        return result

    rows, widths = [], Counter()
    for i in code_lines:
        line = lines[i]
        code = re.search(r'(?<!\d)([1-9]\d{9})(?!\d)', line).group(1)
        # Layout text separates columns with runs of spaces; numbers inside a title stay part of the title
        cells = [c for c in re.split(r'\s{2,}', line.strip()) if c and c != code]
        values = [number(c) for c in cells if number(c) is not None and not YEAR.fullmatch(c)]
        title = ' '.join(c.replace(code, '').strip() for c in cells if number(c) is None)
        rows.append((code, title, values))
        widths[len(values)] += 1
    n_values = widths.most_common(1)[0][0]

    # Column headers: the nearest line above the table that names several years; layout text keeps headers and
    # values in the same left-to-right order, so they pair up by position
    head = ' '.join(lines[max(0, code_lines[0] - 8):code_lines[0]])
    header_line = next((lines[j] for j in range(code_lines[0] - 1, max(-1, code_lines[0] - 9), -1)
                        if len(YEAR.findall(lines[j])) >= min(n_values, 2)), head)
    head_years = [int(y) for y in YEAR.findall(header_line)]
    result.update(kind='detailed', n_rows=len(rows), code_col=0, title_col=1, header_row=max(0, code_lines[0] - 1))
    result['value_columns'] = [describe_value_column(head_part)
                               for head_part in split_header(header_line, n_values)]
    by_kind = {}
    for code, title, values in rows:
        kind = chapter_kind(code)
        if kind and values:
            by_kind.setdefault(kind, []).append(values[0])
    result['income_sign'], result['expense_sign'] = signs(by_kind)
    result['units'], result['units_source'] = units(head, by_kind.get('income', []) + by_kind.get('expense', []))
    if not head_years:
        result['issues'].append('no-years-in-headers')
    if result['income_sign'] is None and result['expense_sign'] is None:
        result['issues'].append('sign-unknown')
    if any(len(v) != n_values for _, _, v in rows):
        result['issues'].append('ragged-rows')
    result['expected_columns'] = expected_columns(result['value_columns'], fiscal_year)
    result['ready'] = bool(n_values and result['units'] and (result['income_sign'] or result['expense_sign'])
                           and head_years)
    return result


def split_header(head, n_values):
    """Best-effort header text per value column: one chunk per year mentioned, in order of appearance."""
    chunks = re.split(r'\s{3,}', head.strip())
    with_year = [c for c in chunks if YEAR.search(c)]
    if len(with_year) >= n_values:
        return with_year[:n_values]
    return with_year + [''] * (n_values - len(with_year))


def analyze_file(path, fmt, fiscal_year=None):
    """Returns (file summary, list of table analyses)."""
    tables = []
    if fmt in ('xlsx', 'xls'):
        for name, rows in excel_sheets(path):
            tables.append(analyze_rows(rows, name, fiscal_year))
    elif fmt == 'csv':
        tables.append(analyze_rows(csv_rows(path), 'csv', fiscal_year))
    elif fmt == 'pdf':
        tables.append(analyze_pdf_text(pdf_text(path), pdf_pages(path), fiscal_year))
    return summarize(tables), tables


def summarize(tables):
    with_table = [t for t in tables if t['kind'] in ('detailed', 'summary')]
    ready = [t for t in tables if t['ready']]
    best = (ready or [t for t in tables if t['kind'] == 'detailed'] or with_table or tables or [{}])[0]
    kinds = set(t['kind'] for t in tables)
    return dict(
        table_kind=next((k for k in ('detailed', 'summary', 'scanned', 'none') if k in kinds), 'none'),
        ready_to_process=bool(ready),
        n_tables=len(with_table),
        n_rows=sum(t['n_rows'] for t in tables if t['kind'] == 'detailed'),
        value_columns='; '.join('%s %s' % (v['kind'] or '?', v['year'] or '') for v in best.get('value_columns', [])),
        expected_columns=any(t['expected_columns'] for t in tables),
        # A budget may keep incomes and expenses on separate sheets
        income_sign=next((t['income_sign'] for t in ready or tables if t.get('income_sign')), None),
        expense_sign=next((t['expense_sign'] for t in ready or tables if t.get('expense_sign')), None),
        units=best.get('units'),
        issues=','.join(sorted(set(i for t in (ready or with_table or tables) for i in t['issues']))) or None,
    )
