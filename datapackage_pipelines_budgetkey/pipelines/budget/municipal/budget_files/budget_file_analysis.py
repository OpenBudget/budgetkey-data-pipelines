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

ANALYZER_VERSION = 3
BALANCE_TOLERANCE = 0.05

MAX_ROWS = 20000
HEADER_SEARCH = 12
MIN_CODE_ROWS = 5

CODE = re.compile(r'^[1-9]\d{9}$')
# Some cities (Tel Aviv) use their own codes, chapter first: 11000/121/8 = פרק 11000, סעיף 121, check digit
SLASH_CODE = re.compile(r'^[1-9]\d{3,5}/\d{1,4}(/\d{1,2})?$')
YEAR = re.compile(r'(?<!\d)(19[9]\d|20[0-4]\d)(?!\d)')
THOUSANDS = re.compile(r'אלפי|באלפים|אלפים|אלש"?״?ח|בא?לפי|K ?₪|thousands|بالآلاف|آلاف', re.I)
NIS = re.compile(r'ש"?״?ח|₪|שקל|NIS|شيكل', re.I)
NOT_VALUE_HEADER = re.compile(r'פרק|סעיף|סוג|מס\'|מספר|קוד|כרטיס|משרות|תקנים|תקן|אחוז|%|שורה|מזהה', re.I)
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
    return s if CODE.match(s) or SLASH_CODE.match(s) else None


def chapter_of(code):
    """The 2-digit chapter (פרק): digits 2-3 of a 10-digit code, or the start of a slash-separated one."""
    if code and CODE.match(code):
        return code[1:3]
    if code and SLASH_CODE.match(code):
        return code[:2]


def chapter_kind(code):
    """'income' or 'expense' from the chapter of a budget code."""
    chapter = chapter_of(code)
    if chapter is None:
        return None
    chapter = int(chapter)
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
        if NOT_VALUE_HEADER.search(head) and (not YEAR.search(head) or STAFF.search(head)):
            continue
        if all(abs(number(rows[i][c]) or 0) < 1000 and float(number(rows[i][c]) or 0).is_integer() for i in data) \
                and not YEAR.search(head):
            continue  # small integers without a year in the header: chapter numbers and the like
        value_cols.append(c)
    if header is not None and any(YEAR.search(column_header(rows, header, above, c)) for c in value_cols):
        # Unlabelled numbers next to a labelled budget table are helper columns, not budget values
        value_cols = [c for c in value_cols if column_header(rows, header, above, c)]
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
        # Signs are judged on all value columns together: a single column may be empty for incomes or expenses
        by_kind = {}
        for i in data:
            kind = chapter_kind(code_of(rows[i][code_col]))
            for c in value_cols:
                value = number(rows[i][c])
                if kind and value is not None:
                    by_kind.setdefault(kind, []).append(value)
        result['income_sign'], result['expense_sign'] = signs(by_kind)
        # Units: the main column's own header and the title rows above the table, not other columns' headers
        header_text = ' '.join([name, column_header(rows, header, above, main) if header is not None else '']
                               + [text(c) for r in rows[:header or 0] for c in r])
        main_values = [number(rows[i][main]) for i in data if number(rows[i][main]) is not None]
        result['units'], result['units_source'] = units(header_text, main_values)
        if result['income_sign'] is None and result['expense_sign'] is None:
            result['issues'].append('sign-unknown')
        if result['units'] is None:
            result['issues'].append('units-unknown')
        if not any(v['year'] for v in result['value_columns']):
            result['issues'].append('no-years-in-headers')

    if composite:
        result['code_col'] = 'composite:' + '+'.join(str(c) for c in composite[1:] if c is not None)
        result['issues'].append('composite-code')
    result['rows'] = [dict(row=i + 1, code=code_of(rows[i][code_col]),
                           title=text(rows[i][result['title_col']]) if result['title_col'] is not None else '',
                           values=[number(rows[i][c]) for c in value_cols])
                      for i in data]
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


HEBREW = re.compile(r'[\u0590-\u05ff\u0600-\u06ff]')
WORD = re.compile(r'<page width="[\d.]+" height="[\d.]+">|'
                  r'<word xMin="([\d.]+)" yMin="([\d.]+)" xMax="([\d.]+)" yMax="([\d.]+)">(.*?)</word>')
ROW_TOLERANCE = 3
COLUMN_GAP = 30
HEADER_DISTANCE = 45


def pdf_words(path, max_pages=300):
    """Words with their boxes, per page. Right-to-left words come out in glyph order and are flipped back."""
    import html
    with tempfile.NamedTemporaryFile(suffix='.html') as out:
        subprocess.run(['pdftotext', '-bbox', '-l', str(max_pages), path, out.name],
                       check=True, timeout=300, capture_output=True)
        with open(out.name, encoding='utf-8', errors='replace') as f:
            content = f.read()
    pages = []
    for m in WORD.finditer(content):
        if m.group(1) is None:
            pages.append([])
            continue
        word = RTL_MARKS.sub('', html.unescape(m.group(5)))
        if HEBREW.search(word):
            word = word[::-1]
        x0, y0, x1, y1 = (float(v) for v in m.group(1, 2, 3, 4))
        if pages and word:
            pages[-1].append(dict(x=(x0 + x1) / 2, x0=x0, x1=x1, y=(y0 + y1) / 2, text=word))
    return pages


def visual_rows(words):
    rows = []
    for w in sorted(words, key=lambda w: w['y']):
        if rows and abs(rows[-1][0]['y'] - w['y']) <= ROW_TOLERANCE:
            rows[-1].append(w)
        else:
            rows.append([w])
    return rows


def split_row(row):
    """(code, title, values) for a table row: values are the outermost run of numbers, away from the code."""
    code_word = next((w for w in row if re.fullmatch(r'[1-9]\d{9}', w['text'])), None)
    if code_word is None:
        return None
    others = [w for w in row if w is not code_word]
    left = [w for w in others if w['x'] < code_word['x']]
    right = [w for w in others if w['x'] > code_word['x']]
    numeric = lambda w: number(w['text']) is not None and not YEAR.fullmatch(w['text'])
    # Values sit on the side of the code with more numbers (left in right-to-left tables)
    side = left if sum(map(numeric, left)) >= sum(map(numeric, right)) else right
    side = sorted(side, key=lambda w: -abs(w['x'] - code_word['x']))
    values = []
    for w in side:
        if not numeric(w):
            break
        values.append(w)
    title = [w for w in side if w not in values and not re.fullmatch(r'[-–]', w['text'])]
    title = ' '.join(w['text'] for w in sorted(title, key=lambda w: -w['x']))
    return code_word['text'], title, values


def cluster(xs):
    groups = []
    for x in sorted(xs):
        if groups and x - groups[-1][-1] <= COLUMN_GAP:
            groups[-1].append(x)
        else:
            groups.append([x])
    return [sum(g) / len(g) for g in groups]


STAFF = re.compile(r'תקנים|משרות|תקן')
PHRASE_GAP = 10


def header_phrases(rows):
    """Header words grouped into phrases ("תקציב 2022 מעודכן") by the gaps between them."""
    phrases = []
    for row in rows:
        words = sorted((w for w in row if number(w['text']) is None or YEAR.fullmatch(w['text'])),
                       key=lambda w: w['x0'])
        for w in words:
            if phrases and phrases[-1][-1]['y'] == w['y'] and w['x0'] - phrases[-1][-1]['x1'] <= PHRASE_GAP:
                phrases[-1].append(w)
            else:
                phrases.append([w])
    return [dict(x=(p[0]['x0'] + p[-1]['x1']) / 2, y=p[0]['y'],
                 text=' '.join(w['text'] for w in sorted(p, key=lambda w: -w['x']))) for p in phrases]


def column_key(label, used):
    column = describe_value_column(label)
    key = '%s %s' % (column['kind'], column['year']) if column['year'] else (label or 'column')
    n = 1
    while ('%s #%d' % (key, n) if n > 1 else key) in used:
        n += 1
    return '%s #%d' % (key, n) if n > 1 else key


def extract_pdf_table(pages):
    """Rows and value columns of a coded budget table, placing each value under its column by position."""
    columns, rows, previous = {}, [], []
    for page_no, words in enumerate(pages, 1):
        all_rows = visual_rows(words)
        page_rows = [(r, split_row(row)) for r, row in enumerate(all_rows)]
        page_rows = [(r, split) for r, split in page_rows if split]
        if not page_rows:
            continue
        xs = [w['x'] for _, (_, _, values) in page_rows for w in values]
        centers = [c for c in cluster(xs)
                   if sum(1 for x in xs if abs(x - c) <= COLUMN_GAP) >= max(3, len(page_rows) * 0.03)]
        if not centers:
            continue
        nearest = lambda x: min(range(len(centers)), key=lambda i: abs(centers[i] - x))

        # Header: phrases in the rows just above the first table row, each attached to the nearest column
        first = page_rows[0][0]
        heads = {i: [] for i in range(len(centers))}
        for phrase in header_phrases(all_rows[max(0, first - 4):first]):
            i = nearest(phrase['x'])
            if abs(centers[i] - phrase['x']) <= HEADER_DISTANCE:
                heads[i].append(phrase)
        labels = [' '.join(p['text'] for p in sorted(heads[i], key=lambda p: p['y'])) for i in range(len(centers))]
        if not any(YEAR.search(l) for l in labels) and previous:
            # Continuation pages repeat no header: take the label of the nearest column seen before
            labels = [min(previous, key=lambda p: abs(p[0] - c))[1]
                      if min(abs(p[0] - c) for p in previous) <= COLUMN_GAP else '' for c in centers]
        elif any(YEAR.search(l) for l in labels):
            previous = list(zip(centers, labels))

        keys, used = [], set()
        for label in labels:
            key = column_key(label, used)
            used.add(key)
            keys.append(key)
            if key not in columns and not STAFF.search(label):
                # A second column with the same kind and year on one page is ambiguous: keep it, but flag it
                columns[key] = dict(describe_value_column(label), order=len(columns), duplicate='#' in key)
        for _, (code, title, values) in page_rows:
            cells = {}
            for w in values:
                i = nearest(w['x'])
                if abs(centers[i] - w['x']) <= COLUMN_GAP and keys[i] in columns:
                    cells[keys[i]] = number(w['text'])
            rows.append(dict(row='page %d' % page_no, code=code, title=title, cells=cells))
    ordered = sorted(columns, key=lambda k: columns[k]['order'])
    for row in rows:
        cells = row.pop('cells')
        row['values'] = [cells.get(key) for key in ordered]
    return [{k: v for k, v in columns[key].items() if k != 'order'} for key in ordered], rows


def analyze_pdf(path, fiscal_year=None):
    pages = pdf_pages(path)
    result = analyze_pdf_text(pdf_text(path), pages, fiscal_year)
    if result['kind'] != 'detailed':
        return result
    columns, rows = extract_pdf_table(pdf_words(path))
    if len(rows) < MIN_CODE_ROWS or not columns:
        result['issues'].append('no-positions')
        return result
    result.update(value_columns=columns, rows=rows, n_rows=len(rows))
    result['issues'] = [i for i in result['issues'] if i != 'ragged-rows']
    main = next((n for n, c in enumerate(columns) if fiscal_year and c['year'] == fiscal_year), 0)
    by_kind = {}
    for row in rows:
        kind = chapter_kind(row['code'])
        for value in row['values']:
            if kind and value is not None:
                by_kind.setdefault(kind, []).append(value)
    result['income_sign'], result['expense_sign'] = signs(by_kind)
    result['units'], result['units_source'] = units(columns[main]['header'],
                                                    [r['values'][main] for r in rows if r['values'][main] is not None])
    has_years = any(c['year'] for c in columns)
    if not has_years and 'no-years-in-headers' not in result['issues']:
        result['issues'].append('no-years-in-headers')
    result['expected_columns'] = expected_columns(columns, fiscal_year)
    result['ready'] = bool(result['units'] and (result['income_sign'] or result['expense_sign']) and has_years)
    return result


def split_header(head, n_values):
    """Best-effort header text per value column: one chunk per year mentioned, in order of appearance."""
    chunks = re.split(r'\s{3,}', head.strip())
    with_year = [c for c in chunks if YEAR.search(c)]
    if len(with_year) >= n_values:
        return with_year[:n_values]
    return with_year + [''] * (n_values - len(with_year))


def budget_column(columns, fiscal_year):
    """The column holding the budget for the file's year: approved/proposed for that year, else the latest year."""
    candidates = [n for n, c in enumerate(columns) if not c.get('duplicate') and c['year']
                  and c['kind'] in ('approved', 'proposed', 'revised')]
    exact = [n for n in candidates if columns[n]['year'] == fiscal_year]
    if exact:
        return exact[0]
    if candidates:
        return max(candidates, key=lambda n: columns[n]['year'])


def balance(tables, fiscal_year):
    """Normalized incomes / expenses of the year's budget column across the file's ready tables."""
    totals = {'income': 0.0, 'expense': 0.0}
    for t in tables:
        column = budget_column(t['value_columns'], fiscal_year) if t['ready'] else None
        if column is None:
            continue
        factor = 1000 if t['units'] == 'thousands' else 1
        for row in t.get('rows') or []:
            kind, value = chapter_kind(row['code']), row['values'][column]
            if kind and value is not None:
                sign = -1 if t['%s_sign' % kind] == 'negative' else 1
                totals[kind] += value * factor * sign
    return totals


def check_balance(tables, fiscal_year):
    """A regular budget balances: flag files whose incomes and expenses disagree, which usually means
    misread columns or signs."""
    totals = balance(tables, fiscal_year)
    if not totals['income'] or not totals['expense']:
        return None, 'one-sided' if totals['income'] or totals['expense'] else None
    ratio = totals['income'] / totals['expense']
    return ratio, None if abs(1 - ratio) <= BALANCE_TOLERANCE else 'unbalanced'


def analyze_file(path, fmt, fiscal_year=None):
    """Returns (file summary, list of table analyses)."""
    tables = []
    if fmt in ('xlsx', 'xls'):
        for name, rows in excel_sheets(path):
            tables.append(analyze_rows(rows, name, fiscal_year))
    elif fmt == 'csv':
        tables.append(analyze_rows(csv_rows(path), 'csv', fiscal_year))
    elif fmt == 'pdf':
        tables.append(analyze_pdf(path, fiscal_year))
    summary = summarize(tables)
    summary['balance'], issue = check_balance(tables, fiscal_year)
    if issue:
        summary['issues'] = ','.join(filter(None, [summary['issues'], issue]))
    if issue == 'unbalanced':
        summary['ready_to_process'] = False
    return summary, tables


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
