"""Text helpers for the related-items linker: Hebrew name normalization and the deterministic extractors.

The extractors find references that need no judgement: corporation numbers (with their check digit), citations of
government decisions (number, plus the date or government when given), budget codes and tender numbers. Each returns
plain dicts with the matched text and its span, so the caller can cut an excerpt around it.
"""
import datetime
import re

HEB = 'א-ת'
NIQQUD = re.compile('[֑-ׇ]')
TAGS = re.compile(r'<[^>]+>')
ENTITIES = {'&quot;': '"', '&#39;': "'", '&amp;': '&', '&nbsp;': ' ', '&#13;': ' ', '&lt;': '<', '&gt;': '>'}


def html_to_text(s):
    """Strips the tags of the decisions' HTML, keeping paragraph breaks as newlines."""
    if not s:
        return ''
    s = re.sub(r'(?i)<br\s*/?>|</p>|</li>|</tr>|</h\d>', '\n', s)
    s = TAGS.sub(' ', s)
    for k, v in ENTITIES.items():
        s = s.replace(k, v)
    s = re.sub(r'[ \t\r\xa0]+', ' ', s)
    return re.sub(r'\n\s*\n+', '\n', s).strip()


def clean_quotes(s):
    return (s.replace('״', '"').replace('׳', "'").replace('“', '"').replace('”', '"').replace('„', '"')
            .replace('’', "'").replace('‘', "'").replace('`', "'").replace('–', '-').replace('—', '-')
            .replace('‑', '-').replace('־', '-'))


def excerpt(text, start, end, width=150):
    """The mention with up to `width` characters of context on each side, cut at word boundaries."""
    a = max(0, start - width)
    b = min(len(text), end + width)
    if a > 0:
        space = text.find(' ', a, start)
        a = space + 1 if space >= 0 else a
    if b < len(text):
        space = text.rfind(' ', end, b)
        b = space if space >= 0 else b
    return ('…' if a > 0 else '') + re.sub(r'\s+', ' ', text[a:b]).strip() + ('…' if b < len(text) else '')


# --- names ---------------------------------------------------------------------------------------------------

# Corporate suffixes and abbreviations that differ between how a body is written in a report and how the registrar
# (and budgetkey's entities) spells it. Applied to the quote-free, space-normalized form.
SUFFIXES = [
    r'\bבע ?מ\b', r'\bבערבון מוגבל\b', r'\(?\bע ?ר\b\)?', r'\(?\bחל ?צ\b\)?', r'\bחברה לתועלת הציבור\b',
    r'\blimited\b', r'\bltd\b', r'\binc\b',
]
ABBREVIATIONS = [
    (r'^מ ?מ\b', 'מועצה מקומית'), (r'^מ ?א\b', 'מועצה אזורית'), (r'^מוא\b', 'מועצה אזורית'),
    (r'^עירית\b', 'עיריית'), (r'\bבי ?ח\b', 'בית חולים'), (r'\bביה ?ח\b', 'בית החולים'),
    (r'\bרשות המיסים\b', 'רשות המסים'), (r'\bהאנרגייה\b', 'האנרגיה'), (r'\bתכנית\b', 'תוכנית'),
]


def normalize_name(name):
    """A comparison key for organization names: no niqqud, quotes, punctuation or corporate suffixes; standard
    spelling of municipal prefixes and a few common variants. Not for display."""
    if not name:
        return ''
    s = NIQQUD.sub('', clean_quotes(name)).lower()
    s = re.sub(r'(?<=[' + HEB + r'])["\'](?=[' + HEB + r'])', '', s)
    s = re.sub(r'["\'.,:;!?()\[\]{}<>]', ' ', s)
    s = re.sub(r'\s*[-/\\]\s*', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    for pattern in SUFFIXES:
        s = re.sub(pattern, ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    for pattern, repl in ABBREVIATIONS:
        s = re.sub(pattern, repl, s)
    return re.sub(r'\s+', ' ', s).strip()


# --- corporation numbers ---------------------------------------------------------------------------------------

def valid_corporation_number(number):
    """Israeli corporation numbers: 9 digits starting with 5, with the same check digit as ID numbers."""
    if not re.fullmatch(r'5\d{8}', number or ''):
        return False
    total = 0
    for i, digit in enumerate(int(d) for d in number):
        v = digit * (1 + i % 2)
        total += v - 9 if v > 9 else v
    return total % 10 == 0


CORP_NUMBER = re.compile(r'(?<![\d\-/.])(5\d)[-]?(\d{6})[-]?(\d)(?![\d\-/])')


def corporation_numbers(text):
    """Valid corporation numbers in the text (with separators 51-234567-8 or without)."""
    text = clean_quotes(text)
    out = []
    for m in CORP_NUMBER.finditer(text):
        number = ''.join(m.groups())
        if valid_corporation_number(number):
            out.append(dict(number=number, mention=m.group(0), start=m.start(), end=m.end()))
    return out


# --- dates -----------------------------------------------------------------------------------------------------

MONTHS = {
    'ינואר': 1, 'פברואר': 2, 'מרץ': 3, 'מרס': 3, 'אפריל': 4, 'מאי': 5, 'יוני': 6, 'יולי': 7, 'אוגוסט': 8,
    'ספטמבר': 9, 'אוקטובר': 10, 'נובמבר': 11, 'דצמבר': 12,
}
MONTH_RE = '|'.join(sorted(MONTHS, key=len, reverse=True))
NUMERIC_DATE = re.compile(r'(?<!\d)(\d{1,2})[./](\d{1,2})[./](\d{4}|\d{2})(?!\d)')
WORDS_DATE = re.compile(r'(?<!\d)(\d{1,2})\s+(?:ב|ל)?(' + MONTH_RE + r')\s*,?\s*(\d{4})(?!\d)')
MONTH_YEAR = re.compile(r'(?<![' + HEB + r'])[מבו]?-?(' + MONTH_RE + r')\s*,?\s*(\d{4})(?!\d)')


def _year(y):
    y = int(y)
    if y < 100:
        y += 2000 if y < 50 else 1900
    return y


def find_date(s):
    """The first Gregorian date in s, as (date, precision) with precision 'day' or 'month'; or (None, None)."""
    best = None
    for m in NUMERIC_DATE.finditer(s):
        try:
            best = (m.start(), datetime.date(_year(m.group(3)), int(m.group(2)), int(m.group(1))), 'day')
        except ValueError:
            continue
        break
    for m in WORDS_DATE.finditer(s):
        if best is None or m.start() < best[0]:
            try:
                best = (m.start(), datetime.date(int(m.group(3)), MONTHS[m.group(2)], int(m.group(1))), 'day')
            except ValueError:
                pass
        break
    if best is None:
        m = MONTH_YEAR.search(s)
        if m:
            best = (m.start(), datetime.date(int(m.group(2)), MONTHS[m.group(1)], 1), 'month')
    return (best[1], best[2]) if best else (None, None)


# --- government decisions --------------------------------------------------------------------------------------

# "החלטת ממשלה מס' 1577", "בהמשך להחלטת הממשלה מספר 3200 מיום 4.7.2025", "החלטות הממשלה 1234 ו-5678",
# "החלטה מס' 2397 של הממשלה ה-31". Committee numbers like (רהמ/12) after the number are skipped.
DECISION_HEAD = re.compile(
    r'(?<![' + HEB + r'])[ולבהשמכ]{0,3}'
    r'(?:החלט(?:ת|ות)\s+(?:ה)?ממשל(?:ה|ת\s+ישראל)|החלטה)'
    r'(?:\s+(?:ה)?(?:ממשלה|ממשלתית))?'
)
NUMBER_PREFIX = re.compile(r'\s*(?:(?:מס(?:\'|"|\.)|מספר|מס\b)\s*)?:?\s*')
DECISION_NUMBER = re.compile(r'(\d{1,5})(?![\d./])(?:\s*\((?:[' + HEB + r'"\']{2,6})/\d+\))?')
LIST_SEP = re.compile(r'\s*(?:,|ו-?|וכן|ו\s*-)\s*(?:מס(?:\'|")\s*)?')
GOV_NUMBER = re.compile(r'(?:של\s+)?\(?\s*(?:ה)?ממשלה\s+ה\s*-?\s*(\d{2})\b')
AMEND = re.compile(r'(?:לתקן|תיקון|ולתקן|יתוקנו|תתוקן|מתקנת|לשנות|שינוי)\s+(?:את\s+)?(?:[' + HEB + r']+\s+){0,3}$')
CANCEL = re.compile(r'(?:לבטל|ביטול|ולבטל|תבוטל|יבוטלו|מבטלת)\s+(?:את\s+)?(?:[' + HEB + r']+\s+){0,3}$')
ALIAS = re.compile(r'(?:להלן|לעיל)\s*[-:]?\s*[\'"]?\s*$')
# Phrases right before "החלטה" that make it someone else's decision, not the government's.
NOT_GOVERNMENT = re.compile(r'(?:מועצת|ועדת|ועדה|הנהלת|דירקטוריון|רשות|בית המשפט|נציב|מנכ"ל|השר|ראש)\s*$')


DATE_AT = re.compile(
    r'\s*(?:\([^)]{0,30}\)\s*)?,?\s*(?:של\s+הממשלה\s+ה\s*-?\s*\d{2}\s*,?\s*)?'
    r'(?:(?:מיום|מתאריך|בתאריך|שהתקבלה ביום|שהתקבלה בתאריך|מה-?|מ-)\s*'
    r'(?:[^()\d\n]{0,30}\((?=[^)]*\d{4}\)))?\s*'
    r'(?P<day>\d{1,2}[./]\d{1,2}[./](?:\d{4}|\d{2})|\d{1,2}\s+(?:ב|ל)?(?:' + MONTH_RE + r')\s*,?\s*\d{4})'
    r'|(?:מ|מחודש\s+|בחודש\s+)-?(?P<month>(?:' + MONTH_RE + r')\s*,?\s*\d{4}))'
)


def _date_at(text, pos):
    """A date clause ("מיום 4.7.2025", "מיום ד' בחשוון התשפ"ז (15 באוקטובר 2026)", "מיוני 2014") starting at pos:
    (date, precision, end) or None."""
    m = DATE_AT.match(text, pos)
    if not m:
        return None
    date, precision = find_date(m.group('day') or m.group('month'))
    if date is None:
        return None
    end = m.end()
    if text[end:end + 1] == ')':
        end += 1
    return date, precision, end


def decision_citations(text):
    """Citations of government decisions: [{number, date, date_precision, government, relation, mention, start,
    end}]. `date` and `government` are None unless the citation gives them; `relation` is cites/amends/cancels.
    In a list ("מס' 2443 ו-2444 מיום 15.02.2014") a date given once at the end applies to all the numbers."""
    text = clean_quotes(text)
    out = []
    for head in DECISION_HEAD.finditer(text):
        is_plain = 'ממשל' not in head.group(0)
        pos = NUMBER_PREFIX.match(text, head.end()).end()
        items = []
        while True:
            m = DECISION_NUMBER.match(text, pos)
            if not m:
                break
            pos = m.end()
            dated = _date_at(text, pos)
            if dated:
                pos = dated[2]
            items.append(dict(number=m.group(1), date=dated[0] if dated else None,
                              date_precision=dated[1] if dated else None, end=pos))
            sep = LIST_SEP.match(text, pos)
            if sep is None or not DECISION_NUMBER.match(text, sep.end()):
                break
            pos = sep.end()
        if not items:
            continue
        gov = GOV_NUMBER.search(text[head.end():pos + 60])
        if is_plain and not gov:
            # "החלטה מס' 12" alone could be any body's decision.
            continue
        before = text[max(0, head.start() - 60):head.start()]
        if ALIAS.search(before):
            # "(להלן – החלטה 1306)": a name given to a decision cited just before, not a citation of its own.
            continue
        if is_plain and NOT_GOVERNMENT.search(before):
            continue
        relation = 'amends' if AMEND.search(before) else 'cancels' if CANCEL.search(before) else 'cites'
        if items[-1]['date'] and not any(i['date'] for i in items[:-1]):
            for i in items[:-1]:
                i['date'], i['date_precision'] = items[-1]['date'], items[-1]['date_precision']
        for i in items:
            out.append(dict(
                number=i['number'], date=i['date'], date_precision=i['date_precision'],
                government=int(gov.group(1)) if gov else None, relation=relation,
                mention=text[head.start():i['end']].strip(), start=head.start(), end=i['end'],
            ))
    return out


# Israel's governments and the dates they were sworn in. Decision numbers restart with each government.
GOVERNMENTS = [
    (27, datetime.date(1996, 6, 18)), (28, datetime.date(1999, 7, 6)), (29, datetime.date(2001, 3, 7)),
    (30, datetime.date(2003, 2, 28)), (31, datetime.date(2006, 5, 4)), (32, datetime.date(2009, 3, 31)),
    (33, datetime.date(2013, 3, 18)), (34, datetime.date(2015, 5, 14)), (35, datetime.date(2020, 5, 17)),
    (36, datetime.date(2021, 6, 13)), (37, datetime.date(2022, 12, 29)),
]


def government_at(date):
    if date is None:
        return None
    current = None
    for number, start in GOVERNMENTS:
        if date >= start:
            current = number
    return current


def government_number(label):
    """'הממשלה ה- 34, בנימין נתניהו' -> 34."""
    m = re.search(r'ה\s*-?\s*(\d{2})', label or '')
    return int(m.group(1)) if m else None


# --- budget codes ----------------------------------------------------------------------------------------------

BUDGET_CODE = re.compile(
    r'(?<![' + HEB + r'])[ובלה]{0,2}(?:תקנ(?:ה|ת|ות)|סעיפ?י?|סעיף|תוכנית|תכנית|תחום|רכיב|קוד)'
    r'(?:\s+(?:ה)?תקציבי(?:ת|ים|ות)?)?(?:\s+(?:ה)?(?:תקציב|מס(?:\'|")|מספר))?\s*:?\s*'
    r'(?<!\d)(\d{2})[-. ](\d{2})(?:[-. ](\d{2}))?(?:[-. ](\d{2}))?(?![\d])'
)
BUDGET_CONTEXT = re.compile(r'תקציב|תקנ|₪|ש"ח|אלפי|מיליון')


def budget_codes(text):
    """Explicit budget codes ("תקנה 20-38-02-01", "תכנית תקציבית 20.38"), as budgetkey codes ('0020380201').
    Two-group codes ("20-38") count only when the sentence is about money, since they look like other numbers."""
    text = clean_quotes(text)
    out = []
    for m in BUDGET_CODE.finditer(text):
        groups = [g for g in m.groups() if g]
        if len(groups) == 2:
            sentence = text[max(0, m.start() - 120):m.end() + 120]
            if not BUDGET_CONTEXT.search(sentence):
                continue
        code = '00' + ''.join(groups)
        out.append(dict(code=code, mention=m.group(0).strip(), start=m.start(), end=m.end()))
    return out


# --- tenders ---------------------------------------------------------------------------------------------------

TENDER_NUMBER = re.compile(
    r'(?<![' + HEB + r'])[ובלהש]?מכרז(?:\s+(?:פומבי|מסגרת|מרכזי|דו-?שלבי|בינלאומי))*\s*'
    r'(?:(?:(?:מס(?:\'|"|\.)|מספר)\s*)?([0-9][0-9A-Za-z]*(?:[/\-][0-9A-Za-z]+)+)|(?:מס(?:\'|"|\.)|מספר)\s*(\d{3,}))'
)


def tender_numbers(text):
    """Tender numbers ("מכרז מס' 33/2006", "מכרז פומבי 12-2019"), with digits and separators only."""
    text = clean_quotes(text)
    return [dict(number=m.group(1) or m.group(2), mention=m.group(0).strip(), start=m.start(), end=m.end())
            for m in TENDER_NUMBER.finditer(text)]
