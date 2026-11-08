"""Turns a report chapter (docx or pdf) into markdown.

docx goes through pandoc (keeps tables, lists and emphasis); pdf through pdftotext, with running headers and
footers dropped and lines rejoined into paragraphs. Formatting is inconsistent across reports and even within one
file, so nothing here tries to recover the document's structure beyond cosmetics: bold-only lines are shown as
headings, but no later step relies on that.
"""
import collections
import re
import subprocess

import sc_common as sc

TEXT_VERSION = 3


def docx_to_markdown(path):
    out = subprocess.run(['pandoc', '-f', 'docx', '-t', 'gfm-raw_html', '--wrap=none', path],
                         capture_output=True, timeout=600)
    if out.returncode != 0:
        raise RuntimeError('pandoc failed: %s' % out.stderr.decode('utf-8', 'replace')[:500])
    return tidy_markdown(out.stdout.decode('utf-8'))


BOLD_LINE = re.compile(r'^\s*(\*{2,3})([^*\n]{2,150}?)\1\s*$')
IMAGE = re.compile(r'!\[[^\]]*\]\([^)]*\)(\{[^}]*\})?')
PAGE_NUMBER = re.compile(r'^\s*[-–|]?\s*\d{1,4}\s*[-–|]?\s*$')


def tidy_markdown(md):
    md = sc.clean_text(md)
    md = IMAGE.sub('', md)
    lines = []
    for line in md.split('\n'):
        m = BOLD_LINE.match(line)
        if m:
            # Cosmetic only: bold-italic lines are usually chapter titles, bold ones section titles
            line = ('## ' if len(m.group(1)) == 3 else '### ') + m.group(2).strip()
        elif PAGE_NUMBER.match(line):
            continue
        lines.append(line.rstrip())
    md = '\n'.join(lines)
    return re.sub(r'\n{3,}', '\n\n', md).strip() + '\n'


def pdf_pages(path):
    out = subprocess.run(['pdftotext', '-enc', 'UTF-8', path, '-'], capture_output=True, timeout=600)
    if out.returncode != 0:
        raise RuntimeError('pdftotext failed: %s' % out.stderr.decode('utf-8', 'replace')[:500])
    return [sc.clean_text(p) for p in out.stdout.decode('utf-8', 'replace').split('\f')]


def page_count(path):
    out = subprocess.run(['pdfinfo', path], capture_output=True, timeout=120)
    m = re.search(r'^Pages:\s+(\d+)', out.stdout.decode('utf-8', 'replace'), re.M)
    return int(m.group(1)) if m else None


def running_lines(pages, min_share=0.3):
    """Short lines that repeat on many pages (running headers/footers), compared with digits masked out."""
    if len(pages) < 4:
        return set()
    counts = collections.Counter()
    for page in pages:
        seen = set()
        for line in page.split('\n'):
            key = re.sub(r'\d+', '#', line.strip())
            if key and len(key) < 120 and key not in seen:
                seen.add(key)
                counts[key] += 1
    return {k for k, c in counts.items() if c >= max(3, len(pages) * min_share)}


BULLET = re.compile(r'^\s*([•▪◦●\-–]|\(?\d{1,2}[.)]|\(?[א-ת][.)])\s')


def _legacy_font_chars():
    """Characters that old Hebrew fonts produce instead of letters: the font maps cp1255 byte values to Latin
    glyphs, which pdftotext reads as MacRoman ('‰' for ה, 'Ì' for ם...). The text also comes out in visual order and
    every נ becomes a space, so it can't be decoded back; such pages are transcribed from the PDF instead."""
    chars = set()
    for b in range(0x80, 0x100):
        latin = bytes([b]).decode('mac_roman')
        hebrew = bytes([b]).decode('cp1255', errors='ignore')
        if hebrew and '\u05d0' <= hebrew <= '\u05ea' and latin.strip():
            chars.add(latin)
    return chars


LEGACY_CHARS = _legacy_font_chars()


def garbled_share(text):
    """The share of a text's letters that look like legacy-font garbage."""
    letters = [c for c in text if c.isalpha() or c in LEGACY_CHARS]
    if not letters:
        return 0.0
    return sum(1 for c in letters if c in LEGACY_CHARS) / len(letters)


HEBREW_LETTER = re.compile('[\u05d0-\u05ea]')
FINAL_LETTER = re.compile('[םןךףץ]')


def final_letter_share(text):
    """Final letters (ם ן ך ף ץ) are about 5% of the letters of Hebrew text; some old fonts lose them altogether
    ('מכרזי' for 'מכרזים', '%' for 'ך'). None when the text has too little Hebrew to tell."""
    letters = len(HEBREW_LETTER.findall(text))
    if letters < 300:
        return None
    return len(FINAL_LETTER.findall(text)) / letters


def is_expected_char(c):
    return ('֐' <= c <= '׿' or 'יִ' <= c <= 'ﭏ'  # Hebrew
            or '؀' <= c <= 'ۿ' or 'ﭐ' <= c <= '﷿' or 'ﹰ' <= c <= '﻿'  # Arabic
            or ' ' <= c <= '⁯' or c in '₪€°×·')  # punctuation, directional marks


def foreign_share(text):
    """The share of a page's non-ASCII characters that are neither Hebrew, Arabic nor common punctuation. Fonts
    that pdftotext can't decode come out as symbols of other scripts: Latin-1 ('§£ª££ª¬'), Hebrew letters shifted
    into the IPA block ('ʬʥʧʡ'), and others. None when the text has too few non-ASCII characters to tell."""
    chars = [c for c in text if ord(c) > 0x7f and not c.isspace()]
    if len(chars) < 100:
        return None
    return sum(1 for c in chars if not is_expected_char(c)) / len(chars)


def broken_page(text, max_garbled=0.02, min_finals=0.012, max_foreign=0.3):
    """Whether pdftotext's text of a page is unusable: legacy-font garbage, foreign symbols, or missing final
    letters."""
    if garbled_share(text) > max_garbled:
        return True
    foreign = foreign_share(text)
    if foreign is not None and foreign > max_foreign:
        return True
    finals = final_letter_share(text)
    return finals is not None and finals < min_finals


def textless(text):
    return len(''.join(text.split())) < 30


def scanned_pages(pages):
    """The pages with no text layer, when most of the document has none (a scanned document); a blank page here and
    there in a document that has text is just blank."""
    empty = [i + 1 for i, p in enumerate(pages) if textless(p)]
    return empty if pages and len(empty) > len(pages) / 2 else []


def page_paragraphs(page, repeated):
    lines = [l.strip() for l in page.split('\n')]
    lines = [l for l in lines if not l or not (re.sub(r'\d+', '#', l) in repeated or PAGE_NUMBER.match(l))]
    lengths = sorted(len(l) for l in lines if len(l) > 15)
    typical = lengths[len(lengths) * 3 // 4] if lengths else 0
    paragraphs, current = [], []
    for line in lines:
        if not line:
            if current:
                paragraphs.append(' '.join(current))
                current = []
            continue
        if BULLET.match(line) and current:
            paragraphs.append(' '.join(current))
            current = []
        current.append(line)
        # pdftotext moves Hebrew punctuation around, so a paragraph's end is recognized by a short last line
        if typical and len(line) < typical * 0.7:
            paragraphs.append(' '.join(current))
            current = []
    if current:
        paragraphs.append(' '.join(current))
    return paragraphs


def pdf_pages_to_markdown(pages, repeated=None):
    repeated = running_lines(pages) if repeated is None else repeated
    merged = []
    for page in pages:
        for i, p in enumerate(page_paragraphs(page, repeated)):
            # The first paragraph of a page continues the previous page's last one, unless that one ended short
            if i == 0 and merged and merged[-1][1] and not BULLET.match(p):
                merged[-1] = (merged[-1][0] + ' ' + p, merged[-1][1])
            else:
                merged.append((p, False))
        if merged:
            merged[-1] = (merged[-1][0], _ends_full(page, repeated))
    md = '\n\n'.join(fix_punctuation(re.sub(r'\s{2,}', ' ', p)) for p, _ in merged)
    return md.strip() + '\n'


DISPLACED_PUNCTUATION = re.compile(r'(?<=[\u05d0-\u05ea"\'\)]) ([,.;:!?])(?=[\u05d0-\u05ea"\(])')


def fix_punctuation(s):
    """pdftotext puts the punctuation after a Hebrew word at the start of the next one ('ישראל ,ובכלל')."""
    return DISPLACED_PUNCTUATION.sub(r'\1 ', s)


def _ends_full(page, repeated):
    """Whether a page's last text line is full width, i.e. its paragraph likely continues on the next page."""
    lines = [l.strip() for l in page.split('\n') if l.strip()]
    lines = [l for l in lines if not (re.sub(r'\d+', '#', l) in repeated or PAGE_NUMBER.match(l))]
    if not lines:
        return False
    lengths = sorted(len(l) for l in lines if len(l) > 15)
    typical = lengths[len(lengths) * 3 // 4] if lengths else 0
    return bool(typical) and len(lines[-1]) >= typical * 0.7


def pdf_to_markdown(path):
    return pdf_pages_to_markdown(pdf_pages(path))


def to_markdown(path, ext):
    if ext == 'docx':
        return docx_to_markdown(path)
    if ext == 'pdf':
        return pdf_to_markdown(path)
    raise ValueError('Unsupported format %s' % ext)
