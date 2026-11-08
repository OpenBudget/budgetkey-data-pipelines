"""Turns each report chapter (an audit) into markdown and extracts its findings with Gemini.

For every chapter published from MIN_YEAR on:
1. Markdown: from the chapter's docx (pandoc) when there is one, otherwise from its pdf (pdftotext). Pages set in
   old fonts that pdftotext can't read are transcribed by Gemini from the PDF itself.
2. Analysis: Gemini reads the whole markdown and returns the audit's metadata and its findings as JSON. Chapters
   too long for one call are read in overlapping windows; nothing relies on the document's headings, since their
   formatting is inconsistent even within one file.
3. Validation: each finding carries a verbatim excerpt, which must be found in the text; findings whose excerpt
   isn't found get one repair call and are otherwise kept with verified=False.

The markdown and the analysis are cached in budgetkey-files next to the chapter's source file
(<object>.md, <object>.text.json, <object>.analysis.json), keyed on TEXT_VERSION / ANALYZER_VERSION.
"""
import difflib
import hashlib
import json
import logging
import os
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor

import dataflows as DF

import sc_common as sc
import chapter_text as ct
from mevaker_scraper import file_object_name

from datapackage_pipelines_budgetkey.common import llm

ANALYZER_VERSION = 2
MIN_YEAR = 2000
REPORTS_PATH = '/var/datapackages/state_comptroller/reports'
OUT_PATH = '/var/datapackages/state_comptroller/chapters'
GARBLED_PAGE = 0.02
TRANSCRIBE_WINDOW = 6
MAX_CHARS = 350_000
WINDOW_CHARS = 280_000
WINDOW_OVERLAP = 15_000
THREADS = int(os.environ.get('STATE_COMPTROLLER_THREADS') or 6)

USAGE = llm.Usage()


# --- text ---------------------------------------------------------------------------------------------------

TRANSCRIBE_PROMPT = '''These are pages {first}-{last} of a Hebrew report by the Israeli State Comptroller.
Transcribe every page faithfully into Markdown, in Hebrew, in reading order. Do not summarize, translate or omit
anything except running headers, running footers and page numbers. Join the lines of each paragraph into one line.
Render headings as Markdown headings, tables as Markdown tables, and footnotes as plain paragraphs at the end of
their page. Return one entry per page, numbered as in this file (1 = first page).'''

TRANSCRIBE_SCHEMA = {
    'type': 'object',
    'properties': {'pages': {'type': 'array', 'items': {
        'type': 'object',
        'properties': {'page': {'type': 'integer'}, 'markdown': {'type': 'string'}},
        'required': ['page', 'markdown'],
    }}},
    'required': ['pages'],
}


def transcribe(pdf_path, page_numbers):
    """Gemini transcriptions of the given 1-based pages, as {page number: markdown}."""
    runs, run = [], []
    for n in sorted(page_numbers):
        if run and (n != run[-1] + 1 or len(run) >= TRANSCRIBE_WINDOW):
            runs.append(run)
            run = []
        run.append(n)
    if run:
        runs.append(run)
    result = {}
    with tempfile.TemporaryDirectory() as tmp:
        while runs:
            run = runs.pop(0)
            path = sc.pdf_slice(pdf_path, run[0], run[-1], os.path.join(tmp, 'slice.pdf'))
            with open(path, 'rb') as f:
                data = f.read()
            out = None
            # Gemini now and then returns an empty response, and blocks some PDF pages outright
            # (block_reason OTHER) while reading the same page fine as an image: try again, then in halves,
            # then a single page as an image
            attempts = [('application/pdf', data)] * 2
            if len(run) == 1:
                attempts.append(('image/png', sc.page_image(pdf_path, run[0], tmp)))
            for mime_type, payload in attempts:
                try:
                    _, out = llm.complete_with_file(
                        TRANSCRIBE_PROMPT.format(first=run[0], last=run[-1]), payload, mime_type,
                        schema=TRANSCRIBE_SCHEMA, model=llm.MODEL_FAST, temperature=0, usage=USAGE, thinking='low')
                    break
                except Exception as e:
                    logging.warning('Transcription of pages %s-%s (%s) failed (%s)', run[0], run[-1], mime_type, e)
            if out is None:
                if len(run) > 1:
                    half = len(run) // 2
                    runs[:0] = [run[:half], run[half:]]
                continue
            for page in out.get('pages') or []:
                n = run[0] + int(page.get('page', 0)) - 1
                if n in run and page.get('markdown', '').strip():
                    result[n] = page['markdown'].strip()
    return result


def pdf_markdown(path):
    """Markdown for a PDF, and which pages had to be transcribed."""
    pages = ct.pdf_pages(path)
    repeated = ct.running_lines(pages)
    garbled = sorted({i + 1 for i, p in enumerate(pages) if ct.broken_page(p, GARBLED_PAGE)} | set(ct.scanned_pages(pages)))
    transcribed = transcribe(path, garbled) if garbled else {}
    parts, plain = [], []
    for i, page in enumerate(pages, 1):
        if i in transcribed:
            if plain:
                parts.append(ct.pdf_pages_to_markdown(plain, repeated))
                plain = []
            parts.append(transcribed[i] + '\n')
        elif i not in garbled:
            plain.append(page)
    if plain:
        parts.append(ct.pdf_pages_to_markdown(plain, repeated))
    md = '\n'.join(parts)
    return md, dict(pages=len(pages), garbled_pages=garbled, transcribed_pages=sorted(transcribed))


def source_file(chapter):
    """The (kind, url, object name) of the file to read a chapter from: its docx if any, else its pdf."""
    for ext in ('docx', 'pdf'):
        url = chapter.get('full_%s_url' % ext)
        if url:
            return ext, url, file_object_name(chapter['pub_id'], url)
    return None, None, None


def chapter_markdown(chapter):
    """(markdown, text meta, md object name) for a chapter, from the store's cache when up to date."""
    ext, url, name = source_file(chapter)
    if not name:
        return None, dict(status='no-file'), None
    meta = sc.read_json(name + '.text.json')
    if meta and meta.get('version') == ct.TEXT_VERSION:
        md = sc.read_object(name + '.md')
        if md is not None:
            return md.decode('utf-8'), meta, name + '.md'
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, 'file.' + ext)
        if not sc.fetch_stored(name, url, path):
            return None, dict(status='download-failed'), None
        meta = dict(version=ct.TEXT_VERSION, source=ext, source_url=url)
        md = None
        if ext == 'docx':
            try:
                md = ct.docx_to_markdown(path)
            except Exception as e:
                logging.warning('pandoc failed on %s: %s', url, e)
        if md is None or len(md.strip()) < 200:
            pdf_url = chapter.get('full_pdf_url')
            if ext == 'docx' and pdf_url:
                pdf_name = file_object_name(chapter['pub_id'], pdf_url)
                pdf_path = os.path.join(tmp, 'file.pdf')
                if sc.fetch_stored(pdf_name, pdf_url, pdf_path):
                    md, pdf_meta = pdf_markdown(pdf_path)
                    meta.update(pdf_meta, source='pdf', source_url=pdf_url)
            elif ext == 'pdf':
                md, pdf_meta = pdf_markdown(path)
                meta.update(pdf_meta)
    if not md or not md.strip():
        meta['status'] = 'no-text'
        return None, meta, None
    meta['status'] = 'ok'
    meta['chars'] = len(md)
    sc.write_text(name + '.md', md, 'md')
    # Pages Gemini failed to transcribe are left out; without the meta file the next run transcribes them again
    if set(meta.get('garbled_pages') or []) <= set(meta.get('transcribed_pages') or []):
        sc.write_json(name + '.text.json', meta)
    return md, meta, name + '.md'


# --- analysis -----------------------------------------------------------------------------------------------

STR = {'type': 'string'}
STRS = {'type': 'array', 'items': STR}

ANALYSIS_SCHEMA = {
    'type': 'object',
    'properties': {
        'summary': STR,
        'audit_subject': STR,
        'audit_period': STR,
        'audit_period_start_year': {'type': 'integer'},
        'audit_period_end_year': {'type': 'integer'},
        'audited_bodies': {'type': 'array', 'items': {
            'type': 'object',
            'properties': {'name': STR, 'role': {'type': 'string', 'enum': ['main', 'secondary']}},
            'required': ['name', 'role'],
        }},
        'follow_up': {'type': 'string', 'enum': ['follow_up_audit', 'includes_follow_up', 'none']},
        'earlier_reports': {'type': 'array', 'items': {
            'type': 'object',
            'properties': {'report': STR, 'year': {'type': 'integer'}, 'title': STR,
                           'relation': {'type': 'string', 'enum': ['followed_up', 'referenced']}},
            'required': ['report', 'relation'],
        }},
        'topics': STRS,
        'key_figures': {'type': 'array', 'items': {
            'type': 'object', 'properties': {'label': STR, 'value': STR}, 'required': ['label', 'value'],
        }},
        'findings': {'type': 'array', 'items': {
            'type': 'object',
            'properties': {
                'title': STR,
                'description': STR,
                'recommendation': STR,
                'responsible_bodies': STRS,
                'finding_type': {'type': 'string', 'enum': ['deficiency', 'positive', 'observation']},
                'severity': {'type': 'string', 'enum': ['high', 'medium', 'low']},
                'body_responses': {'type': 'array', 'items': {
                    'type': 'object',
                    'properties': {'body': STR, 'date': STR, 'summary': STR},
                    'required': ['body', 'summary'],
                }},
                'followup_status': {'type': 'string', 'enum': [
                    'rectified', 'mostly_rectified', 'partially_rectified', 'not_rectified', 'not_applicable']},
                'followup_status_text': STR,
                'section': STR,
                'excerpt': STR,
            },
            'required': ['title', 'description', 'finding_type', 'severity', 'followup_status', 'excerpt'],
        }},
    },
    'required': ['summary', 'audit_subject', 'audited_bodies', 'follow_up', 'earlier_reports', 'topics',
                 'findings'],
}

ANALYSIS_PROMPT = '''You are analyzing one chapter of a report by the Israeli State Comptroller (מבקר המדינה).
The chapter was published on {date} in "{publication}". Its title is "{title}". The comptroller's site lists
these audited bodies: {bodies}.

The text below was extracted automatically from the chapter's Word or PDF file. Its formatting is inconsistent:
headings, numbering and section structure vary within the document and may be missing or wrong, so rely on the
content, not on the layout.

Extract, writing every value in Hebrew (except enum values):
- summary: 3-6 sentences on what was audited and the main conclusions.
- audit_subject: a short phrase naming the subject of the audit.
- audit_period: the period the audit covered, as stated (e.g. "2021 - 2024"), plus its start/end years if stated.
- audited_bodies: every audited body, with role main (the chapter is mainly about it) or secondary.
- follow_up: follow_up_audit if the chapter is a follow-up audit (ביקורת מעקב / ממצאי מעקב) checking whether the
  deficiencies of an earlier comptroller report were rectified; includes_follow_up if it is a new audit that also
  checks the rectification of deficiencies found in an earlier comptroller report; none otherwise. Merely
  mentioning or quoting an earlier report, or the body's own internal follow-up, is not a follow-up.
- earlier_reports: every earlier State Comptroller report the chapter refers to: report (its name, e.g. "דוח שנתי
  68ב"), year, title (the earlier chapter's title), and relation: followed_up if this chapter checks the
  rectification of its deficiencies, referenced otherwise. As far as they are stated.
- topics: 3-8 short subject tags.
- key_figures: up to 8 key figures stated in the text (label and value, with units).
- findings: every distinct finding of the audit - each deficiency (ליקוי) the comptroller found, and noteworthy
  positive findings. Do not merge distinct findings, and do not split one finding into several. For each:
  title (a short headline), description (what was found, with the important facts and figures),
  recommendation (what the comptroller recommends about it, if anything), responsible_bodies,
  finding_type, severity (your judgment of its importance), body_responses (what each audited body answered
  about this finding - "X מסר בתשובתו..." - with the date of the answer if stated), followup_status (in a
  follow-up audit: to what extent the deficiency found in the earlier audit was rectified; otherwise
  not_applicable) with followup_status_text (the comptroller's words on it), section (the heading it appears
  under, if any), and excerpt: a verbatim quote of 1-3 consecutive sentences from the text that states the
  finding, copied exactly as it appears.
{extra}
TEXT:
{text}'''

WINDOW_EXTRA = '''
This is part {part} of {parts} of a long chapter (characters {start}-{end} of {total}). Extract only the findings
stated in this part. Findings already extracted from earlier parts (do not repeat them unless this part adds to
them): {previous}
'''

REPAIR_SCHEMA = {
    'type': 'object',
    'properties': {'excerpts': {'type': 'array', 'items': {
        'type': 'object', 'properties': {'index': {'type': 'integer'}, 'excerpt': STR}, 'required': ['index', 'excerpt'],
    }}},
    'required': ['excerpts'],
}

REPAIR_PROMPT = '''Below is a chapter of an Israeli State Comptroller report, and a numbered list of findings that
were extracted from it. For each finding, copy from the text, exactly and verbatim, 1-3 consecutive sentences that
state it. If a finding is not stated in the text at all, return an empty excerpt for it.

FINDINGS:
{findings}

TEXT:
{text}'''


def windows(md):
    if len(md) <= MAX_CHARS:
        return [(0, len(md))]
    spans, start = [], 0
    while start < len(md):
        end = min(len(md), start + WINDOW_CHARS)
        if end < len(md):
            cut = md.rfind('\n\n', start + WINDOW_CHARS // 2, end)
            end = cut if cut > 0 else end
        spans.append((start, end))
        if end >= len(md):
            break
        start = max(end - WINDOW_OVERLAP, start + 1)
    return spans


def same_finding(a, b):
    ta, tb = sc.normalize_for_match(a.get('title')), sc.normalize_for_match(b.get('title'))
    da, db = sc.normalize_for_match(a.get('description'))[:300], sc.normalize_for_match(b.get('description'))[:300]
    return (difflib.SequenceMatcher(None, ta, tb).ratio() > 0.85 or
            difflib.SequenceMatcher(None, da, db).ratio() > 0.8)


def analyze_text(chapter, md):
    bodies = ', '.join(b['name'] for b in chapter.get('audited_bodies') or [] if b.get('name')) or 'לא צוינו'
    spans = windows(md)
    result, findings = None, []
    for part, (start, end) in enumerate(spans, 1):
        extra = ''
        if len(spans) > 1:
            previous = '; '.join(f['title'] for f in findings) or 'none'
            extra = WINDOW_EXTRA.format(part=part, parts=len(spans), start=start, end=end, total=len(md),
                                        previous=previous)
        prompt = ANALYSIS_PROMPT.format(date=chapter.get('publish_date'), publication=chapter.get('publication_title'),
                                        title=chapter.get('title'), bodies=bodies, extra=extra, text=md[start:end])
        _, out = llm.complete(prompt, schema=ANALYSIS_SCHEMA, model=llm.MODEL_FAST, temperature=0, usage=USAGE,
                              thinking='low')
        if result is None:
            result = out
        for f in out.get('findings') or []:
            if not any(same_finding(f, g) for g in findings):
                findings.append(f)
    result['findings'] = findings
    result['windows'] = len(spans)
    return result


def verify(md, findings):
    """Marks each finding verified when its excerpt is found in the text; asks once more for missing excerpts."""
    norm = sc.normalize_for_match(md)
    for f in findings:
        f['verified'] = sc.fuzzy_contains(norm, f.get('excerpt') or '')
    missing = [i for i, f in enumerate(findings) if not f['verified']]
    if missing and len(md) <= MAX_CHARS:
        listing = '\n'.join('%d. %s - %s' % (i, findings[i]['title'], findings[i].get('description', '')[:300])
                            for i in missing)
        try:
            _, out = llm.complete(REPAIR_PROMPT.format(findings=listing, text=md), schema=REPAIR_SCHEMA,
                                  model=llm.MODEL_FAST, temperature=0, usage=USAGE, thinking='low')
            for e in out.get('excerpts') or []:
                i = e.get('index')
                if i in missing and e.get('excerpt') and sc.fuzzy_contains(norm, e['excerpt']):
                    findings[i]['excerpt'] = e['excerpt']
                    findings[i]['verified'] = True
        except Exception as e:
            logging.warning('Excerpt repair failed: %s', e)
    return findings


def language(text):
    """'he', 'en' or 'ar', by the letters most of the text is written in."""
    counts = dict(he=len(re.findall('[\u05d0-\u05ea]', text)), en=len(re.findall('[A-Za-z]', text)),
                  ar=len(re.findall('[\u0621-\u064a]', text)))
    return max(counts, key=counts.get) if any(counts.values()) else None


def analyze_chapter(chapter):
    """Returns (audit row, finding rows) for a chapter."""
    row = dict(chapter_id=chapter['chapter_id'], analysis_status=None)
    try:
        md, meta, md_name = chapter_markdown(chapter)
    except Exception:
        logging.exception('Failed to get the text of chapter %s', chapter['chapter_id'])
        md, meta, md_name = None, dict(status='text-failed'), None
    row.update(text_source=meta.get('source'), transcribed_pages=len(meta.get('transcribed_pages') or []),
               text_md=md, md_url=sc.stored_url(md_name) if md_name else None)
    if md is None:
        row['analysis_status'] = meta.get('status')
        return row, []
    row['language'] = language(md)
    if row['language'] != 'he':
        # English and Arabic translations of chapters (recent reports have them); their findings are the Hebrew
        # chapter's, so they aren't read again
        row['analysis_status'] = 'not-hebrew'
        return row, []
    _, _, name = source_file(chapter)
    if meta.get('garbled_pages') and set(meta['garbled_pages']) - set(meta.get('transcribed_pages') or []):
        row['analysis_status'] = 'partial-text'
    text_hash = hashlib.sha256(md.encode('utf-8')).hexdigest()
    cached = sc.read_json(name + '.analysis.json')
    if cached and cached.get('version') == ANALYZER_VERSION and cached.get('text_hash') == text_hash:
        result = cached['result']
    else:
        try:
            result = analyze_text(chapter, md)
            result['findings'] = verify(md, result['findings'])
        except Exception:
            logging.exception('Failed to analyze chapter %s', chapter['chapter_id'])
            row['analysis_status'] = 'analysis-failed'
            return row, []
        sc.write_json(name + '.analysis.json', dict(version=ANALYZER_VERSION, text_hash=text_hash, result=result))
    findings = result.get('findings') or []
    verified = sum(1 for f in findings if f.get('verified'))
    status = row['analysis_status'] or 'ok'
    if not findings:
        status = 'no-findings'
    elif verified < len(findings) * 0.6:
        status = 'low-verification'
    follow_up = result.get('follow_up') or 'none'
    followed = next((r for r in result.get('earlier_reports') or [] if r.get('relation') == 'followed_up'), {})
    row.update(
        summary=result.get('summary'),
        audit_subject=result.get('audit_subject'),
        audit_period=result.get('audit_period'),
        audit_period_start_year=result.get('audit_period_start_year') or None,
        audit_period_end_year=result.get('audit_period_end_year') or None,
        llm_audited_bodies=result.get('audited_bodies') or [],
        follow_up=follow_up,
        is_follow_up=follow_up in ('follow_up_audit', 'includes_follow_up'),
        follows_up_report=followed.get('report') or None,
        follows_up_year=followed.get('year') or None,
        follows_up_title=followed.get('title') or None,
        earlier_reports=result.get('earlier_reports') or [],
        topics=result.get('topics') or [],
        key_figures=result.get('key_figures') or [],
        n_findings=len(findings),
        n_verified=verified,
        analysis_status=status,
    )
    rows = []
    for seq, f in enumerate(findings, 1):
        rows.append(dict(
            finding_id='%s-%d' % (chapter['chapter_id'], seq),
            chapter_id=chapter['chapter_id'],
            pub_id=chapter['pub_id'],
            publish_date=chapter.get('publish_date'),
            chapter_title=chapter.get('title'),
            page_url=chapter.get('page_url'),
            seq=seq,
            title=f.get('title'),
            description=f.get('description'),
            recommendation=f.get('recommendation') or None,
            responsible_bodies=f.get('responsible_bodies') or [],
            finding_type=f.get('finding_type'),
            severity=f.get('severity'),
            body_responses=f.get('body_responses') or [],
            followup_status=f.get('followup_status'),
            followup_status_text=f.get('followup_status_text') or None,
            section=f.get('section') or None,
            excerpt=f.get('excerpt'),
            verified=bool(f.get('verified')),
        ))
    return row, rows


def year_of(chapter):
    d = chapter.get('publish_date')
    if isinstance(d, str):
        return int(d[:4]) if d[:4].isdigit() else None
    return d.year if d else None


def analyze(chapters, min_year=MIN_YEAR, limit=None, chapter_ids=None):
    selected = [c for c in chapters if (year_of(c) or 0) >= min_year]
    if chapter_ids:
        selected = [c for c in selected if c['chapter_id'] in chapter_ids]
    if limit:
        selected = selected[:limit]
    logging.info('Analyzing %d of %d chapters', len(selected), len(chapters))
    audits, findings = {}, []

    def one(chapter):
        row, rows = analyze_chapter(chapter)
        logging.info('%s %s: %s, %d findings (%s)', chapter['chapter_id'], chapter.get('title'),
                     row.get('analysis_status'), len(rows), row.get('text_source'))
        return row, rows

    with ThreadPoolExecutor(THREADS) as executor:
        for row, rows in executor.map(one, selected):
            audits[row['chapter_id']] = row
            findings.extend(rows)
    logging.info('Gemini usage: %s', json.dumps(USAGE.by_model))
    failed = sum(1 for r in audits.values() if r['analysis_status'] in ('analysis-failed', 'text-failed'))
    if selected and failed > len(selected) * 0.2:
        raise RuntimeError('%d of %d chapters failed' % (failed, len(selected)))
    return audits, findings


CHAPTER_COPY = ['chapter_id', 'pub_id', 'chapter_order', 'title', 'page_url', 'publish_date', 'publication_title',
                'publication_type', 'report_code', 'main_body', 'audited_bodies', 'full_pdf_url', 'full_docx_url',
                'full_pdf_stored', 'full_docx_stored', 'abstract_pdf_stored', 'abstract_docx_stored']
AUDIT_FIELDS = [
    ('chapter_id', 'integer'), ('pub_id', 'integer'), ('chapter_order', 'integer'), ('title', 'string'),
    ('page_url', 'string'), ('publish_date', 'date'), ('publication_title', 'string'),
    ('publication_type', 'string'), ('report_code', 'string'), ('main_body', 'string'), ('audited_bodies', 'array'),
    ('full_pdf_url', 'string'), ('full_docx_url', 'string'), ('full_pdf_stored', 'string'),
    ('full_docx_stored', 'string'), ('abstract_pdf_stored', 'string'), ('abstract_docx_stored', 'string'),
    ('language', 'string'),
    ('summary', 'string'), ('audit_subject', 'string'), ('audit_period', 'string'),
    ('audit_period_start_year', 'integer'), ('audit_period_end_year', 'integer'), ('llm_audited_bodies', 'array'),
    ('follow_up', 'string'), ('is_follow_up', 'boolean'), ('follows_up_report', 'string'),
    ('follows_up_year', 'integer'), ('follows_up_title', 'string'), ('earlier_reports', 'array'), ('topics', 'array'),
    ('key_figures', 'array'), ('n_findings', 'integer'),
    ('n_verified', 'integer'), ('text_source', 'string'), ('transcribed_pages', 'integer'), ('md_url', 'string'),
    ('text_md', 'string'), ('analysis_status', 'string'),
]
FINDING_FIELDS = [
    ('finding_id', 'string'), ('chapter_id', 'integer'), ('pub_id', 'integer'), ('publish_date', 'date'),
    ('chapter_title', 'string'), ('page_url', 'string'), ('seq', 'integer'), ('title', 'string'),
    ('description', 'string'), ('recommendation', 'string'), ('responsible_bodies', 'array'),
    ('finding_type', 'string'), ('severity', 'string'), ('body_responses', 'array'), ('followup_status', 'string'),
    ('followup_status_text', 'string'), ('section', 'string'), ('excerpt', 'string'), ('verified', 'boolean'),
]


def audit_rows(chapters, audits):
    for c in chapters:
        a = audits.get(c['chapter_id'])
        if a is not None:
            row = {k: c.get(k) for k in CHAPTER_COPY}
            row.update(a)
            yield row


def load_chapters(path):
    return DF.Flow(DF.load(os.path.join(path, 'datapackage.json'), resources=['chapters'])).results()[0][0]


def output_flow(chapters, audits, findings):
    return DF.Flow(
        sc.typed_resource(list(audit_rows(chapters, audits)), 'audits', AUDIT_FIELDS, ['chapter_id']),
        sc.typed_resource(findings, 'findings', FINDING_FIELDS, ['finding_id']),
    )


def flow(*_):
    from datapackage_pipelines_budgetkey.common.dump_to_sql_atomic import dump_to_sql_atomic
    chapters = load_chapters(REPORTS_PATH)
    limit = int(os.environ.get('STATE_COMPTROLLER_LIMIT') or 0) or None
    audits, findings = analyze(chapters, limit=limit)
    return DF.Flow(
        output_flow(chapters, audits, findings),
        DF.update_resource(None, **{'dpp:streaming': True}),
        DF.dump_to_path(OUT_PATH),
        dump_to_sql_atomic(dict(
            state_comptroller_audits={'resource-name': 'audits'},
            state_comptroller_findings={'resource-name': 'findings'},
        )),
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    for noisy in ('httpx', 'httpcore', 'google_genai', 'urllib3'):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    chapters = load_chapters(os.environ.get('REPORTS', REPORTS_PATH))
    ids = {int(i) for i in os.environ['CHAPTER_IDS'].split(',')} if os.environ.get('CHAPTER_IDS') else None
    limit = int(os.environ['LIMIT']) if os.environ.get('LIMIT') else None
    audits, findings = analyze(chapters, int(os.environ.get('MIN_YEAR') or MIN_YEAR), limit, ids)
    DF.Flow(output_flow(chapters, audits, findings), DF.dump_to_path(os.environ.get('OUT', 'state_comptroller_chapters_out'))).process()
