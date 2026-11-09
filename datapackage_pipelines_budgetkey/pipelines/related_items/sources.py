"""Source adapters: turn each source's rows into the linker's documents (see linker.py for the shape)."""
import json
import re

from datapackage_pipelines_budgetkey.pipelines.related_items import ri_text as T
from datapackage_pipelines_budgetkey.pipelines.related_items.linker import as_date


def _list(v):
    if isinstance(v, str):
        try:
            return json.loads(v) if v else []
        except ValueError:
            return []
    return v or []


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# --- State Comptroller ---------------------------------------------------------------------------------------

# The audits the indexer makes documents of (state_comptroller/es_documents.py: Hebrew, analyzed). A link from or to
# any other chapter would point at a page that doesn't exist.
INDEXED_STATUSES = ('ok', 'low-verification', 'partial-text', 'no-findings')


def indexed_audit(a):
    status = a.get('analysis_status')
    return (a.get('language') or 'he') == 'he' and (not status or status in INDEXED_STATUSES)


def comptroller_finding_text(f):
    lines = ['[{}] {}'.format(f['finding_id'], f.get('title') or '')]
    for key, label in (('description', 'ממצא'), ('recommendation', 'המלצה'), ('excerpt', 'מתוך הדוח')):
        if f.get(key):
            lines.append('{}: {}'.format(label, f[key]))
    bodies = _list(f.get('responsible_bodies'))
    if bodies:
        lines.append('גופים אחראים: ' + ', '.join(bodies))
    return '\n'.join(lines)


def earlier_reports(a):
    """The earlier comptroller reports a chapter cites or follows up on: `earlier_reports` when the chapters
    pipeline has it, else the single follow-up reference of its older schema."""
    refs = _list(a.get('earlier_reports'))
    if not refs and a.get('follows_up_report') and a.get('follows_up_title'):
        refs = [dict(report=a['follows_up_report'], year=_int(a.get('follows_up_year')),
                     title=a['follows_up_title'], relation='followed_up')]
    return [r for r in refs if isinstance(r, dict) and r.get('title')]


def comptroller_documents(audits, findings):
    """One document per indexed (Hebrew, analyzed) audit chapter; its findings are the parts."""
    by_chapter = {}
    for f in findings:
        by_chapter.setdefault(str(f['chapter_id']), []).append(f)
    for a in audits:
        if not indexed_audit(a):
            continue
        chapter_id = str(a['chapter_id'])
        fs = sorted(by_chapter.get(chapter_id, []), key=lambda f: _int(f.get('seq')) or 0)
        audited = _list(a.get('audited_bodies'))
        figures = _list(a.get('key_figures'))
        llm_text = '\n'.join(x for x in [
            'כותרת: ' + (a.get('title') or ''),
            'דוח: {} ({})'.format(a.get('publication_title') or '', a.get('publish_date') or ''),
            'גופים מבוקרים: ' + ', '.join(b.get('name') or '' for b in audited) if audited else '',
            'נושא הביקורת: ' + a['audit_subject'] if a.get('audit_subject') else '',
            'תקופת הביקורת: ' + a['audit_period'] if a.get('audit_period') else '',
            'תקציר: ' + a['summary'] if a.get('summary') else '',
            'נתונים מרכזיים: ' + '; '.join(json.dumps(k, ensure_ascii=False) if not isinstance(k, str) else k
                                            for k in figures) if figures else '',
            'ממצאים:' if fs else '',
        ] + [comptroller_finding_text(f) for f in fs] if x)
        yield dict(
            doc_id='state_comptroller_audits/' + chapter_id,
            kind='state_comptroller_audits',
            title=a.get('title'),
            date=as_date(a.get('publish_date')),
            text=a.get('text_md') or '',
            llm_text=llm_text,
            what='a State Comptroller audit report chapter ("{}", published {}): its summary and findings'.format(
                a.get('title'), a.get('publish_date')),
            parts=[dict(part_id=f['finding_id'], text=comptroller_finding_text(f)) for f in fs],
            parts_hint='the ids of the findings (in square brackets, e.g. "{}-3") where the item appears; empty '
                       'if only in the summary.'.format(chapter_id),
            hints=dict(
                bodies=[dict(code=b.get('code'), name=b.get('name'), relation='audited') for b in audited],
                earlier_reports=earlier_reports(a),
                years=(_int(a.get('audit_period_start_year')), _int(a.get('audit_period_end_year'))),
                llm=True,
                same_government_default=False,
            ),
        )


# --- Government decisions -----------------------------------------------------------------------------------

DECISION_TYPE = 'החלטות ממשלה'
# Published "decisions" of other kinds are mostly procedures, licenses and land deals of private people: scanned for
# references, but not read by the LLM.
LLM_TYPES = {DECISION_TYPE, 'החלטות ועדת שירות המדינה', 'מסמכי מדיניות', 'החלטות הנהלה'}
NO_LLM_TYPES = {'החלטות ועדת פטור'}


def decision_documents(rows):
    for r in rows:
        text = T.html_to_text(r.get('text'))
        if not text and not r.get('title'):
            continue
        policy_type = r.get('policy_type')
        date = as_date(r.get('doc_published_date')) or as_date(r.get('publish_date'))
        is_decision = policy_type == DECISION_TYPE
        what = 'an Israeli government decision (החלטת ממשלה {}, "{}", {})'.format(
            r.get('procedure_number_str') or '', r.get('title'), date) if is_decision else \
            'a publication of {} ({}, "{}", {})'.format(r.get('office') or 'an Israeli government office',
                                                       policy_type, r.get('title'), date)
        yield dict(
            doc_id='gov_decisions/' + r['id'],
            kind='gov_decisions',
            title=r.get('title'),
            date=date,
            text='{}\n{}'.format(r.get('title') or '', text),
            what=what,
            parts=[],
            hints=dict(
                llm=policy_type in LLM_TYPES and policy_type not in NO_LLM_TYPES and len(text) > 200,
                # Government decisions cite the government's own earlier decisions without a date.
                same_government_default=is_decision,
                exclude_decision=r['id'],
            ),
        )


# --- AI analysis pages --------------------------------------------------------------------------------------------

# The text may hold one level of brackets: decision titles cite laws as "[נוסח חדש]".
MD_LINK = re.compile(r'\[((?:[^\[\]]|\[[^\[\]]*\])*)\]\((https?://[^)\s]+)\)')
TABLE_SEPARATOR = re.compile(r'^\|?\s*:?-{3,}')
SENTENCE_END = re.compile(r'(?<=[.!?])\s+')
EXCERPT_MAX = 300
# Where a link sits, best context first: a sentence says why the page mentions the target, a table row shows its
# figures, and a chart caption or the list of budget lines covered only says it's included.
PLACES = ['prose', 'table', 'list']
# An organization's role by the section it's linked from; anything else is `mentioned`.
ANALYSIS_ORG_RELATIONS = {'מי אחראי': 'responsible', 'התקשרויות וספקים': 'contractor'}


def _plain(md):
    """Markdown inline text -> (plain text, [(start, end, text, url)] of its links in the plain text)."""
    out, links, last = '', [], 0
    for m in MD_LINK.finditer(md):
        out += md[last:m.start()]
        links.append((len(out), len(out) + len(m.group(1)), m.group(1).strip(), m.group(2)))
        out += m.group(1)
        last = m.end()
    out += md[last:]
    # Emphasis markers aren't text; dropping them would shift the link offsets, so blank them out instead.
    return re.sub(r'[*_`]', ' ', out), links


def _squash(s):
    return re.sub(r'\s+', ' ', s or '').strip()


def _cap(s, start=0, end=0):
    """At most EXCERPT_MAX characters, around [start, end) when given."""
    s = _squash(s)
    if len(s) <= EXCERPT_MAX:
        return s
    if end:
        return T.excerpt(s, start, end, width=(EXCERPT_MAX - (end - start)) // 2)
    return s[:EXCERPT_MAX - 1].rsplit(' ', 1)[0] + '…'


def _cells(line):
    line = line.strip()
    if line.startswith('|'):
        line = line[1:]
    if line.endswith('|'):
        line = line[:-1]
    return [c.strip() for c in line.split('|')]


def _chart_title(block):
    try:
        return json.loads(block).get('title')
    except (ValueError, AttributeError):
        m = re.search(r'"title"\s*:\s*"([^"]*)"', block)
        return m.group(1) if m else None


def markdown_links(body):
    """Every site link in a markdown page: [{url, mention, section, place, context}] in page order, where context
    says where it sits - the sentence, the table row as "header: cell" pairs, or a list's label (with the chart's title
    for a chart's caption)."""
    out = []
    section, chart, table, block = None, None, None, None
    for line in (body or '').split('\n'):
        stripped = line.strip()
        if block is not None:
            if stripped.startswith('```'):
                chart = _chart_title('\n'.join(block)) or chart
                block = None
            else:
                block.append(line)
            continue
        if stripped.startswith('```'):
            block = []
            continue
        if stripped.startswith('#'):
            section = _squash(stripped.lstrip('#'))
            chart, table = None, None
            continue
        if stripped.startswith('|'):
            if table is None:
                table = [_squash(_plain(c)[0]) for c in _cells(stripped)]
                continue
            if TABLE_SEPARATOR.match(stripped):
                continue
            cells = [_plain(c) for c in _cells(stripped)]
            row = ' · '.join('{}: {}'.format(h, _squash(c)) if h else _squash(c)
                             for h, (c, _) in zip(table + [''] * len(cells), cells) if _squash(c))
            for _, links in cells:
                for _, _, text, url in links:
                    out.append(dict(url=url, mention=text, section=section, place='table', context=_cap(row)))
            continue
        table = None
        if not stripped:
            continue
        text, links = _plain(stripped)
        if not links:
            chart = None if not stripped.startswith('*') else chart
            continue
        if (stripped.startswith('*') and not stripped.startswith('* ')) or (len(links) > 2 and ' · ' in stripped):
            # A list of linked items: a chart's caption, or the budget lines the page covers.
            label = _squash(text[:links[0][0]]).rstrip(':').strip()
            context = 'תרשים "{}" - {}'.format(chart, label) if chart and stripped.startswith('*') else label
            for _, _, mention, url in links:
                out.append(dict(url=url, mention=mention, section=section, place='list', context=_cap(context)))
            chart = None
            continue
        bounds = [0] + [m.end() for m in SENTENCE_END.finditer(text)] + [len(text)]
        for start, end, mention, url in links:
            a = max(b for b in bounds if b <= start)
            z = min([b for b in bounds if b >= end] or [len(text)])
            out.append(dict(url=url, mention=mention, section=section, place='prose',
                            context=_cap(text[a:z], start - a, end - a)))
        chart = None
    return out


def analysis_documents(rows):
    """One document per AI analysis page. Its links are explicit (hints['links']), so nothing else is extracted."""
    for r in rows:
        links = []
        for link in markdown_links(r.get('body')):
            if not link['mention']:
                continue
            relation = ANALYSIS_ORG_RELATIONS.get(link['section'], 'mentioned') \
                if '/i/org/' in link['url'] else 'mentioned'
            excerpt = '{}: {}'.format(link['section'], link['context']) if link['section'] else link['context']
            links.append(dict(url=link['url'], mention=link['mention'], section=link['section'],
                              place=link['place'], relation=relation, excerpt=excerpt))
        # The best placed occurrence of a target first: the linker keeps the first excerpt of each target.
        links.sort(key=lambda link: PLACES.index(link['place']))
        yield dict(
            doc_id='analysis/' + r['slug'],
            kind='analysis',
            title=r.get('title'),
            date=as_date(r.get('generated_at')),
            text='',
            what='an AI analysis page ("{}")'.format(r.get('title')),
            parts=[],
            hints=dict(links=links, llm=False),
        )
