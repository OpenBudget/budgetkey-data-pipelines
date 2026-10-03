"""Page templates: placeholder substitution and the checks a template must pass."""
import html
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor

import requests

from datapackage_pipelines_budgetkey.pipelines.analysis import figures as F

PLACEHOLDER_RE = re.compile(r'\{\{\s*(value|chart|table)\s*:\s*([A-Za-z0-9_]+)\s*\}\}')
LINK_RE = re.compile(r'\]\((https?://[^)\s]+)\)')
MD_LINK_RE = re.compile(r'\[([^\]]+)\]\((https?://[^)\s]+)\)')

REQUIRED_SECTIONS = ['בקצרה', 'כמה כסף ולאן', 'מי אחראי', 'התכניות העיקריות']
CONTRACTS_SECTION = 'התקשרויות וספקים'
SUPPORTS_SECTION = 'תמיכות ושירותים חברתיים'
DECISIONS_SECTION = 'החלטות ממשלה'
OPTIONAL_SECTIONS = [CONTRACTS_SECTION, SUPPORTS_SECTION, DECISIONS_SECTION]

# Numbers the model must not write itself - they belong in a {{value:...}}.
_LITERAL_AMOUNT_RES = [
    re.compile(r'\d[\d,]*(?:\.\d+)?\s*%'),
    re.compile(r'\d[\d,]*(?:\.\d+)?\s*(?:מיליון|מיליארד|מיליארדי|מליון|מליארד|אלף|אלפי|₪|ש"ח|ש״ח|שקל)'),
    re.compile(r'(?<![\d.])\d{1,3}(?:,\d{3})+(?![\d,])'),
    re.compile(r'(?<![\d.,])\d{5,}(?![\d,])'),
]


def placeholders(template):
    return [(kind, name) for kind, name in PLACEHOLDER_RE.findall(template)]


def sections(template):
    return [h.strip() for h in re.findall(r'^##\s+(.+)$', template, re.MULTILINE)]


def literal_amounts(template):
    text = PLACEHOLDER_RE.sub(' ', template)
    text = re.sub(r'\]\([^)]*\)', ']', text)          # link targets
    text = re.sub(r'```.*?```', ' ', text, flags=re.DOTALL)
    found = []
    for regex in _LITERAL_AMOUNT_RES:
        found.extend(m.group(0).strip() for m in regex.finditer(text))
    return found


def check_template(template, figures, url_ok):
    """Returns a list of problems (in English, fed back to the agent verbatim)."""
    problems = []
    by_name = {f['name']: f for f in figures}

    for kind, name in placeholders(template):
        fig = by_name.get(name)
        if fig is None:
            problems.append('Placeholder {{%s:%s}} refers to a figure that was never defined.' % (kind, name))
        elif fig['kind'] != kind and not (kind == 'chart' and fig['kind'] == 'budget_trend'):
            problems.append('Placeholder {{%s:%s}} refers to a %s figure.' % (kind, name, fig['kind']))
    leftovers = re.findall(r'\{\{[^}]*\}\}', PLACEHOLDER_RE.sub('', template))
    if leftovers:
        problems.append('Malformed placeholders: %s' % ', '.join(leftovers[:5]))

    present = sections(template)
    for s in REQUIRED_SECTIONS:
        if s not in present:
            problems.append('Missing required section "## %s".' % s)
    repeated = sorted({s for s in present if present.count(s) > 1})
    if repeated:
        problems.append('Each section must appear once; repeated: %s. Start directly with the page, no preamble.'
                        % ', '.join(repeated))
    unknown = [s for s in present if s not in REQUIRED_SECTIONS + OPTIONAL_SECTIONS]
    if unknown:
        problems.append('Unexpected sections (use only the skeleton headings): %s' % ', '.join(unknown))
    if re.search(r'^#\s', template, re.MULTILINE):
        problems.append('Do not write a level-1 "#" title; the page title is added separately.')

    used = {name for _, name in placeholders(template)}
    if not any(f['kind'] == 'budget_trend' and f['name'] in used for f in figures):
        problems.append('The page needs the budget-over-the-years chart in "כמה כסף ולאן", made with define_budget_trend.')
    if not any(f['kind'] == 'table' and f['name'] in used for f in figures):
        problems.append('The page needs a table of the main programs in "התכניות העיקריות".')

    amounts = literal_amounts(template)
    if amounts:
        problems.append('These amounts are written as literal text; every amount, count or percentage must come '
                        'from a {{value:...}} placeholder: %s' % ', '.join(sorted(set(amounts))[:10]))

    for label, url in MD_LINK_RE.findall(template):
        if not url_ok(url):
            problems.append('The link [%s](%s) is invented - that URL never appeared in the data and may not exist. '
                            'Query the item\'s item_url (e.g. from budget_items_data by its code) and use that, or '
                            'drop the link.' % (label, url))
    return problems


SITE_GET_URL = os.environ.get('BUDGETKEY_SITE_GET_URL', 'https://next.obudget.org/get/')
_resolved = {}
_resolved_lock = threading.Lock()


def resolves(url):
    """Whether a site link leads to a real item page. Some item_urls in the data point at items the site
    doesn't index (e.g. budget lines with an all-zero budget), so links are checked against the site's API."""
    with _resolved_lock:
        if url in _resolved:
            return _resolved[url]
    m = re.match(r'^https://next\.obudget\.org/i/(.+)$', url)
    if not m:
        ok = False
    else:
        try:
            response = requests.get(SITE_GET_URL + m.group(1), headers={'User-Agent': 'budgetkey-analysis'}, timeout=30)
            ok = response.status_code == 200 and bool(response.json().get('value'))
        except (requests.RequestException, ValueError):
            return True     # don't drop links over a transient failure; the next refresh re-checks
    with _resolved_lock:
        _resolved[url] = ok
    return ok


def unlink_unresolvable(markdown):
    """Replaces links to pages that don't exist on the site with their plain text."""
    urls = sorted({u for _, u in MD_LINK_RE.findall(markdown)})
    with ThreadPoolExecutor(8) as pool:
        ok = dict(zip(urls, pool.map(resolves, urls)))
    broken = [u for u in urls if not ok[u]]
    if broken:
        print('UNLINKED %d unresolvable links: %s' % (len(broken), ', '.join(broken)))
    return MD_LINK_RE.sub(lambda m: m.group(0) if ok[m.group(2)] else m.group(1), markdown)


def unlink_invalid(template, url_ok):
    """Replaces links whose URL didn't come from the data with their plain text."""
    return MD_LINK_RE.sub(lambda m: m.group(0) if url_ok(m.group(2)) else m.group(1), template)


def link_targets(figures, rows_by_name, scope=None):
    """name -> url for every linked cell in the page's tables, plus the scope's budget items."""
    targets = {}
    for fig in figures:
        if fig['kind'] != 'table':
            continue
        for col in fig['columns']:
            if not col.get('link_column'):
                continue
            for r in rows_by_name.get(fig['name']) or []:
                name, url = r.get(col['column']), r.get(col['link_column'])
                if isinstance(name, str) and url:
                    targets.setdefault(name.strip(), url)
    for item in (scope or {}).get('items') or []:
        targets.setdefault(item['title'].strip(), item['item_url'])
    return targets


def autolink(template, targets, min_length=6):
    """Links the first plain mention of each known entity name in the prose.

    Only whole-name matches in paragraph text: never inside headings, tables, existing links or placeholders.
    """
    lines = template.split('\n')
    linked = set(re.findall(r'\[([^\]]+)\]\(', template))
    for name in sorted(targets, key=len, reverse=True):
        if len(name) < min_length or name in linked:
            continue
        pattern = re.compile(r'(?<![\w\[])%s(?![\w\]])' % re.escape(name))
        for i, line in enumerate(lines):
            if line.startswith(('#', '|', '{{')):
                continue
            # skip matches inside existing links or placeholders
            protected = [m.span() for m in re.finditer(r'\[[^\]]*\]\([^)]*\)|\{\{[^}]*\}\}', line)]
            m = next((m for m in pattern.finditer(line)
                      if not any(a <= m.start() < b for a, b in protected)), None)
            if m:
                lines[i] = line[:m.start()] + '[%s](%s)' % (name, targets[name]) + line[m.end():]
                linked.add(name)
                break
    return '\n'.join(lines)


def caption_markdown(caption):
    """The lines under a budget chart: which budget lines it sums (each linked), and any fixed notes."""
    out = '*הסעיפים התקציביים בתרשים:* ' + ' · '.join(
        '[%s](%s) (%s)' % (i['title'], i['item_url'], i['code']) for i in caption['items'])
    for note in caption.get('notes') or []:
        out += '\n\n*%s*' % note
    return out


def caption_html(caption):
    out = 'הסעיפים התקציביים בתרשים: ' + ' · '.join(
        '<a href="%s">%s</a> (%s)' % (html.escape(i['item_url']), html.escape(i['title']), i['code'])
        for i in caption['items'])
    for note in caption.get('notes') or []:
        out += '<br/>' + html.escape(note)
    return out


def drop_preamble(template):
    """Removes a model preamble that repeats a heading: "## X / short text with no figures / ## X" -> "## X".

    Seen as "## בקצרה / הנה עמוד הניתוח המבוקש… / ## בקצרה …".
    """
    sections = re.split(r'(?m)^(?=## )', template)
    out = []
    for section in sections:
        heading = section.split('\n', 1)[0].strip()
        if out and heading.startswith('## ') and out[-1].split('\n', 1)[0].strip() == heading:
            body = out[-1].split('\n', 1)[1] if '\n' in out[-1] else ''
            if len(body.strip()) < 300 and not PLACEHOLDER_RE.search(body):
                out[-1] = section
                continue
        out.append(section)
    return ''.join(out)


def render(template, figures, results, captions=None):
    """Substitutes every placeholder with its rendered figure. captions: {chart name: [budget items]}."""
    by_name = {f['name']: f for f in figures}
    captions = captions or {}

    def substitute(match):
        fig = by_name[match.group(2)]
        out = F.render_figure(fig, results[fig['name']])
        if captions.get(fig['name']):
            out += '\n\n' + caption_markdown(captions[fig['name']])
        if fig['kind'] != 'value':
            out = '\n\n%s\n\n' % out      # tables and charts are blocks: never glued to a neighbouring table
        return out

    body = PLACEHOLDER_RE.sub(substitute, drop_preamble(template))
    return re.sub(r'\n{3,}', '\n\n', body).strip()


def charts(template, figures, results, captions=None):
    """The page's chart descriptors, in order of appearance."""
    by_name = {f['name']: f for f in figures}
    captions = captions or {}
    out = []
    for kind, name in placeholders(template):
        if kind == 'chart' and name in by_name:
            fig = by_name[name]
            descriptor = (F.trend_descriptor(fig, results[name]) if fig['kind'] == 'budget_trend'
                          else F.chart_descriptor(fig, results[name]))
            if captions.get(name):
                descriptor['description'] = caption_html(captions[name])
            out.append(descriptor)
    return out
