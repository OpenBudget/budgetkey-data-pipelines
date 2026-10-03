"""Generates (or refreshes) one analysis page per topic.

    agent research + template  ->  check template  ->  run figures  ->  render  ->  critic
          ^                                                                          |
          +------------------- feedback (at most MAX_REVISIONS times) ---------------+
"""
import datetime
import hashlib
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from datapackage_pipelines_budgetkey.common import llm
from datapackage_pipelines_budgetkey.pipelines.analysis import figures as F
from datapackage_pipelines_budgetkey.pipelines.analysis import history as H
from datapackage_pipelines_budgetkey.pipelines.analysis import render as R
from datapackage_pipelines_budgetkey.pipelines.analysis.agent import MCP_TOOLS, PageAgent, build_tools
from datapackage_pipelines_budgetkey.pipelines.analysis.mcp_client import MCPClient
from datapackage_pipelines_budgetkey.pipelines.analysis.query import query
from datapackage_pipelines_budgetkey.pipelines.analysis.topics import latest_year, load_prompt

ROOT = Path(__file__).parent
MAX_REVISIONS = 3
PROMPT_FILES = ('page_system.md', 'page_critic.md')
# low: ~1.5 min/page but shallow research; high (the model default): deeper but ~15 min/page.
THINKING_LEVEL = os.environ.get('ANALYSIS_THINKING_LEVEL', 'medium')
# Schemas pre-loaded into the system prompt, so the agent doesn't spend turns on DatasetInfo.
SCHEMA_DATASETS = ('budget_items_data', 'contracts_data', 'support_programs_data', 'social_services_data',
                   'government_decisions_data', 'supports_transactions_data')

DATASET_NAMES = {
    'budget_items_data': 'ספר התקציב',
    'contracts_data': 'התקשרויות רכש',
    'support_programs_data': 'תכניות תמיכה',
    'supports_transactions_data': 'תשלומי תמיכות',
    'social_services_data': 'שירותים חברתיים במיקור חוץ',
    'government_decisions_data': 'החלטות ממשלה',
    'entities_data': 'גופים וארגונים',
    'income_items_data': 'הכנסות המדינה',
    'budgetary_change_requests_data': 'בקשות לשינויים תקציביים',
    'budgetary_change_transactions_data': 'שינויים תקציביים',
}

ISSUE_CATEGORIES = ['neutrality', 'interpretation', 'contradiction', 'unaddressed_anomaly', 'relevance',
                    'missing_link', 'not_answering']
# Sentence-level issues are fixed by swapping in the critic's corrected sentence; the rest need the agent.
SENTENCE_FIXABLE = {'neutrality', 'interpretation', 'missing_link', 'contradiction'}
BLOCKING = {'neutrality', 'interpretation', 'contradiction', 'unaddressed_anomaly', 'not_answering'}
MAX_CRITIC_PASSES = 3
# After the last revision, the best draft is published with its open issues recorded for review - except for
# partisan or loaded framing, which a non-partisan site must never publish.
HARD_BLOCKING = {'neutrality'}

CRITIC_SCHEMA = {
    'type': 'object',
    'properties': {
        'issues': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'category': {'type': 'string', 'enum': ISSUE_CATEGORIES},
                    'severity': {'type': 'string', 'enum': ['blocking', 'minor']},
                    'quote': {'type': 'string'},
                    'problem': {'type': 'string'},
                    'replacement': {'type': 'string'},
                },
                'required': ['category', 'severity', 'quote', 'problem', 'replacement'],
            },
        },
    },
    'required': ['issues'],
}

META_SCHEMA = {
    'type': 'object',
    'properties': {
        'title': {'type': 'string'},
        'description': {'type': 'string'},
    },
    'required': ['title', 'description'],
}


def prompt_version():
    digest = hashlib.sha256()
    for name in PROMPT_FILES:
        digest.update((ROOT / 'prompts' / name).read_bytes())
    return digest.hexdigest()[:12]


def dataset_schemas(mcp):
    """Compact markdown of the DatasetInfo results for the common datasets."""
    out = []
    for dataset in SCHEMA_DATASETS:
        info = mcp.call_tool('DatasetInfo', dict(dataset=dataset))
        out.append('## %s\n\n%s\n' % (dataset, (info.get('description') or '').strip()))
        for f in info.get('fields') or []:
            line = '- `%s`%s: %s' % (f['name'], ' (%s)' % f['type'] if f.get('type') else '',
                                     re.sub(r'\s+', ' ', f.get('description') or '').strip())
            samples = f.get('sample_values') or f.get('most_common_values')
            if samples:
                line += ' e.g. %s' % ', '.join(json.dumps(s, ensure_ascii=False) for s in samples[:5])
            out.append(line)
        out.append('')
    return '\n'.join(out)


def suggested_codes(codes):
    """The suggested budget codes with their titles, levels, years and latest allocation."""
    codes = [c for c in codes or [] if re.match(r'^[\d.]+$', c)]
    if not codes:
        return '(none)'
    rows = query('''
        SELECT code, level, MIN(year) AS first_year, MAX(year) AS last_year,
               (ARRAY_AGG(title ORDER BY year DESC))[1] AS title,
               (ARRAY_AGG(amount_allocated ORDER BY year DESC))[1] AS latest_allocated
        FROM budget_items_data WHERE code IN (%s) GROUP BY code, level ORDER BY code
    ''' % ','.join("'%s'" % c for c in codes))
    return '\n'.join('- %s (level %s) %s - years %s-%s, latest allocated %s' % (
        r['code'], r['level'], r['title'], r['first_year'], r['last_year'],
        F.format_value(r['latest_allocated'], 'currency')) for r in rows)


def task_message(topic):
    lines = [
        'Question: %s' % topic['question'],
        'Topic: %s' % topic['topic'],
        'What the page should cover: %s' % topic['description'],
        'Budget codes suggested when the topic was proposed (a starting point - verify them, and look for more):',
        suggested_codes(topic.get('budget_codes')),
        'Other leads: %s' % '; '.join(topic.get('evidence') or []),
    ]
    if topic.get('temporal'):
        lines.append('This topic is tied to a specific period or event. Focus on the years it covers.')
    lines.append('Research the data, declare the scope, define the figures, and write the page.')
    return '\n'.join(lines)


# ------------------------------------------------------------------ figures

def run_figures(figures):
    """Re-runs every figure's SQL directly (DB, or the public API locally). Returns (results, problems)."""
    results, problems, by_sql = {}, [], {}
    trends = {}
    for fig in figures:
        if fig['kind'] == 'budget_trend':
            trend = H.trend(fig['budget_codes'], fig.get('from_year'))
            if trend['rows']:
                results[fig['name']] = trends[fig['name']] = trend
            else:
                problems.append('Figure %s failed when re-run: no budget data' % fig['name'])
    for fig in figures:
        if fig['kind'] == 'budget_trend':
            continue
        if fig.get('source_trend'):
            trend = trends.get(fig['source_trend']) or H.trend(*_trend_args(figures, fig['source_trend']))
            row = next((r for r in trend['rows'] if r['year'] == fig['year']), None)
            if row is None or row[fig['column']] is None:
                problems.append('Figure %s failed when re-run: no %s for %d' % (fig['name'], fig['column'], fig['year']))
            else:
                results[fig['name']] = [{fig['column']: row[fig['column']]}]
            continue
        try:
            if fig['sql'] not in by_sql:     # values defined together share one query
                by_sql[fig['sql']] = query(fig['sql'], max_rows=5000)
            rows = by_sql[fig['sql']]
            F.check_rows(fig, rows)
            results[fig['name']] = rows
        except Exception as e:
            problems.append('Figure %s failed when re-run: %s' % (fig['name'], e))
    return results, problems


def _trend_args(figures, name):
    fig = next(f for f in figures if f['name'] == name)
    return fig['budget_codes'], fig.get('from_year')


def critic_view(template, figures, results):
    """The page as the critic sees it: the template's own text, with each value shown next to its placeholder
    ({{value:x|5.1 מיליארד ₪}}) so it can check numbers and still quote the template exactly, and each chart and
    table followed by its data."""
    by_name = {f['name']: f for f in figures}

    def substitute(match):
        kind, name = match.group(1), match.group(2)
        fig, rows = by_name[name], results[name]
        if kind == 'value':
            return '{{value:%s|%s}}' % (name, F.render_value(fig, rows))
        if fig['kind'] == 'budget_trend':
            data = '; '.join('%s: %s' % (r['year'], ', '.join('%s=%s' % (label, F.format_value(r[key], 'currency'))
                                                               for key, label in F.TREND_SERIES))
                             for r in rows['rows'])
            notes = ' '.join(F.trend_notes(rows))
            return '{{chart:%s}}\n[CHART "%s" - %s] %s' % (name, fig['title'], data, notes)
        if kind == 'chart':
            series = [s['column'] for s in fig['series']] + ([fig['group_column']] if fig.get('group_column') else [])
            data = '; '.join('%s: %s' % (r[fig['x']], ', '.join('%s=%s' % (k, F.format_value(r.get(k), 'number'))
                                                                  for k in series)) for r in rows[:40])
            return '{{chart:%s}}\n[CHART "%s" - %s]' % (name, fig['title'], data)
        return '{{table:%s}}\n%s' % (name, F.render_table(fig, rows))

    return R.PLACEHOLDER_RE.sub(substitute, template)


def critique(topic, template, figures, results, anomalies, usage=None):
    """The critic's issues, each with the exact quoted sentence and a corrected replacement."""
    prompt = load_prompt('page_critic.md', QUESTION=topic['question'], PAGE=critic_view(template, figures, results),
                         TODAY=datetime.date.today().isoformat(), LATEST_YEAR=latest_year(),
                         ANOMALIES='\n'.join('- ' + a for a in anomalies) or '(none)')
    # Pro, not Flash: Flash missed most interpretation and loaded-wording issues in the 7-page evaluation.
    _, verdict = llm.complete(prompt, schema=CRITIC_SCHEMA, model=llm.MODEL, use_cache=False, usage=usage)
    issues = verdict['issues']
    for i in issues:
        if i['category'] in BLOCKING and i['category'] != 'neutrality':
            i['severity'] = 'blocking'
    return issues


_ANNOTATION_RE = re.compile(r'\{\{\s*(value|chart|table)\s*:\s*([A-Za-z0-9_]+)\s*\|[^}]*\}\}')


def strip_annotations(text):
    return _ANNOTATION_RE.sub(r'{{\1:\2}}', text)


def apply_fixes(template, issues, url_ok):
    """Swaps in the critic's corrected sentences. Returns (template, applied, remaining).

    A fix is applied only if its quote appears exactly once in the template and the replacement keeps the
    quote's placeholders (it may drop some, but not invent new ones); anything else is left for the agent.
    """
    applied, remaining = [], []
    for issue in issues:
        quote = strip_annotations(issue['quote']).strip()
        replacement = strip_annotations(issue['replacement']).strip()
        fixable = (issue['category'] in SENTENCE_FIXABLE and quote and template.count(quote) == 1
                   and quote != replacement
                   and set(R.placeholders(replacement)) <= set(R.placeholders(quote)))
        if fixable:
            template = template.replace(quote, R.unlink_invalid(replacement, url_ok))
            applied.append(issue)
        else:
            remaining.append(issue)
    return template, applied, remaining


def issue_text(issue):
    return '%s: "%s" - %s%s' % (issue['category'], strip_annotations(issue['quote'])[:300], issue['problem'],
                                (' Suggested: "%s"' % strip_annotations(issue['replacement'])[:300])
                                if issue['replacement'] else '')


def describe(topic, body, usage=None):
    prompt = (
        'Below is an analysis page from the Israeli budget website "מפתח התקציב", answering the question "%s".\n'
        'Write in Hebrew:\n'
        '- title: a short, neutral page title (up to 60 characters), not phrased as a question.\n'
        '- description: one or two neutral sentences summarising what the page shows, for search results. '
        'No numbers.\n\n%s' % (topic['question'], re.sub(r'```plotly.*?```', '', body, flags=re.DOTALL))
    )
    _, meta = llm.complete(prompt, schema=META_SCHEMA, model=llm.MODEL_FAST, temperature=0, use_cache=False,
                           usage=usage)
    return meta


def methodology(scope, figures, model, generated_at):
    datasets = sorted({f.get('dataset', 'budget_items_data') for f in figures}, key=lambda d: list(DATASET_NAMES).index(d) if d in DATASET_NAMES else 99)
    lines = [
        '## על הנתונים',
        '',
        'העמוד נכתב באופן אוטומטי בעזרת בינה מלאכותית (%s) על סמך נתוני מפתח התקציב, ונבדק באופן אוטומטי. '
        'כל המספרים, התרשימים והטבלאות מחושבים ישירות מהנתונים ומתעדכנים כשהנתונים מתעדכנים. '
        'הסכומים בשקלים נומינליים.' % model,
        '',
        '**מאגרי המידע:** %s' % ', '.join(DATASET_NAMES.get(d, d) for d in datasets),
    ]
    if scope and scope.get('items'):
        lines += ['', '**סעיפי התקציב שנכללו בניתוח:** %s' % ' · '.join(
            '[%s](%s) (%s)' % (i['title'], i['item_url'], i['code']) for i in scope['items'])]
    lines += ['', 'נוצר ב-%s.' % generated_at.strftime('%d/%m/%Y')]
    return '\n'.join(lines)


# ------------------------------------------------------------------ coverage

def scope_contracts(scope):
    """How many procurement contracts are booked against the scope's budget lines."""
    by_length = {}
    for code in (scope or {}).get('codes') or []:
        by_length.setdefault(len(code), []).append(code)
    if not by_length:
        return 0
    condition = ' OR '.join("LEFT(budget_code, %d) IN (%s)" % (n, ','.join("'%s'" % c for c in codes))
                            for n, codes in sorted(by_length.items()))
    return query('SELECT COUNT(*) AS n FROM contracts_data WHERE %s' % condition)[0]['n']


def coverage_problems(template, agent, n_contracts):
    """Sections that must be there whenever the data has something for them."""
    problems = []
    present = R.sections(template)
    if n_contracts and R.CONTRACTS_SECTION not in present:
        problems.append('There are %d procurement contracts booked against this topic\'s budget lines. Add the '
                        '"## %s" section: the largest contracts and suppliers, with links.'
                        % (n_contracts, R.CONTRACTS_SECTION))
    searched_decisions = any(e.get('tool') in MCP_TOOLS and (e.get('args') or {}).get('dataset') == 'government_decisions_data'
                             for e in agent.transcript)
    if not searched_decisions:
        problems.append('Search government_decisions_data for government decisions on this topic, and add the '
                        '"## %s" section if relevant ones exist.' % R.DECISIONS_SECTION)
    return problems


# ------------------------------------------------------------------ time series

def page_anomalies(figures, results):
    """Anomalies in the page's budget charts over years, which the text must address (checked by the critic)."""
    found = []
    latest = latest_year()
    for fig in figures:
        rows = results.get(fig['name'])
        if not rows:
            continue
        if fig['kind'] == 'budget_trend':
            found += ['Chart "%s": %s' % (fig['title'], a) for a in F.trend_anomalies(fig, rows, latest)]
        elif F.is_budget_time_series(fig, rows):
            found += ['Chart "%s": %s' % (fig['title'], a) for a in F.anomalies(fig, rows, latest)]
    return found


def chart_captions(figures, results):
    """{chart name: {items, notes}} for every budget chart over years: the lines it sums, linked, and fixed notes."""
    captions = {}
    for fig in figures:
        rows = results.get(fig['name'])
        if not rows:
            continue
        if fig['kind'] == 'budget_trend':
            captions[fig['name']] = dict(items=H.budget_items(fig['budget_codes']), notes=F.trend_notes(rows))
        elif F.is_budget_time_series(fig, rows):
            captions[fig['name']] = dict(items=H.budget_items(F.chart_codes(fig)), notes=[])
    return captions


# ------------------------------------------------------------------ generation

def fix_template(template):
    """Deterministic repairs for slips that aren't worth a revision round."""
    template = template.strip()
    template = re.sub(r'\A```(?:markdown|md)?\s*\n(.*)\n```\Z', r'\1', template, flags=re.DOTALL)
    # The model often treats the opening paragraphs as an intro and drops the first heading.
    if not template.startswith('## '):
        template = '## %s\n%s' % (R.REQUIRED_SECTIONS[0], template)
    return template


class Setup:
    """What every page in a run shares: the MCP session, the system prompt, the tools and their prompt cache."""

    def __init__(self, use_cache=False):
        # Off by default: with an explicit cache, Gemini only discounts the cached prefix and stops implicitly
        # caching the growing conversation history - which measured as a net cost increase (19-39% cached
        # vs 56-81% with implicit caching alone).
        self.mcp = MCPClient()
        self.mcp.initialize()
        self.system = load_prompt('page_system.md', TODAY=datetime.date.today().isoformat(), LATEST_YEAR=latest_year(),
                                  MCP_INSTRUCTIONS=self.mcp.instructions or '', SCHEMAS=dataset_schemas(self.mcp))
        self.tools = build_tools(self.mcp)
        self.cache = llm.PromptCache(llm.MODEL, self.system, self.tools) if use_cache else None

    def close(self):
        if self.cache is not None:
            self.cache.delete()


REVISION_INSTRUCTIONS = (
    'Fix exactly these problems and nothing else: keep every other sentence, section and figure as it is, and '
    'reply with the complete page. Never write a number yourself, even one quoted below - every number comes from '
    'a {{value:...}} placeholder.')


def generate_page(topic, setup, log=print):
    """Runs the agent for one topic. Returns (doc, agent); doc['status'] is 'published' or 'failed'.

    Mechanical problems (placeholders, sections, links, coverage) go back to the agent. Once a draft passes them,
    the critic reviews it: wording issues are fixed in place by swapping in the critic's corrected sentence, and
    only structural issues go back to the agent, with an instruction to change nothing else.
    """
    now = datetime.datetime.now()
    agent = PageAgent(setup.mcp, setup.system, tools=setup.tools, prompt_cache=setup.cache,
                      thinking_level=THINKING_LEVEL, log=log)
    side_usage = llm.Usage()
    template = agent.run(task_message(topic))

    n_contracts, critic_passes, revisions = None, 0, 0
    best = None         # (template, figures, results, remaining issues) of the last reviewed draft
    problems, notes = [], []

    def mechanical(template):
        nonlocal n_contracts
        figures = list(agent.figures.values())
        problems = R.check_template(template, figures, agent.url_ok)
        if agent.scope is None:
            problems.append('declare_scope was never called.')
        else:
            if n_contracts is None:
                n_contracts = scope_contracts(agent.scope)
            problems += coverage_problems(template, agent, n_contracts)
        results = {}
        if not problems:
            used = {name for _, name in R.placeholders(template)}
            figures = [f for f in figures if f['name'] in used]
            results, problems = run_figures(figures)
        return figures, results, problems

    while True:
        template = R.autolink(fix_template(template), R.link_targets(agent.figures.values(), agent.rows, agent.scope))
        if revisions >= 1:
            # The model was already told; a page shouldn't fail over a link, so drop the ones still invented.
            template = R.unlink_invalid(template, agent.url_ok)
        figures, results, problems = mechanical(template)

        if not problems:
            if critic_passes >= MAX_CRITIC_PASSES:
                break
            critic_passes += 1
            issues = critique(topic, template, figures, results, page_anomalies(figures, results), usage=side_usage)
            fixed, applied, remaining = apply_fixes(template, issues, agent.url_ok)
            if applied:
                f2, r2, p2 = mechanical(fixed)
                if p2:      # a swapped-in sentence broke something: keep the draft, hand those issues to the agent
                    remaining, applied = remaining + applied, []
                else:
                    template, figures, results = fixed, f2, r2
            blocking = [i for i in remaining if i['severity'] == 'blocking']
            notes = [issue_text(i) for i in remaining if i['severity'] != 'blocking']
            log('  critic pass %d: %d issues, %d fixed in place, %d blocking left%s' % (
                critic_passes, len(issues), len(applied), len(blocking),
                ''.join('\n    - ' + issue_text(i) for i in remaining)))
            if best is None or len(blocking) <= len([i for i in best[3] if i['severity'] == 'blocking']):
                best = (template, figures, results, remaining)
            problems = [issue_text(i) for i in blocking]
            if not problems:
                break

        log('  revision %d needed: %s' % (revisions + 1, ''.join('\n    - ' + p for p in problems)))
        if revisions >= MAX_REVISIONS:
            break
        revisions += 1
        template = agent.revise(REVISION_INSTRUCTIONS + '\n' + '\n'.join('- ' + p for p in problems + notes))

    needs_review = False
    if problems and best is not None:
        hard = [i for i in best[3] if i['severity'] == 'blocking' and i['category'] in HARD_BLOCKING]
        if not hard:
            # Out of revisions: publish the best reviewed draft, and keep what's still open for a person to review.
            template, figures, results, remaining = best
            open_blocking = [i for i in remaining if i['severity'] == 'blocking']
            log('  publishing the best draft (%d open blocking issues, recorded for review)' % len(open_blocking))
            problems, notes = [], [issue_text(i) for i in remaining]
            needs_review = bool(open_blocking)

    doc = dict(
        slug=topic['slug'], question=topic['question'], topic=topic['topic'], category=topic.get('category'),
        temporal=topic.get('temporal', False), expires_at=topic.get('expires_at'),
        template=template, figures=[dict((k, v) for k, v in f.items()) for f in agent.figures.values()],
        scope=agent.scope, model=agent.model, prompt_version=prompt_version(),
        generated_at=now, problems=problems, review_notes=notes, needs_review=needs_review,
        status='failed' if problems else 'published',
        tool_calls=agent.tool_calls, revisions=revisions, critic_passes=critic_passes,
        thinking_level=THINKING_LEVEL or 'default', latest_year=latest_year(),
    )
    if not problems:
        doc.update(render_doc(doc, results))
        doc.update(describe(topic, doc['body'], usage=side_usage))
    side_usage.merge(agent.usage)
    doc['usage'] = side_usage.by_model
    doc['seconds'] = round((datetime.datetime.now() - now).total_seconds())
    return doc, agent


def render_doc(doc, results):
    """The parts of a doc derived from figure results - all that a data refresh recomputes."""
    figures = [f for f in doc['figures'] if f['name'] in results]
    captions = chart_captions(figures, results)
    body = R.render(doc['template'], figures, results, captions)
    body += '\n\n' + methodology(doc['scope'], figures, doc['model'], doc['generated_at'])
    body = R.unlink_unresolvable(body)
    return dict(
        body=body,
        charts=R.charts(doc['template'], figures, results, captions),
        data_hash=F.data_hash(results),
        rendered_at=datetime.datetime.now(),
        render_version=RENDER_VERSION,
    )


# ------------------------------------------------------------------ pipeline

PAGES_TABLE = 'analysis_pages'
# Bump when rendering changes (charts, captions, methodology), so existing pages are re-rendered even when
# their data didn't change.
RENDER_VERSION = 3
MAX_AGE_DAYS = 180              # regenerate pages older than this even if nothing else changed
MAX_GENERATIONS_PER_RUN = 25    # cap on agent runs per pipeline run (cost); the rest wait for the next run
JSON_FIELDS = ('figures', 'scope', 'charts', 'problems', 'review_notes', 'usage', 'budget_codes', 'evidence')

STATE_FIELDS = [
    ('slug', 'string'), ('status', 'string'), ('question', 'string'), ('topic', 'string'),
    ('category', 'string'), ('title', 'string'), ('description', 'string'), ('template', 'string'),
    ('figures', 'array'), ('scope', 'object'), ('body', 'string'), ('charts', 'array'),
    ('model', 'string'), ('prompt_version', 'string'), ('thinking_level', 'string'),
    ('generated_at', 'datetime'), ('rendered_at', 'datetime'), ('data_hash', 'string'), ('latest_year', 'integer'),
    ('problems', 'array'), ('review_notes', 'array'), ('needs_review', 'boolean'),
    ('temporal', 'boolean'), ('expires_at', 'date'), ('usage', 'object'), ('seconds', 'integer'),
    ('tool_calls', 'integer'), ('revisions', 'integer'), ('critic_passes', 'integer'), ('render_version', 'integer'),
]


def _parse(row):
    """Rows read back from the state table: JSON columns may arrive as text, timestamps as strings."""
    for k in JSON_FIELDS:
        if isinstance(row.get(k), str):
            try:
                row[k] = json.loads(row[k])
            except ValueError:
                pass
    for k in ('generated_at', 'rendered_at'):
        if isinstance(row.get(k), str):
            row[k] = datetime.datetime.fromisoformat(row[k].replace('Z', ''))
    if isinstance(row.get('expires_at'), str):
        row['expires_at'] = datetime.date.fromisoformat(row['expires_at'][:10])
    return row


def load_table(name, where=''):
    try:
        return [_parse(r) for r in query('SELECT * FROM %s %s' % (name, where))]
    except Exception as e:
        print('No %s table yet (%s)' % (name, e))
        return []


def plan(topics, previous, current_latest_year, now=None):
    """Decides what to do with each active topic: generate (agent), refresh (re-run figures) or keep.

    Returns (to_generate, to_refresh) as lists of (topic, previous doc or None). Generation is ordered new
    topics first, then by age, and capped per run; topics over the cap fall back to a refresh when they can.
    """
    now = now or datetime.datetime.now()
    version = prompt_version()
    generate, refresh = [], []
    for topic in topics:
        doc = previous.get(topic['slug'])
        if doc is None:
            generate.append((0, now, topic, None))
            continue
        reasons = []
        if doc.get('status') != 'published':
            reasons.append('last attempt failed')
        if doc.get('prompt_version') != version:
            reasons.append('prompts changed')
        if doc.get('generated_at') and (now - doc['generated_at']).days > MAX_AGE_DAYS:
            reasons.append('older than %d days' % MAX_AGE_DAYS)
        if doc.get('latest_year') and current_latest_year and doc['latest_year'] < current_latest_year:
            reasons.append('new budget year')
        if reasons:
            generate.append((1, doc.get('generated_at') or now, topic, doc))
            print('REGENERATE %s: %s' % (topic['slug'], ', '.join(reasons)))
        else:
            refresh.append((topic, doc))
    generate.sort(key=lambda x: (x[0], x[1]))
    chosen = [(t, d) for _, _, t, d in generate[:MAX_GENERATIONS_PER_RUN]]
    for _, _, topic, doc in generate[MAX_GENERATIONS_PER_RUN:]:
        print('DEFERRED %s (generation cap reached)' % topic['slug'])
        if doc is not None and doc.get('status') == 'published':
            refresh.append((topic, doc))
    return chosen, refresh


def refresh_doc(doc):
    """Re-runs a published page's figures. Returns (doc, action): 'skipped', 'rendered' or 'regenerate'."""
    used = {name for _, name in R.placeholders(doc['template'])}
    figures = [f for f in doc['figures'] if f['name'] in used]
    results, problems = run_figures(figures)
    if problems:
        print('REFRESH %s failed: %s' % (doc['slug'], '; '.join(problems)))
        return doc, 'regenerate'
    if F.data_hash(results) == doc.get('data_hash') and doc.get('render_version') == RENDER_VERSION:
        return doc, 'skipped'
    doc = dict(doc, **render_doc(doc, results))
    return doc, 'rendered'


def search_text(body):
    """The page as plain text for search: no chart JSON, no link targets, no markdown markup."""
    text = re.sub(r'```plotly.*?```', ' ', body or '', flags=re.DOTALL)
    text = re.sub(r'\[([^\]]*)\]\([^)]*\)', r'\1', text)
    text = re.sub(r'[#*|>`_-]+', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()


def flow(parameters, *_):
    import dataflows as DF
    global MAX_GENERATIONS_PER_RUN
    MAX_GENERATIONS_PER_RUN = parameters.get('max-generations', MAX_GENERATIONS_PER_RUN)
    parallel = parameters.get('parallel', 3)

    topics = load_table('analysis_topics', "WHERE status = 'active'")
    previous = {d['slug']: d for d in load_table(PAGES_TABLE)}
    current_latest_year = latest_year()
    to_generate, to_refresh = plan(topics, previous, current_latest_year)
    print('TOPICS %d active: %d to generate, %d to refresh' % (len(topics), len(to_generate), len(to_refresh)))

    docs = {}
    for topic, doc in to_refresh:
        try:
            doc, action = refresh_doc(doc)
        except Exception as e:
            print('REFRESH %s crashed: %r' % (topic['slug'], e))
            action = 'kept'
        print('REFRESH %s: %s' % (topic['slug'], action))
        if action == 'regenerate' and len(to_generate) < MAX_GENERATIONS_PER_RUN:
            to_generate.append((topic, doc))
        docs[topic['slug']] = doc

    if to_generate:
        setup = Setup()

        def generate(item):
            topic, old = item
            log = lambda msg: print('[%s] %s' % (topic['slug'], msg), flush=True)
            try:
                doc, _ = generate_page(topic, setup, log=log)
            except Exception as e:
                log('CRASHED: %r' % e)
                return topic['slug'], old
            log('%s in %ds' % (doc['status'], doc['seconds']))
            # A failed regeneration keeps serving the last published version of the page.
            if doc['status'] != 'published' and old is not None and old.get('status') == 'published':
                return topic['slug'], dict(old, problems=doc['problems'])
            return topic['slug'], doc

        try:
            with ThreadPoolExecutor(parallel) as pool:
                for slug, doc in pool.map(generate, to_generate):
                    if doc is not None:
                        docs[slug] = doc
        finally:
            setup.close()

    rows = [{name: d.get(name) for name, _ in STATE_FIELDS} for d in docs.values()]
    published = [r for r in rows if r['status'] == 'published']
    print('PAGES %d published, %d failed' % (len(published), len(rows) - len(published)))

    # 1. The state table: every page, published or not, with what the next run needs to decide.
    DF.Flow(
        rows or [dict((name, None) for name, _ in STATE_FIELDS)],
        DF.filter_rows(lambda r: r['slug'] is not None),
        DF.update_resource(-1, name=PAGES_TABLE, path=PAGES_TABLE + '.csv'),
        *[DF.set_type(name, type=type_) for name, type_ in STATE_FIELDS],
        DF.set_primary_key(['slug']),
        DF.dump_to_sql({PAGES_TABLE: {'resource-name': PAGES_TABLE}}, engine='env://DPP_DB_ENGINE'),
    ).process()

    # 2. The published pages, for the `analysis` indexer (see CONTRACT.md).
    index_rows = [index_row(r) for r in published] or [dict((name, None) for name, _ in INDEX_FIELDS)]
    return DF.Flow(
        index_rows,
        DF.filter_rows(lambda r: r['slug'] is not None),
        DF.update_resource(-1, name='analysis', path='analysis.csv'),
        *[DF.set_type(name, type=type_, **options) for name, type_, options in INDEX_SCHEMA],
        DF.set_primary_key(['slug']),
        DF.dump_to_path('/var/datapackages/analysis/pages'),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )


INDEX_SCHEMA = [
    ('slug', 'string', {'es:keyword': True}),
    ('title', 'string', {'es:title': True}),
    ('question', 'string', {'es:title': True}),
    ('description', 'string', {'es:hebrew': True}),
    ('category', 'string', {'es:keyword': True}),
    ('text', 'string', {'es:hebrew': True}),
    ('body', 'string', {'es:index': False}),
    ('charts', 'array', {'es:itemType': 'object', 'es:index': False}),
    ('needs_review', 'boolean', {}),
    ('temporal', 'boolean', {}),
    ('expires_at', 'date', {}),
    ('generated_at', 'datetime', {}),
    ('rendered_at', 'datetime', {}),
    ('score', 'number', {'es:score-column': True}),
]
INDEX_FIELDS = [(name, type_) for name, type_, _ in INDEX_SCHEMA]
PAGE_SCORE = 10


def index_row(r):
    return dict(
        slug=r['slug'], title=r['title'] or r['question'], question=r['question'], description=r['description'],
        category=r['category'], text=search_text(r['body']), body=r['body'], charts=r['charts'],
        needs_review=bool(r['needs_review']), temporal=bool(r['temporal']), expires_at=r['expires_at'],
        generated_at=r['generated_at'], rendered_at=r['rendered_at'], score=PAGE_SCORE,
    )


# ------------------------------------------------------------------ CLI

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Generate analysis pages for topics from a discovery dry-run')
    parser.add_argument('--topics', required=True, help='JSON written by topics.py (uses its new_topics)')
    parser.add_argument('--slugs', required=True, help='comma-separated slugs')
    parser.add_argument('--out', default='pages-dry-run')
    parser.add_argument('--parallel', type=int, default=3)
    parser.add_argument('--prompt-cache', action='store_true', help='use an explicit prompt cache for the static prefix')
    args = parser.parse_args()
    setup = Setup(use_cache=args.prompt_cache)

    topics = {t['slug']: t for t in json.load(open(args.topics))['new_topics']}
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    def one(slug):
        topic = topics[slug]
        log = lambda msg: print('[%s] %s' % (slug, msg), flush=True)
        log('START: %s' % topic['question'])
        try:
            doc, agent = generate_page(topic, setup, log=log)
        except Exception as e:
            log('CRASHED: %r' % e)
            raise
        (out / ('%s.transcript.json' % slug)).write_text(
            json.dumps(agent.transcript, ensure_ascii=False, indent=1, default=str))
        (out / ('%s.json' % slug)).write_text(json.dumps(doc, ensure_ascii=False, indent=1, default=str))
        if doc['status'] == 'published':
            (out / ('%s.md' % slug)).write_text('# %s\n\n%s\n\n%s' % (doc['title'], doc['question'], doc['body']))
        else:
            (out / ('%s.failed.md' % slug)).write_text(doc['template'])
        log('DONE: %s, %d tool calls' % (doc['status'], doc['tool_calls']))
        return slug, doc['status']

    try:
        with ThreadPoolExecutor(args.parallel) as pool:
            for slug, status in pool.map(one, args.slugs.split(',')):
                print(slug, status)
    finally:
        setup.close()


if __name__ == '__main__':
    main()
