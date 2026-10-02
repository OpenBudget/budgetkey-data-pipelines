"""The page agent: a Gemini tool-calling loop over the BudgetKey MCP tools plus local figure tools.

The MCP tools (DatasetInfo, DatasetFullTextSearch, DatasetDBQuery) are declared to the model
exactly as the MCP server describes them. The local tools (declare_scope, define_values,
define_chart, define_table) run their SQL through the same MCP DatasetDBQuery, hand the result
back to the model, and register the definition. That gives us the exact SQL behind every
figure on the page without scraping it out of the conversation.
"""
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor

from google.genai import types

from datapackage_pipelines_budgetkey.common import llm
from datapackage_pipelines_budgetkey.pipelines.analysis import figures as F
from datapackage_pipelines_budgetkey.pipelines.analysis import history as H

MCP_TOOLS = ('DatasetInfo', 'DatasetFullTextSearch', 'DatasetDBQuery')
ORG_URL_RE = re.compile(r'^https://next\.obudget\.org/i/org/[a-z_-]+/([^/?#]+)$')
MAX_TOOL_CALLS = 60
MAX_PARALLEL_CALLS = 4      # the MCP server asks for at most 4 concurrent calls
MAX_ROWS_TO_MODEL = 100
MAX_RESULT_CHARS = 30000
QUERY_PAGE_SIZE = 1000

_DATASET = {'type': 'string', 'description': 'Dataset the SQL runs against, e.g. budget_items_data.'}
_SQL = {'type': 'string', 'description': 'PostgreSQL query. Stored and re-run when data updates, so it must be self-contained and stay correct over time.'}
_NAME = {'type': 'string', 'description': 'Unique figure name (letters, digits, underscore). Referenced from the page as a placeholder.'}
_FORMAT = {'type': 'string', 'enum': list(F.FORMATS),
           'description': 'currency = shekels; number = a count; percent = a number already in percent (12.5 means 12.5%); year; date (shown as dd/mm/yyyy); text.'}

LOCAL_TOOLS = [
    dict(
        name='declare_scope',
        description='Declare the budget lines that make up this topic. Call once, after research and before defining '
                    'budget figures; call again only to correct it. Returns the titles of the codes, and reports '
                    'unknown codes and codes whose parent is also included.',
        parameters_json_schema={
            'type': 'object',
            'properties': {
                'budget_codes': {'type': 'array', 'items': {'type': 'string'},
                                 'description': 'Budget codes (levels 2-4, e.g. 24.07.14) that are on-topic.'},
                'notes': {'type': 'string', 'description': 'One or two sentences on what was included and excluded, and why.'},
            },
            'required': ['budget_codes', 'notes'],
        },
    ),
    dict(
        name='define_values',
        description='Define numbers shown in the text as {{value:name}}. One SQL query returning exactly one row '
                    'can define several values at once, one per column - compute related numbers together '
                    '(e.g. this year\'s total, last year\'s total and the share of the largest program).',
        parameters_json_schema={
            'type': 'object',
            'properties': {
                'dataset': _DATASET, 'sql': _SQL,
                'values': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'name': _NAME,
                            'column': {'type': 'string', 'description': 'Result column holding this value.'},
                            'format': _FORMAT,
                        },
                        'required': ['name', 'column', 'format'],
                    },
                },
            },
            'required': ['dataset', 'sql', 'values'],
        },
    ),
    dict(
        name='define_budget_trend',
        description='Define the topic\'s budget-over-the-years chart, shown as {{chart:name}}. Give the budget codes '
                    'it sums (usually your scope); the chart is built from each line\'s history, so lines whose code '
                    'changed over the years stay continuous. It always shows the original budget, the revised budget '
                    'and execution. Years in which some main lines have no history are shaded and labelled as '
                    'estimates automatically, and the lines are listed and linked under the chart. The result lists '
                    'anomalies (sharp changes, gaps) that your text must address.',
        parameters_json_schema={
            'type': 'object',
            'properties': {
                'name': _NAME,
                'budget_codes': {'type': 'array', 'items': {'type': 'string'},
                                 'description': 'Budget codes (levels 2-4) to sum. Don\'t mix a parent with its children.'},
                'title': {'type': 'string', 'description': 'Hebrew chart title.'},
                'from_year': {'type': 'integer', 'description': 'Optional first year (default: the last 15 years).'},
            },
            'required': ['name', 'budget_codes', 'title'],
        },
    ),
    dict(
        name='define_chart',
        description='Define a chart shown as {{chart:name}}. For a line or bar chart, x is the category column '
                    '(e.g. year) and each series is a numeric column - or give group_column plus one series to draw '
                    'one line/bar per group value (long-format data). For a pie, x is the label column and the single '
                    'series holds the values.',
        parameters_json_schema={
            'type': 'object',
            'properties': {
                'name': _NAME, 'dataset': _DATASET, 'sql': _SQL,
                'chart_type': {'type': 'string', 'enum': list(F.CHART_TYPES)},
                'x': {'type': 'string', 'description': 'Column for the x axis (or pie labels).'},
                'series': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'column': {'type': 'string'},
                            'label': {'type': 'string', 'description': 'Hebrew legend label.'},
                        },
                        'required': ['column', 'label'],
                    },
                },
                'group_column': {'type': 'string', 'description': 'Optional: column whose values become separate traces.'},
                'title': {'type': 'string', 'description': 'Hebrew chart title.'},
                'x_title': {'type': 'string'},
                'y_title': {'type': 'string'},
            },
            'required': ['name', 'dataset', 'sql', 'chart_type', 'x', 'series', 'title'],
        },
    ),
    dict(
        name='define_table',
        description='Define a table shown as {{table:name}} (at most %d rows are shown). Include item_url in the SQL '
                    'and use it as link_column of the name column.' % F.MAX_TABLE_ROWS,
        parameters_json_schema={
            'type': 'object',
            'properties': {
                'name': _NAME, 'dataset': _DATASET, 'sql': _SQL,
                'columns': {
                    'type': 'array',
                    'items': {
                        'type': 'object',
                        'properties': {
                            'column': {'type': 'string'},
                            'label': {'type': 'string', 'description': 'Hebrew column header.'},
                            'format': _FORMAT,
                            'link_column': {'type': 'string', 'description': 'Optional column with a URL to link this cell to.'},
                        },
                        'required': ['column', 'label'],
                    },
                },
                'title': {'type': 'string'},
            },
            'required': ['name', 'dataset', 'sql', 'columns'],
        },
    ),
]


LEVEL_FILTER_RE = re.compile(r'\blevel\s*(=|IN\b)', re.IGNORECASE)


def _level_warning_is_false_positive(warning, sql):
    """The API flags any SQL containing code-like numbers of different lengths as mixing budget levels - even
    `LIMIT 10` next to a level-4 code. With an explicit level filter the query can't double-count, which is
    exactly what the warning protects against."""
    return warning.startswith('Matching codes with different levels') and bool(LEVEL_FILTER_RE.search(sql))


def _collect(obj, urls, values):
    """Gathers every URL, and every other scalar value, that appeared in a tool result."""
    if isinstance(obj, dict):
        for v in obj.values():
            _collect(v, urls, values)
    elif isinstance(obj, list):
        for v in obj:
            _collect(v, urls, values)
    elif isinstance(obj, str) and obj.startswith('http'):
        urls.add(obj)
    elif isinstance(obj, (str, int)) and not isinstance(obj, bool):
        values.add(str(obj))


def _for_model(result):
    """Trims a tool result so one big query can't flood the context."""
    if isinstance(result.get('rows'), list) and len(result['rows']) > MAX_ROWS_TO_MODEL:
        result = dict(result, rows=result['rows'][:MAX_ROWS_TO_MODEL],
                      note='Only the first %d rows are shown to you.' % MAX_ROWS_TO_MODEL)
    text = json.dumps(result, ensure_ascii=False, default=str)
    if len(text) > MAX_RESULT_CHARS:
        return dict(truncated_json=text[:MAX_RESULT_CHARS],
                    note='Result truncated; narrow the query (fewer columns or rows).')
    return result


def build_tools(mcp):
    """The agent's tool declarations: the MCP's own tools, as the server describes them, plus the local ones."""
    declarations = [
        types.FunctionDeclaration(name=t['name'], description=t['description'],
                                  parameters_json_schema=t['inputSchema'])
        for t in mcp.list_tools() if t['name'] in MCP_TOOLS
    ] + [types.FunctionDeclaration(**t) for t in LOCAL_TOOLS]
    return [types.Tool(function_declarations=declarations)]


class PageAgent:

    def __init__(self, mcp, system_prompt, tools=None, prompt_cache=None, model=None, thinking_level=None, log=print):
        """With a prompt_cache (llm.PromptCache over the same system prompt and tools), the
        static prefix is read from the explicit cache on every turn."""
        self.mcp = mcp
        self.usage = llm.Usage()
        self.prompt_cache = prompt_cache
        self.model = model or llm.MODEL
        self.log = log
        self.figures = {}       # name -> definition
        self.rows = {}          # name -> rows as seen at definition time
        self.scope = None
        self.seen_urls = set()
        self.seen_values = set()
        self.transcript = []
        self.tool_calls = 0
        self.contents = []
        self.config = llm.apply_thinking(types.GenerateContentConfig(
            automatic_function_calling=types.AutomaticFunctionCallingConfig(disable=True),
        ), thinking_level)
        if prompt_cache is None:
            self.config.system_instruction = system_prompt
            self.config.tools = tools or build_tools(mcp)

    # -------------------------------------------------------------- tools

    def _query(self, dataset, sql):
        result = self.mcp.call_tool('DatasetDBQuery', dict(dataset=dataset, query=sql, page_size=QUERY_PAGE_SIZE))
        if result.get('error'):
            raise F.FigureError('query failed: %s' % result['error'])
        warnings = [w for w in result.get('warnings') or [] if not _level_warning_is_false_positive(w, sql)]
        if warnings:
            raise F.FigureError('query returned warnings, fix it: %s' % warnings)
        rows = result.get('rows') or []
        if (result.get('total_rows') or 0) > len(rows):
            raise F.FigureError('query returned %s rows but only %d fit; aggregate or limit it'
                                % (result['total_rows'], len(rows)))
        return rows

    def _declare_scope(self, args):
        codes = sorted({c.strip() for c in args['budget_codes'] if c.strip()})
        if not codes:
            return dict(error='budget_codes is empty')
        # Latest year with any money in it: the site doesn't index all-zero budget years, so their item_url 404s.
        sql = ("SELECT DISTINCT ON (code) code, title, level, year, item_url FROM budget_items_data "
               "WHERE code IN (%s) ORDER BY code, (COALESCE(amount_allocated, 0) <> 0 OR COALESCE(amount_revised, 0) <> 0 "
               "OR COALESCE(amount_used, 0) <> 0) DESC, year DESC" % ','.join("'%s'" % c.replace("'", '') for c in codes))
        rows = self._query('budget_items_data', sql)
        found = {r['code']: r for r in rows}
        unknown = [c for c in codes if c not in found]
        nested = [c for c in codes if any(c != p and c.startswith(p + '.') for p in codes)]
        self.scope = dict(codes=[c for c in codes if c in found], notes=args.get('notes', ''),
                          items=[found[c] for c in codes if c in found])
        return dict(ok=True, items=[dict(code=r['code'], title=r['title'], last_year=r['year']) for r in rows],
                    unknown_codes=unknown, codes_whose_parent_is_also_in_scope=nested)

    def _define_values(self, args):
        figs = [dict(kind='value', dataset=args.get('dataset'), sql=args.get('sql'), **v) for v in args.get('values') or []]
        if not figs:
            raise F.FigureError('values is empty')
        for fig in figs:
            F.check_definition(fig)
        rows = self._query(figs[0]['dataset'], figs[0]['sql'])
        for fig in figs:
            F.check_rows(fig, rows)
        for fig in figs:
            self.figures[fig['name']] = fig
            self.rows[fig['name']] = rows
        return dict(ok=True, values={'{{value:%s}}' % f['name']: F.render_value(f, rows) for f in figs})

    def _define_budget_trend(self, args):
        fig = dict(args, kind='budget_trend')
        F.check_definition(fig)
        trend = H.trend([c.strip() for c in fig['budget_codes']], fig.get('from_year'))
        if not trend['rows']:
            raise F.FigureError('no budget data for these codes: %s' % ', '.join(trend['unknown'] or fig['budget_codes']))
        fig['budget_codes'] = trend['codes']
        self.figures[fig['name']] = fig
        self.rows[fig['name']] = trend
        # The chart's own numbers, as values: text that quotes the trend then matches the chart exactly.
        for r in trend['rows']:
            for key, _ in F.TREND_SERIES:
                if r[key] is not None:
                    vname = '%s_%s_%d' % (fig['name'], key, r['year'])
                    self.figures[vname] = dict(kind='value', name=vname, source_trend=fig['name'], year=r['year'],
                                               column=key, format='currency')
                    self.rows[vname] = [{key: r[key]}]
        latest = trend['rows'][-1]['year']
        result = dict(ok=True, placeholder='{{chart:%s}}' % fig['name'],
                      values=('When the text quotes this chart\'s numbers, use its own values so they match exactly: '
                              '{{value:%s_<allocated|revised|used>_<year>}}, e.g. {{value:%s_used_%d}}.'
                              % (fig['name'], fig['name'], trend['rows'][0]['year'])),
                      years={r['year']: {k: F.format_value(r[k], 'currency') for k in ('allocated', 'revised', 'used')}
                             for r in trend['rows']})
        if trend['estimate_before']:
            result['estimated_years'] = ('Years before %d are shaded as estimates, and a note under the chart says the '
                                         'budget structure changed. Don\'t research earlier codes.' % trend['estimate_before'])
        if trend['excluded']:
            result['excluded_codes'] = 'Left out, as transfers/earmarked income/reserves: %s' % ', '.join(trend['excluded'])
        if trend['unknown']:
            result['unknown_codes'] = trend['unknown']
        anomalies = F.trend_anomalies(fig, trend, latest)
        if anomalies:
            result['anomalies_to_address_in_text'] = anomalies
        return result

    def _chart_warnings(self, fig, rows):
        """Coverage and anomaly warnings for a budget chart over years, returned right away so the agent can plan for them."""
        if not F.is_budget_time_series(fig, rows):
            return []
        warnings = []
        codes = F.chart_codes(fig)
        if not codes:
            return ['This chart sums budget lines without naming them; select them by explicit codes.']
        years = F.chart_years(fig, rows)
        missing = F.missing_code_years(codes, years, H.code_amounts(codes))
        if missing:
            span = F.safe_range(years, missing)
            warnings.append('Main lines missing for part of the range (%s): limit the chart to %s, or use '
                            'define_budget_trend, which follows renumbered lines.'
                            % ('; '.join('%s missing in %s' % (c, ', '.join(map(str, ys))) for c, ys in sorted(missing.items())),
                               '%d-%d' % span if span else 'the years in which they all exist'))
        warnings += ['Anomaly to address in the text: %s' % a for a in F.anomalies(fig, rows, years[-1])]
        return warnings

    def _define(self, kind, args):
        fig = dict(args, kind=kind)
        F.check_definition(fig)
        rows = self._query(fig['dataset'], fig['sql'])
        F.check_rows(fig, rows)
        self.figures[fig['name']] = fig
        self.rows[fig['name']] = rows
        preview = F.render_value(fig, rows) if kind == 'value' else rows
        result = dict(ok=True, placeholder='{{%s:%s}}' % (kind, fig['name']), num_rows=len(rows), result=preview)
        if kind == 'chart':
            warnings = self._chart_warnings(fig, rows)
            if warnings:
                result['warnings'] = warnings
        return result

    def url_ok(self, url):
        """A link is valid if it came from the data, or is an organization page built from a seen entity id."""
        if url in self.seen_urls:
            return True
        m = ORG_URL_RE.match(url)
        return bool(m) and m.group(1) in self.seen_values

    def call(self, name, args):
        try:
            if name in MCP_TOOLS:
                result = self.mcp.call_tool(name, args)
            elif name == 'declare_scope':
                result = self._declare_scope(args)
            elif name == 'define_values':
                result = self._define_values(args)
            elif name == 'define_budget_trend':
                result = self._define_budget_trend(args)
            elif name in ('define_chart', 'define_table'):
                result = self._define(name.split('_')[1], args)
            else:
                result = dict(error='unknown tool %s' % name)
        except F.FigureError as e:
            result = dict(error=str(e))
        except Exception as e:
            result = dict(error='%s: %s' % (type(e).__name__, e))
        _collect(result, self.seen_urls, self.seen_values)
        self.transcript.append(dict(tool=name, args=args, result=_for_model(result)))
        return _for_model(result)

    # -------------------------------------------------------------- loop

    def _generate(self, final=False):
        config = self.config
        if self.prompt_cache is not None:
            # A cached prefix can't be combined with tool_config; over-budget calls are refused in _loop instead.
            try:
                response = llm.generate(self.contents, config.model_copy(update=dict(
                    cached_content=self.prompt_cache.name())), model=self.model)
            except llm.genai.errors.APIError as e:
                if 'cache' not in str(e).lower():
                    raise
                self.log('  prompt cache unavailable (%s), recreating' % e)
                response = llm.generate(self.contents, config.model_copy(update=dict(
                    cached_content=self.prompt_cache.name(refresh=True))), model=self.model)
            self.usage.add(self.model, response)
            return response
        if final:
            config = config.model_copy(update=dict(tool_config=types.ToolConfig(
                function_calling_config=types.FunctionCallingConfig(mode='NONE'))))
        response = llm.generate(self.contents, config, model=self.model)
        self.usage.add(self.model, response)
        return response

    def _loop(self):
        nudges = 0
        while True:
            final = self.tool_calls >= MAX_TOOL_CALLS
            response = self._generate(final=final)
            candidate = response.candidates[0] if response.candidates else None
            content = candidate.content if candidate else None
            if content is None or not content.parts:
                nudges += 1
                if nudges > 3:
                    raise RuntimeError('model returned no content (%s)' % (candidate and candidate.finish_reason))
                self.contents.append(types.Content(role='user', parts=[types.Part(
                    text='Your last reply was empty or malformed. Continue: call a tool, or write the page.')]))
                continue
            self.contents.append(content)
            calls = [p.function_call for p in content.parts if p.function_call]
            if not calls:
                text = ''.join(p.text or '' for p in content.parts if not p.thought)
                self.transcript.append(dict(final=text))
                return text
            self.tool_calls += len(calls)
            self.log('  tools #%d: %s' % (self.tool_calls, ', '.join(c.name for c in calls)))
            if self.tool_calls > MAX_TOOL_CALLS:
                outputs = [dict(error='Tool budget exhausted. Write the page now with the figures already defined.')
                           for _ in calls]
            else:
                with ThreadPoolExecutor(MAX_PARALLEL_CALLS) as pool:
                    outputs = list(pool.map(lambda c: self.call(c.name, dict(c.args or {})), calls))
            self.contents.append(types.Content(role='user', parts=[
                types.Part.from_function_response(name=c.name, response=o) for c, o in zip(calls, outputs)
            ]))

    def run(self, task):
        self.contents.append(types.Content(role='user', parts=[types.Part(text=task)]))
        start = time.time()
        text = self._loop()
        self.log('  agent finished after %d tool calls, %.0fs' % (self.tool_calls, time.time() - start))
        return text

    def revise(self, feedback):
        """Continues the same conversation with a list of problems to fix."""
        self.contents.append(types.Content(role='user', parts=[types.Part(text=feedback)]))
        # Give revisions some room even when the research used up the budget.
        self.tool_calls = min(self.tool_calls, MAX_TOOL_CALLS - 10)
        return self._loop()
