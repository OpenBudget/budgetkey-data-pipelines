"""Figures: every number, chart and table on an analysis page, defined as SQL.

A figure is a dict the agent creates through its define_* tools:

    value:  {kind, name, dataset, sql, column, format}
    chart:  {kind, name, dataset, sql, chart_type, x, series:[{column,label}], group_column?, title, x_title?, y_title?}
    table:  {kind, name, dataset, sql, columns:[{column,label,format?,link_column?}], title?}

The page template references them as {{value:name}}, {{chart:name}}, {{table:name}}.
Re-running a figure's SQL and re-rendering is all a data refresh needs: no model involved.
Everything here is pure, so it is unit-testable; query execution lives in the caller.
"""
import hashlib
import json
import re

FORMATS = ('currency', 'number', 'percent', 'year', 'date', 'text')
CHART_TYPES = ('line', 'bar', 'stacked_bar', 'pie')
MAX_TABLE_ROWS = 25
MAX_PIE_SLICES = 12


class FigureError(ValueError):
    pass


# ------------------------------------------------------------------ formatting

def _num(value):
    if value is None or value == '':
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_value(value, fmt):
    if value is None:
        return '—'
    if fmt == 'text':
        return str(value)
    if fmt == 'date':
        m = re.match(r'^(\d{4})-(\d{2})-(\d{2})', str(value))
        return '%s/%s/%s' % (m.group(3), m.group(2), m.group(1)) if m else str(value)
    n = _num(value)
    if n is None:
        return str(value)
    if fmt == 'year':
        return str(int(n))
    if fmt == 'percent':
        return '{:.1f}%'.format(n).replace('.0%', '%')
    if fmt == 'currency':
        a = abs(n)
        if a >= 1e9:
            return '{:,.1f} מיליארד ₪'.format(n / 1e9).replace('.0 ', ' ')
        if a >= 1e6:
            return '{:,.1f} מיליון ₪'.format(n / 1e6).replace('.0 ', ' ')
        return '{:,.0f} ₪'.format(n)
    if n == int(n):
        return '{:,}'.format(int(n))
    return '{:,.1f}'.format(n)


def _cell(text):
    return str(text).replace('|', '\\|').replace('\n', ' ')


# ------------------------------------------------------------------ validation

def _require(cond, message):
    if not cond:
        raise FigureError(message)


def check_definition(fig):
    """Raises FigureError if the definition itself is malformed."""
    _require(fig.get('name') and fig['name'].replace('_', '').isalnum(), 'name must be alphanumeric/underscore')
    kind = fig.get('kind')
    _require(kind == 'budget_trend' or fig.get('source_trend') or fig.get('sql'), 'sql is required')
    if kind == 'value':
        _require(fig.get('column'), 'column is required')
        _require(fig.get('format') in FORMATS, 'format must be one of %s' % (FORMATS,))
    elif kind == 'chart':
        _require(fig.get('chart_type') in CHART_TYPES, 'chart_type must be one of %s' % (CHART_TYPES,))
        _require(fig.get('x'), 'x is required')
        _require(fig.get('series'), 'at least one series is required')
        _require(fig.get('title'), 'title is required')
        _require(not SCALE_IN_TITLE_RE.search(fig['title']),
                 'don\'t put a scale or unit in the title ("במיליארדי ש״ח"); values are shown in shekels')
        if fig.get('group_column'):
            _require(len(fig['series']) == 1, 'with group_column, give exactly one series (the value column)')
    elif kind == 'table':
        _require(fig.get('columns'), 'columns are required')
    elif kind == 'budget_trend':
        _require(fig.get('budget_codes'), 'budget_codes are required')
        _require(fig.get('title'), 'title is required')
    else:
        raise FigureError('unknown figure kind %r' % kind)


def _is_year(value):
    try:
        return 1900 <= int(str(value)) <= 2100 and len(str(value)) == 4
    except ValueError:
        return False


def is_time_series(fig, rows):
    return fig.get('chart_type') != 'pie' and bool(rows) and all(_is_year(r.get(fig['x'])) for r in rows)


def url_columns(rows):
    return [c for c in (rows[0].keys() if rows else []) if c == 'item_url' or c.endswith('_url')]


def check_rows(fig, rows):
    """Raises FigureError if the query result can't feed the figure."""
    _require(rows, 'query returned no rows')
    columns = set(rows[0].keys())
    if fig['kind'] == 'value':
        _require(len(rows) == 1, 'a value query must return exactly one row (got %d)' % len(rows))
        _require(fig['column'] in columns, 'column %r not in result %s' % (fig['column'], sorted(columns)))
    elif fig['kind'] == 'chart':
        needed = [fig['x']] + [s['column'] for s in fig['series']] + ([fig['group_column']] if fig.get('group_column') else [])
        missing = [c for c in needed if c not in columns]
        _require(not missing, 'columns %s not in result %s' % (missing, sorted(columns)))
    elif fig['kind'] == 'table':
        needed = [c['column'] for c in fig['columns']] + [c['link_column'] for c in fig['columns'] if c.get('link_column')]
        missing = [c for c in needed if c not in columns]
        _require(not missing, 'columns %s not in result %s' % (missing, sorted(columns)))
        _require(any(c.get('link_column') for c in fig['columns']),
                 'every table links its rows to the site: select item_url (for organizations, build '
                 'https://next.obudget.org/i/org/<entity_kind>/<entity_id>) and set it as link_column of the name column')


# ------------------------------------------------------------------ rendering

def render_value(fig, rows):
    return format_value(rows[0][fig['column']], fig['format'])


def chart_descriptor(fig, rows):
    """A chart descriptor in the shape the site's chart-router renders: {type:'plotly', title, chart, layout}."""
    ctype = fig['chart_type']
    x = fig['x']
    if ctype == 'pie':
        s = fig['series'][0]
        merged = {}
        for r in rows:
            label = str(r[x])
            merged[label] = (merged.get(label) or 0) + (_num(r[s['column']]) or 0)
        # Same-label slices (e.g. two "other" rows) are one slice; empty and negative ones aren't slices at all.
        rows = sorted(({x: label, s['column']: v} for label, v in merged.items() if v > 0),
                      key=lambda r: -r[s['column']])
        if len(rows) > MAX_PIE_SLICES:
            head, tail = rows[:MAX_PIE_SLICES - 1], rows[MAX_PIE_SLICES - 1:]
            rows = head + [{x: 'אחר', s['column']: sum(_num(r[s['column']]) or 0 for r in tail)}]
        traces = [dict(type='pie', textinfo='label+percent', sort=False,
                       labels=[str(r[x]) for r in rows], values=[_num(r[s['column']]) for r in rows])]
        layout = {}
    else:
        # Anything over years is drawn as lines, so every page's budget-over-time chart looks the same.
        if is_time_series(fig, rows):
            ctype = 'line'
        trace_type = dict(type='scatter', mode='lines+markers') if ctype == 'line' else dict(type='bar')
        if fig.get('group_column'):
            s = fig['series'][0]
            xs = sorted({r[x] for r in rows}, key=lambda v: (v is None, v))
            groups = []
            for r in rows:
                if r[fig['group_column']] not in groups:
                    groups.append(r[fig['group_column']])
            traces = []
            for g in groups:
                values = {r[x]: _num(r[s['column']]) for r in rows if r[fig['group_column']] == g}
                traces.append(dict(trace_type, name=str(g), x=[str(v) for v in xs], y=[values.get(v) for v in xs]))
        else:
            traces = [
                dict(trace_type, name=s.get('label') or s['column'],
                     x=[str(r[x]) for r in rows], y=[_num(r[s['column']]) for r in rows])
                for s in fig['series']
            ]
        layout = dict(
            xaxis=dict(title=fig.get('x_title') or '', type='category'),
            yaxis=dict(title=fig.get('y_title') or '', rangemode='tozero', separatethousands=True),
        )
        if ctype == 'stacked_bar':
            layout['barmode'] = 'stack'
    return dict(type='plotly', title=clean_title(fig['title']), chart=traces, layout=layout)


# Values are always raw shekels, so a title that names a scale ("במיליארדי ש"ח") would misdescribe the axis.
SCALE_IN_TITLE_RE = re.compile(r'\s*\(\s*(?:ב|ב-)?(?:מיליארדי|מיליוני|אלפי|מיליארד|מיליון|אלף)\s*(?:ש"ח|ש״ח|שקלים|₪)?\s*\)')


def clean_title(title):
    return SCALE_IN_TITLE_RE.sub('', title or '').strip()


def chart_markdown(descriptor):
    return '```plotly\n%s\n```' % json.dumps(descriptor, ensure_ascii=False, indent=1)


IDENTIFIER_COLUMN_RE = re.compile(r'(number|(^|_)id$|code|^year$|_year$)', re.IGNORECASE)


def _column_format(c):
    """Identifiers (decision numbers, ids, codes, years) are never thousands-separated, whatever format was asked."""
    fmt = c.get('format') or 'text'
    if fmt == 'number' and IDENTIFIER_COLUMN_RE.search(c['column']):
        return 'text'
    return fmt


def _empty_amount(v):
    n = _num(v)
    return n is None or n == 0


def render_table(fig, rows):
    columns = [dict(c, format=_column_format(c)) for c in fig['columns']]
    money = [c for c in columns if c['format'] == 'currency']
    if money:
        # In a table about money, a row with no amounts and a column with no amounts say nothing.
        rows = [r for r in rows if not all(_empty_amount(r.get(c['column'])) for c in money)] or rows
        empty = {c['column'] for c in money if all(_empty_amount(r.get(c['column'])) for r in rows)}
        if empty and len(empty) < len(columns):
            columns = [c for c in columns if c['column'] not in empty]
    lines = []
    if fig.get('title'):
        lines.append('**%s**' % fig['title'])
        lines.append('')
    lines.append('| %s |' % ' | '.join(_cell(c.get('label') or c['column']) for c in columns))
    lines.append('|%s|' % '|'.join('---' for _ in columns))
    for r in rows[:MAX_TABLE_ROWS]:
        cells = []
        for c in columns:
            text = format_value(r.get(c['column']), c['format'])
            url = r.get(c['link_column']) if c.get('link_column') else None
            cells.append('[%s](%s)' % (_cell(text), url) if url else _cell(text))
        lines.append('| %s |' % ' | '.join(cells))
    return '\n'.join(lines)


TREND_SERIES = [('allocated', 'תקציב מקורי'), ('revised', 'תקציב מאושר'), ('used', 'ביצוע')]

# Gaps in the data that have a known, factual reason.
KNOWN_GAPS = {
    2020: 'בשנת 2020 לא אושר תקציב מדינה, ולכן אין לשנה זו נתוני תקציב מקורי.',
}


def trend_descriptor(fig, trend):
    """The budget-over-time chart: the same three line series on every page, with estimated years shaded."""
    rows = trend['rows']
    xs = [str(r['year']) for r in rows]
    traces = [dict(type='scatter', mode='lines+markers', name=label, x=xs, y=[r[key] for r in rows])
              for key, label in TREND_SERIES]
    layout = dict(
        xaxis=dict(title='שנה', type='category'),
        yaxis=dict(title='₪', rangemode='tozero', separatethousands=True),
    )
    before = trend.get('estimate_before')
    estimated = [i for i, x in enumerate(xs) if int(x) < before] if before else []
    if estimated:
        # On a category axis Plotly reads shape and annotation x values as category indices, not labels
        # ("2015" would mean index 2015), so the shaded span is given by position.
        layout['shapes'] = [dict(type='rect', xref='x', yref='paper', x0=-0.5, x1=estimated[-1] + 0.5, y0=0, y1=1,
                                 fillcolor='rgba(128,128,128,0.12)', line=dict(width=0), layer='below')]
        layout['annotations'] = [dict(xref='x', yref='paper', x=-0.5, y=1, xanchor='left', yanchor='bottom',
                                      showarrow=False, text='הערכה')]
    return dict(type='plotly', title=clean_title(fig['title']), chart=traces, layout=layout)


def trend_notes(trend):
    """Fixed, factual notes for the caption under a budget trend chart."""
    notes = []
    rows = trend['rows']
    before = trend.get('estimate_before')
    if before and rows and rows[0]['year'] < before:
        notes.append('חלק מסעיפי התקציב בתרשים אינם מופיעים בתקציב שלפני %d, בשל שינויים במבנה התקציב או '
                     'סעיפים חדשים, ולכן הנתונים לשנים שלפני %d הם הערכה בלבד.' % (before, before))
    for r in rows:
        if r['allocated'] is None and r['year'] in KNOWN_GAPS and (r['revised'] is not None or r['used'] is not None):
            notes.append(KNOWN_GAPS[r['year']])
    return notes


def trend_anomalies(fig, trend, latest_year=None):
    """Anomalies the text must address: only from the first year that isn't an estimate, and ignoring known gaps."""
    before = trend.get('estimate_before') or 0
    rows = [r for r in trend['rows'] if r['year'] >= before]
    as_chart = dict(kind='chart', x='year', series=[dict(column=k, label=l) for k, l in TREND_SERIES])
    return [a for a in anomalies(as_chart, rows, latest_year)
            if not any(('no value for %d' % y) in a for y in KNOWN_GAPS)]


def render_figure(fig, rows):
    if fig['kind'] == 'value':
        return render_value(fig, rows)
    if fig['kind'] == 'chart':
        return chart_markdown(chart_descriptor(fig, rows))
    if fig['kind'] == 'budget_trend':
        return chart_markdown(trend_descriptor(fig, rows))
    return render_table(fig, rows)


def data_hash(results):
    """Stable hash of all figure results, keyed by figure name."""
    canonical = json.dumps(results, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


# ------------------------------------------------------------------ time-series checks

CODE_LITERAL_RE = re.compile(r"'(\d{2}(?:\.\d{2}){0,3})'")
JUMP_RATIO = 1.5            # year-over-year change that counts as an anomaly (either direction)
JUMP_MIN_SHARE = 0.05       # ignore changes between values this small relative to the series' maximum


def is_budget_time_series(fig, rows):
    return fig['kind'] == 'chart' and fig.get('dataset') == 'budget_items_data' and is_time_series(fig, rows)


def chart_codes(fig):
    """The budget codes a chart's SQL refers to (exact codes, or prefixes used with LEFT(code, n))."""
    return sorted(set(CODE_LITERAL_RE.findall(fig['sql'])))


def chart_years(fig, rows):
    return sorted({int(r[fig['x']]) for r in rows})


MIN_CODE_SHARE = 0.05       # lines smaller than this share of the chart's yearly total may come and go


def missing_code_years(codes, years, code_amounts):
    """{code: [years in the chart's range in which the code has no budget row]}, for significant codes only.

    code_amounts: {code: {year: amount}} for the years each code exists. Small lines start and end all the
    time; a significant one missing for part of the range is what produces a false jump (renumbering).
    """
    totals = {}
    for amounts in code_amounts.values():
        for y, a in amounts.items():
            totals[y] = totals.get(y, 0) + abs(a or 0)
    missing = {}
    for code in codes:
        amounts = code_amounts.get(code, {})
        peak = max([abs(a or 0) / totals[y] for y, a in amounts.items() if totals.get(y)] or [0])
        if peak < MIN_CODE_SHARE:
            continue
        absent = [y for y in years if y not in amounts]
        if absent:
            missing[code] = absent
    return missing


def safe_range(years, missing):
    """The longest run of consecutive chart years, ending at the last one, in which every code exists."""
    bad = {y for ys in missing.values() for y in ys}
    good = []
    for y in reversed(years):
        if y in bad:
            break
        good.append(y)
    return (min(good), max(good)) if good else None


def anomalies(fig, rows, latest_year=None):
    """Sharp year-over-year changes, gaps and negative values in each series of a time-series chart."""
    found = []
    series = []
    if fig.get('group_column'):
        col = fig['series'][0]['column']
        for g in sorted({r[fig['group_column']] for r in rows}, key=str):
            series.append((str(g), {int(r[fig['x']]): _num(r[col]) for r in rows if r[fig['group_column']] == g}))
    else:
        for s in fig['series']:
            series.append((s.get('label') or s['column'], {int(r[fig['x']]): _num(r[s['column']]) for r in rows}))
    for label, values in series:
        years = sorted(values)
        top = max([abs(v) for v in values.values() if v is not None] or [0])
        for y in years:
            v = values[y]
            if v is None:
                # execution of the latest year is legitimately not in yet
                if not (latest_year and y >= latest_year and y == years[-1]):
                    found.append('"%s" has no value for %d' % (label, y))
            elif v < 0:
                found.append('"%s" is negative in %d (%s)' % (label, y, format_value(v, 'currency')))
        for a, b in zip(years, years[1:]):
            va, vb = values[a], values[b]
            if not va or not vb or va < 0 or vb < 0 or max(va, vb) < JUMP_MIN_SHARE * top:
                continue
            ratio = vb / va
            if ratio >= JUMP_RATIO or ratio <= 1 / JUMP_RATIO:
                found.append('"%s" changes from %s in %d to %s in %d (x%.1f)'
                             % (label, format_value(va, 'currency'), a, format_value(vb, 'currency'), b, ratio))
    return found
