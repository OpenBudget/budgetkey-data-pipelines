"""A static HTML page ranking local authorities by how openly they publish their budgets."""
import datetime
from collections import defaultdict
from html import escape

CHIPS_PER_YEAR = 4   # more files in a year are behind a "+N" toggle
LEAST_SHOWN = 20
SPREADSHEETS = {'xlsx', 'xls', 'csv'}
STATUS_HE = {
    'no-budget-page': 'לא נמצא דף תקציב',
    'no-website': 'אין אתר ידוע',
    'stale': 'האתר לא זמין כרגע (מוצגים נתונים קודמים)',
}


def grade(f):
    """How machine-readable a file is: 4 ready spreadsheet, 3 spreadsheet or tabular PDF, 2 text PDF, 1 scan."""
    if f.get('analysis_status') != 'ok' and f['format'] not in SPREADSHEETS:
        return 2  # a PDF we couldn't check: assume a text PDF
    if f['format'] in SPREADSHEETS:
        return 4 if f.get('ready_to_process') else 3
    if f.get('ready_to_process') or f.get('table_kind') == 'detailed':
        return 3
    if f.get('table_kind') in ('summary', 'none') and f.get('analysis_status') == 'ok':
        return 2
    return 1


def chip(f):
    g = grade(f)
    label = 'XLS' if f['format'] in ('xlsx', 'xls') else f['format'].upper()
    if f['format'] == 'pdf' and f.get('table_kind') == 'scanned':
        label = 'סרוק'
    mark = ' ✓' if f.get('ready_to_process') else ''
    title = ' · '.join(x for x in [
        f.get('link_text'), DOC_TYPE_HE.get(f.get('doc_type'), f.get('doc_type')),
        'מוכן לעיבוד' if f.get('ready_to_process') else None,
        f.get('value_columns'), units_he(f.get('units')), f.get('issues'),
    ] if x)
    stored = f.get('stored_url')
    copy = '<a class="copy" href="%s" title="עותק שמור">⤓</a>' % escape(stored) if stored else ''
    css = 'g%d' % g if f.get('analysis_status') == 'ok' else 'g0'
    if f.get('analysis_status') != 'ok':
        title += ' · לא נבדק'
    return '<span class="chip %s"><a href="%s" title="%s">%s%s</a>%s</span>' % (
        css, escape(f['file_url']), escape(title), label, mark, copy)


def year_cell(files):
    files = sorted(files, key=lambda f: -grade(f))
    shown, rest = files[:CHIPS_PER_YEAR], files[CHIPS_PER_YEAR:]
    return ''.join(chip(f) for f in shown) + (
        '<details class="more"><summary>+%d</summary>%s</details>' % (len(rest), ''.join(chip(f) for f in rest))
        if rest else '')


DOC_TYPE_HE = {
    'budget': 'תקציב', 'budget_proposal': 'הצעת תקציב', 'budget_update': 'עדכון תקציב',
    'development_budget': 'תקציב פיתוח / בלתי רגיל', 'budget_vs_execution': 'תקציב מול ביצוע',
}


def units_he(units):
    return {'nis': 'בש"ח', 'thousands': 'באלפי ש"ח'}.get(units)


def render(pages, files, now=None):
    now = now or datetime.datetime.now()
    by_authority = defaultdict(lambda: defaultdict(list))
    for f in files:
        by_authority[f['authority_code']][f.get('fiscal_year') or None].append(f)
    # Every year with a file, newest first; a year later than next year is a misread
    found = [y for a in by_authority.values() for y in a if y and y <= now.year + 1] or [now.year]
    years = list(range(max(found), min(found) - 1, -1))
    undated = any(None in a for a in by_authority.values())

    rows = []
    for p in pages:
        per_year = by_authority.get(p['authority_code'], {})
        grades = {y: max(grade(f) for f in per_year[y]) for y in years if per_year.get(y)}
        rows.append(dict(
            page=p, per_year=per_year, score=sum(grades.values()), years=len(grades),
            machine_years=sum(1 for g in grades.values() if g >= 3),
        ))
    rows.sort(key=lambda r: (-r['score'], -r['years'], r['page']['name']))
    for rank, r in enumerate(rows, 1):
        r['rank'] = rank

    ranked = [r for r in rows if r['score'] > 0]
    top = ranked[:10]
    least = sorted([r for r in rows if r['score'] == 0] + [r for r in ranked[10:]][-10:],
                   key=lambda r: (r['score'], r['years'], r['page']['name']))
    n_ready = sum(1 for r in rows if any(f.get('ready_to_process') and f['format'] in SPREADSHEETS
                                         for fs in r['per_year'].values() for f in fs))

    def authority_cell(r):
        p = r['page']
        name = escape(p['name'])
        if p.get('budget_page_url'):
            name = '<a href="%s" title="דף התקציב">%s</a>' % (escape(p['budget_page_url']), name)
        return name

    def site_cell(p):
        if not p.get('website'):
            return ''
        host = p['website'].split('//', 1)[-1].strip('/').removeprefix('www.')
        return '<a href="%s">%s</a>' % (escape(p['website']), escape(host))

    def status_cell(r):
        p = r['page']
        if p.get('status') == 'ok':
            return ''
        if (p.get('status') or '').startswith('unreachable'):
            return 'האתר לא נגיש'
        return STATUS_HE.get(p.get('status'), p.get('status') or '')

    body = []
    for r in rows:
        p = r['page']
        cells = ''.join('<td class="y">%s</td>' % year_cell(r['per_year'].get(y, []))
                        for y in years + ([None] if undated else []))
        body.append(
            '<tr data-score="%d"><td class="n">%d</td><td class="name">%s<div class="type">%s</div></td>'
            '<td>%s</td><td class="n score">%d</td><td class="n">%d</td><td class="n">%d</td>%s<td class="status">%s</td></tr>' % (
                r['score'], r['rank'], authority_cell(r), escape(p.get('authority_type') or ''),
                site_cell(p), r['score'], r['years'], r['machine_years'], cells, escape(status_cell(r))))

    def short_list(items, empty_reason=False):
        out = []
        for r in items:
            reason = status_cell(r) if empty_reason and r['score'] == 0 else '%d נק\' · %d שנים' % (r['score'], r['years'])
            out.append('<li>%s <span class="muted">%s</span></li>' % (authority_cell(r), escape(reason or 'רק קבצים סרוקים')))
        return '\n'.join(out)

    return TEMPLATE.format(
        generated=now.strftime('%d/%m/%Y %H:%M'),
        n_authorities=len(rows),
        n_pages=sum(1 for r in rows if r['page'].get('budget_page_url')),
        n_ready=n_ready,
        n_files=len(files),
        n_spreadsheets=sum(1 for f in files if f['format'] in SPREADSHEETS),
        top=short_list(top),
        least=short_list(least[:LEAST_SHOWN], empty_reason=True) + (
            '<li class="muted">ועוד %d רשויות</li>' % (len(least) - LEAST_SHOWN) if len(least) > LEAST_SHOWN else ''),
        year_headers=''.join('<th class="y sortable" data-type="year">%d</th>' % y for y in years) + (
            '<th class="y sortable" data-type="year">ללא שנה</th>' if undated else ''),
        rows='\n'.join(body),
        first_year=years[-1], last_year=years[0],
    )


TEMPLATE = '''<!doctype html>
<html lang="he" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>שקיפות תקציבית ברשויות</title>
<style>
:root {{
  --bg: #fbfaf8; --fg: #1f2328; --muted: #6b7076; --line: #e3e1dc; --head: #f1efea;
  --g4: #1a7f4b; --g3: #6c9a2f; --g2: #c08a1e; --g1: #b4513a; --g0: #8a8f96; --chip-fg: #fff; --link: #0b5cad;
}}
@media (prefers-color-scheme: dark) {{
  :root {{
    --bg: #16181b; --fg: #e6e6e3; --muted: #9a9ea3; --line: #2c3036; --head: #1f2226;
    --g4: #2f9e63; --g3: #7fae3e; --g2: #c9962c; --g1: #c4614a; --g0: #5d636b; --link: #6cb0f5;
  }}
}}
* {{ box-sizing: border-box; }}
body {{ margin: 0; padding: 24px 16px 48px; background: var(--bg); color: var(--fg);
  font: 15px/1.5 "Assistant", "Segoe UI", Arial, sans-serif; }}
a {{ color: var(--link); text-decoration: none; }}
a:hover {{ text-decoration: underline; }}
h1 {{ font-size: 26px; margin: 0 0 4px; }}
h2 {{ font-size: 18px; margin: 0 0 8px; }}
.muted, .type {{ color: var(--muted); font-size: 13px; }}
.stats {{ display: flex; flex-wrap: wrap; gap: 12px; margin: 20px 0; }}
.stat {{ border: 1px solid var(--line); border-radius: 8px; padding: 10px 14px; min-width: 150px; }}
.stat b {{ display: block; font-size: 24px; }}
.lists {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; margin-bottom: 24px; }}
.lists section {{ border: 1px solid var(--line); border-radius: 8px; padding: 12px 16px; }}
.lists ol {{ margin: 0; padding-inline-start: 20px; }}
.legend {{ display: flex; flex-wrap: wrap; gap: 12px; align-items: center; margin: 8px 0 12px; font-size: 13px; }}
.controls {{ margin: 8px 0; }}
.controls input {{ padding: 6px 10px; border: 1px solid var(--line); border-radius: 6px; background: var(--bg);
  color: var(--fg); width: min(320px, 100%); }}
.wrap {{ overflow-x: auto; border: 1px solid var(--line); border-radius: 8px; }}
table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
th, td {{ border-bottom: 1px solid var(--line); padding: 6px 8px; text-align: start; vertical-align: top; }}
th {{ background: var(--head); position: sticky; top: 0; white-space: nowrap; }}
th.sortable {{ cursor: pointer; }}
th.sortable:after {{ content: " ↕"; color: var(--muted); font-size: 11px; }}
td.n, th.n {{ text-align: center; }}
td.name {{ white-space: nowrap; font-weight: 600; }}
td.y {{ min-width: 64px; }}
td.status {{ color: var(--muted); font-size: 13px; min-width: 150px; }}
.more {{ display: inline; color: var(--muted); font-size: 12px; margin-inline-start: 2px; }}
.more summary {{ display: inline; cursor: pointer; }}
.more[open] summary {{ display: none; }}
.chip {{ display: inline-flex; align-items: center; border-radius: 4px; margin: 1px; font-size: 12px;
  font-weight: 600; white-space: nowrap; }}
.chip a {{ color: var(--chip-fg); padding: 1px 6px; }}
.chip a.copy {{ padding: 1px 4px; opacity: .8; border-inline-start: 1px solid rgba(255,255,255,.35); }}
.g0 {{ background: var(--g0); }} .g4 {{ background: var(--g4); }} .g3 {{ background: var(--g3); }} .g2 {{ background: var(--g2); }} .g1 {{ background: var(--g1); }}
</style>
</head>
<body>
<h1>שקיפות תקציבית ברשויות המקומיות</h1>
<div class="muted">קבצי תקציב שפורסמו באתרי הרשויות, {first_year}–{last_year}. עודכן {generated}.</div>

<div class="stats">
  <div class="stat"><b>{n_authorities}</b>רשויות מקומיות</div>
  <div class="stat"><b>{n_pages}</b>עם דף תקציב שנמצא</div>
  <div class="stat"><b>{n_ready}</b>עם קובץ אקסל מוכן לעיבוד</div>
  <div class="stat"><b>{n_files}</b>קבצי תקציב ({n_spreadsheets} גיליונות)</div>
</div>

<div class="lists">
  <section><h2>הרשויות השקופות ביותר</h2><ol>{top}</ol></section>
  <section><h2>הרשויות הפחות נגישות</h2><ol>{least}</ol></section>
</div>

<div class="legend">
  <span class="chip g4"><a>XLS ✓</a></span> גיליון מוכן לעיבוד
  <span class="chip g3"><a>XLS</a></span> גיליון / PDF עם טבלה
  <span class="chip g2"><a>PDF</a></span> PDF עם טקסט
  <span class="chip g1"><a>סרוק</a></span> PDF סרוק או לא קריא
  <span class="chip g0"><a>PDF</a></span> לא נבדק
  <span class="muted">· ⤓ עותק שמור · ציון = סכום הדירוג הטוב ביותר בכל שנה (4 לגיליון מוכן, 1 לסריקה)</span>
</div>

<div class="controls"><input id="q" type="search" placeholder="סינון לפי שם רשות…"></div>
<div class="wrap">
<table id="t">
<thead><tr>
  <th class="n sortable" data-type="num">#</th><th class="sortable" data-type="text">רשות</th><th>אתר</th>
  <th class="n sortable" data-type="num">ציון</th><th class="n sortable" data-type="num">שנים</th>
  <th class="n sortable" data-type="num" title="שנים עם קובץ קריא למכונה">קריא</th>
  {year_headers}<th>הערות</th>
</tr></thead>
<tbody>
{rows}
</tbody>
</table>
</div>

<script>
(function () {{
  var table = document.getElementById('t'), body = table.tBodies[0];
  document.getElementById('q').addEventListener('input', function () {{
    var q = this.value.trim();
    Array.prototype.forEach.call(body.rows, function (r) {{
      r.style.display = !q || r.cells[1].textContent.indexOf(q) >= 0 ? '' : 'none';
    }});
  }});
  Array.prototype.forEach.call(table.tHead.rows[0].cells, function (th, i) {{
    if (!th.classList.contains('sortable')) return;
    var asc = false;
    th.addEventListener('click', function () {{
      asc = !asc;
      var type = th.dataset.type, rows = Array.prototype.slice.call(body.rows);
      function key(r) {{
        var c = r.cells[i];
        if (type === 'text') return c.textContent;
        if (type === 'year') return c.querySelectorAll('.g4').length * 100 + c.querySelectorAll('.chip').length;
        return parseFloat(c.textContent) || 0;
      }}
      rows.sort(function (a, b) {{
        var x = key(a), y = key(b), d = typeof x === 'string' ? x.localeCompare(y, 'he') : x - y;
        return asc ? d : -d;
      }});
      rows.forEach(function (r) {{ body.appendChild(r); }});
    }});
  }});
}})();
</script>
</body>
</html>
'''
