# Brief: State Comptroller dashboard data (דוחות מבקר המדינה)

For the frontend (budgetkey-app). The data side is `pipelines/state_comptroller/`: `taxonomy.py` tags the audits and
names the special reports, and `dashboard.py` builds the aggregates. Graphic design comes later. This brief is about
what data exists and how to fetch it.

## How to fetch

Every table is in the budgetkey DB. Read it with the query API:

```
GET https://next.obudget.org/api/query?query=<url-encoded SQL>[&page=N]
→ {"rows": [...], "total": N, "pages": N, "page_size": 1000, "download_url": "..."}
```

- Arrays and objects (`domains`, `share_by_comptroller`, `chapters`...) are jsonb and come back as JSON.
- Dates come back as `YYYY-MM-DD` strings.
- The tables are rebuilt after each weekly comptroller run (Saturdays), together with the search index.
- No auth. `download_url` gives the same rows as CSV.

## The two closed lists

Every analysed audit (the 2,209 Hebrew chapters from 2000 on) is tagged with keys from two closed lists. Gemini does
the tagging from the chapter's title, summary and topics, and it has been spot-checked by hand.
- **Domain** (תחום): the policy area, 20 keys, e.g. `health`, `defense`, `local_gov`. Each audit has a main domain
  (`domain`) and possibly a secondary one, and `domains[]` holds both.
- **Lens** (עדשת ביקורת): the kind of problem examined, 14 keys, e.g. `procurement`, `integrity`, `it_cyber`. Each
  audit has one to three, in `lenses[]`.

The labels:

```sql
select axis, key, label, description, display_order, search_url
from state_comptroller_taxonomy order by axis, display_order
```

`axis` is `domain` or `lens`. Always show `label`; keys are for filters and URLs only.

## Search filters (budgetkey-app change needed)

Every `search_url` in these tables opens the comptroller search tab filtered by one value:

```
https://next.obudget.org/s?q=&dd=state_comptroller&domain=health
https://next.obudget.org/s?q=&dd=state_comptroller&lens=integrity
https://next.obudget.org/s?q=&dd=state_comptroller&event=8242
```

For these to filter, the `state_comptroller` tab in `theme.budgetkey.he.ts` needs three more `filterMenu` entries,
next to `publication_type`. They use the existing `dynamicOptions` mechanism:

```json
{"id": "domain", "display": "תחום", "options": [{"id": "all", "display": "כל התחומים"}],
 "dynamicOptions": {"query": "SELECT key AS id, label AS display FROM state_comptroller_taxonomy WHERE axis = 'domain' ORDER BY display_order",
                    "filterField": "domains"}},
{"id": "lens", "display": "סוג הליקוי", "options": [{"id": "all", "display": "כל הסוגים"}],
 "dynamicOptions": {"query": "SELECT key AS id, label AS display FROM state_comptroller_taxonomy WHERE axis = 'lens' ORDER BY display_order",
                    "filterField": "lenses"}},
{"id": "event", "display": "דוח מיוחד", "options": [{"id": "all", "display": "כל הדוחות"}],
 "dynamicOptions": {"query": "SELECT event AS id, name AS display FROM state_comptroller_dash_events ORDER BY last_published DESC",
                    "filterField": "event"}}
```

- The ES audit documents have `domains`, `lenses` and `event` as keyword fields, so `{"domains": "health"}` filters
  exactly.
- Issues also carry `domains`/`lenses`, but issues aren't in search (budgetkey-api PR #8). They're there for the issue
  pages and for later use.
- The URL parameter names (`domain`, `lens`, `event`) are the menu ids above. If you rename a menu, tell me and I'll
  change `search_url` in the tables.

## 1. Lenses: what kind of problems the comptroller looked at, and how that changed

**Per comptroller and per lens: `state_comptroller_dash_trends`** (34 rows: 20 domains + 14 lenses)

```sql
select key, label, n, share, share_by_comptroller, peak_comptroller, low_comptroller,
       odds_ratio_per_decade, p_trend, p_comptroller, pattern, search_url
from state_comptroller_dash_trends where axis = 'lens' order by n desc
```

- `n`, `share`: audits with this lens, and their share of all 2,209.
- `share_by_comptroller`: e.g. `{"goldberg": 0.18, "lindenstrauss": 0.23, "shapira": 0.18, "englman": 0.07}`. Each
  value is the share of that comptroller's audits.
- `pattern` is the headline:
  - `comptroller`: the share shifts with the comptroller, beyond any long-term trend (e.g. integrity: peak under
    Lindenstrauss, low under Englman).
  - `rising` / `falling`: a steady trend across the years, not tied to who's in office (e.g. IT and cyber rising,
    budget and finance falling).
  - `stable`.
- `odds_ratio_per_decade`, `p_trend`, `p_comptroller`: the statistics behind `pattern`, for a tooltip or a methods
  note. The threshold is p < 0.01.
- The same table has `axis = 'domain'` rows, if the dashboard wants the same view for policy areas.

**The comptrollers: `state_comptroller_dash_comptrollers`**: one row per comptroller since 2000.

```sql
select comptroller, name, start_date, end_date, n_audits, n_findings, findings_per_audit,
       special_share, follow_up_share, high_severity_share, first_year, last_year
from state_comptroller_dash_comptrollers order by start_date
```

- `comptroller` is the key used in `share_by_comptroller` and in `period` below.
- `end_date` is null for the one in office.

**Year by year, for a stacked chart: `state_comptroller_dash_counts`**, the same table as the topic map (section 3):

```sql
select period as year, key, label, n, total, share
from state_comptroller_dash_counts where axis = 'lens' and period_kind = 'year' order by period, key
```

An audit can have up to three lenses, so a year's shares add up to more than 1.

## 2. Special reports and the events behind them

**`state_comptroller_dash_events`**: one row per event or special report. The 122 rows cover 138 special reports.
The 12 reports on the 7 October war are one row, and so are the 4 on COVID.

```sql
select event, name, kind, event_when, description, first_published, last_published, year, comptroller_name,
       n_reports, n_chapters, n_findings, n_high_severity, domains, lenses, reports, chapters, search_url
from state_comptroller_dash_events where kind = 'event' order by last_published desc
```

- `kind`:
  - `event` (21 rows): a disaster, war, operation, crisis or affair, e.g. the Carmel fire, the Second Lebanon War, the
    Marmara flotilla, COVID, the 7 October war.
  - `thematic` (95): a topic examined in depth.
  - `follow_up` (4) and `other` (2).
  - For a "special events" widget, filter on `kind = 'event'`. The thematic reports are an optional second list.
- `name`: a short public name, e.g. "השרפה בכרמל". `event_when` says when the event happened (e.g. "דצמבר 2010"), and
  `description` is one sentence.
- `reports[]`: `{pub_id, title, publish_date, page_url}` per report. `page_url` is the report on the comptroller's
  site.
- `chapters[]`: `{chapter_id, title, url}`, where `url` is our audit page.
- `domains`, `lenses`: the top 3 keys across its chapters.
- `search_url`: all its audits in search (needs the `event` filter menu above).
- `event` is the row's key: the first report's `pub_id`, as a string.

## 3. Topic map (closed list) and how it changes over time

**`state_comptroller_dash_counts`**: per domain and lens, per period: 34 keys × 32 periods = 1,088 rows.

```sql
select key, label, period_kind, period, period_label, n, total, share, lift, search_url
from state_comptroller_dash_counts where axis = 'domain' and period_kind = 'comptroller'
```

- `period_kind`:
  - `all`: one period, `period = 'all'`.
  - `comptroller`: `period` is the comptroller key, `period_label` his name.
  - `year`: `period` is the year as a string, 2000–2026.
- `n`: audits in the period with this key. `total`: the period's audits. `share = n / total`.
- `lift`: the period's share against the overall share. Above 1 means the topic stood out in that period. It's a
  natural "what stood out" signal for highlighting or ordering a period's words.
- For the word map, use `n` or `share` for size, `axis = 'domain'` for the words (20 policy areas), and
  `search_url` for the link. `axis = 'lens'` gives a second map, of kinds of problems.
- Year by year, small years (49 audits in 2023 and in 2025) are noisy. Comptroller periods are steadier.

## 4. Longest-running unresolved issues

**`state_comptroller_dash_chronic_issues`**: issues the comptroller audited at least twice and, at the latest
follow-up audit, found not fixed or only partly fixed. There are 1,223, ordered by how long they've been open.

```sql
select rank, issue_id, title, description, bodies, severity, domains, lenses,
       first_reported, first_chapter_title, latest_verdict_label, latest_verdict_date, latest_verdict_chapter_title,
       years_open, n_audits, n_responses, government_status, url
from state_comptroller_dash_chronic_issues where chain_rank = 1 order by rank limit 12
```

- `url` is the issue page (`/i/state_comptroller_issues/<issue_id>`), the card's link.
- `years_open` runs from `first_reported` to the latest verdict. The top cards run 20–26 years: hospitals' emergency
  readiness from 2000 to the 7 October report, nursing benefits, cellular radiation rules.
- `latest_verdict` is `not_rectified` or `partially_rectified`. `latest_verdict_label` is the Hebrew label to show.
- `government_status` is the latest status the government gave in its own response books (`in_progress`,
  `rectified`, `disputed`...), or null. It can contrast with the comptroller's verdict.
- `chain_rank`:
  - Several findings of the same pair of audits often qualify together (e.g. four quarry-restoration findings, 2000 →
    2021).
  - `chain_rank = 1` is each chain's longest-running finding. Filter on it so every card tells a different story:
    473 issues, 87 of them open 10+ years.
- `first_chapter_title` and `latest_verdict_chapter_title` are the audits it spans: "first reported in …, still open
  in …".

## Caveats

- **The tags and event names are LLM output.** They're good on a hand-checked sample, but don't present an
  individual tag as an official classification. Label them e.g. "סיווג אוטומטי".
- **Comptroller terms:** Goldberg 1998–2005, Lindenstrauss 2005–2012, Shapira 2012–2019, Englman 2019–. Englman's
  term ended in July 2026 and the office has been vacant since. Until a successor is appointed, reports published
  since then count as Englman's. A successor will be a new row in `COMPTROLLERS` in `dashboard.py`. Reports
  published in a comptroller's first year were mostly prepared under the previous one.
- **Links between audits are automatic:** the chronic issues rely on findings linked across audits decades apart. The
  top cards were checked by hand and look right, but a card is a claim. The issue page shows the full timeline.
- Data covers the Hebrew chapters from 2000 on.
