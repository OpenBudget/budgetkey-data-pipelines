# `analysis` documents

AI-generated analysis pages that answer "what does the government do about/for X" from BudgetKey's data. They are
produced by `analysis/pages` and indexed by `index_analysis` (`budgetkey.source-spec.yaml`). This file is the
contract for the site (budgetkey-app).

## Where

- **Doc id:** `analysis/<slug>`, so the page lives at `/i/analysis/<slug>` and is fetched from `/get/analysis/<slug>`.
- **Search:** doc type `analysis`.
- **Datapackage:** `/datapackages/analysis/pages/` (resource `analysis`).

## Fields

| Field | Type | What it is |
|---|---|---|
| `slug` | string | Stable English id (e.g. `mental-health`). |
| `title` | string | Short Hebrew page title. Use as the page heading. |
| `question` | string | The question the page answers (e.g. "מה הממשלה עושה בנושא בריאות הנפש?"). Show it under the title. |
| `description` | string | 1–2 neutral sentences, no numbers. Use as the search-result subtitle and meta description. |
| `category` | string | One of a closed list (`health`, `education`, `welfare`, …; see `topics.CATEGORIES`). |
| `body` | string | The page itself, in Markdown (Hebrew, RTL). See below. |
| `charts` | array | The same charts as in `body`, as chart descriptors. Use them for the `item.charts` visualisation tabs. |
| `text` | string | `body` as plain text, for search only. |
| `needs_review` | boolean | The page was published with an open issue the automatic review couldn't resolve. Maintainers may want to see these. Readers don't need to. |
| `temporal` | boolean | The topic is tied to a period or event, such as a war. |
| `expires_at` | date | For temporal topics: after this date the page stops being updated, and it is removed once the topic expires. |
| `generated_at` | datetime | When the text was written. |
| `rendered_at` | datetime | When the numbers, charts and tables were last refreshed from the data. |
| `score` | number | Search score. |

## `body`

GitHub-flavoured Markdown. It contains:

- Level-2 sections (`## …`), always in this order:
  - **Always present:** `בקצרה`, `כמה כסף ולאן`, `מי אחראי`, `התכניות העיקריות`
  - **When the data has them:** `התקשרויות וספקים`, `תמיכות ושירותים חברתיים`, `החלטות ממשלה`
  - **Always last:** `על הנתונים`, the methodology note
- Tables (pipe tables), with links in cells.
- Links. All of them point to site pages (`https://next.obudget.org/i/...`) and have been checked to resolve. Organisation links have the form `/i/org/<kind>/<id>`.
- Charts, as fenced code blocks tagged `plotly`. Each block holds one JSON chart descriptor, the same shape `chart-router` already renders:

  ````
  ```plotly
  {"type": "plotly", "title": "...", "chart": [<plotly traces>], "layout": {...}, "description": "<html>"}
  ```
  ````

  To render one, parse the JSON, and pass `chart` as the data and `layout` as the layout to `app-chart-plotly`. On a parse error, show the block as `<pre>`.
  - **Trace types:** `scatter`, `bar` and `pie`, all available in `plotly-basic`.
  - **Charts over years** are always line charts. They may have `layout.shapes` and `layout.annotations` that shade the early years labelled "הערכה".
  - **`description`**, when present, is short HTML listing the budget lines the chart sums, each linked, and any notes. The same caption also appears in the Markdown right after the chart block, so a renderer that already shows the Markdown caption can ignore `description`.
- An italic caption line (`*…*`) under each budget chart, with linked budget lines and notes.

The page's own title is not in `body`. Render `title` and `question` above it.

## Lifecycle

- **Topics** are discovered weekly (`analysis/topics`, table `analysis_topics`).
- **Pages** are generated for new topics, and regenerated when the prompts change, after 180 days, when a new budget year arrives, or when a page's figures stop running. At most 25 generations happen per run.
- **Everything else** is refreshed: the stored SQL behind every number, chart and table is re-run, and the page is re-rendered only if the data changed.
- **If a regeneration fails**, the last published version stays.
- **Blocked topics** (`topic_overrides.yaml`) and expired temporal topics disappear from the index.
