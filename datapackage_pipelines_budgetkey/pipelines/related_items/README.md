# Related items (פריטים קשורים)

A generic linker: given source documents with free text, it finds which budgetkey records each one is about, and
stores links that can be read in both directions:
- a source's `related_items`;
- a target's back-links (`mentioned_in`): the same rows, queried by target.

| Pipeline | Reads | Writes |
|---|---|---|
| `related_items/state_comptroller` | `state_comptroller/chapters` (audits + findings) | `/var/datapackages/related_items/state_comptroller` |
| `related_items/gov_decisions` | `government_decisions` | `/var/datapackages/related_items/gov_decisions` |
| `related_items/all` | both of the above, and `state_comptroller/issues` | tables `related_items`, `related_items_mentions`; finding links lifted to their issues (`state_comptroller_issues/{issue_id}`, method `derived:issue`) |

## Tables

`related_items` has one row per (source document, source part, target):

| Field | |
|---|---|
| `link_id` | hash of source doc, part and target |
| `source_doc_id`, `source_kind`, `source_part_id` | e.g. `state_comptroller_audits/7534`, part `7534-3` (a finding); `gov_decisions/{id}` |
| `source_title`, `source_date` | |
| `target_doc_id`, `target_kind`, `target_title` | ES doc ids: `org/{kind}/{id}`, `gov_decisions/{id}`, `budget/{code}/{year}`, `support_programs/{key}`, `state_comptroller_audits/{chapter_id}`, `tenders/{type}/{publication_id}/{tender_id}` |
| `target_year`, `target_code` | budget: the year the source is about, and the item's code that year |
| `relation` | `audited`, `subject`, `responsible`, `funded`, `contractor`, `cites` / `amends` / `cancels` (decisions), `follows_up` / `cites` (comptroller chapters), `mentioned` |
| `mention`, `excerpt` | the text as it appears in the source, and the passage around it |
| `confidence`, `method` | see below |

`related_items_mentions` holds every reference found, linked or not, with its status (`linked`, `not_found`,
`ambiguous`, `no_match`, `unverified`, `no_date`, ...) and details (candidates, parsed number and date). Use it to
audit links, and to find what could be linked with more target data. For example, decisions from before 2017 aren't
in `government_decisions`, but their citations are kept with their number, date and government.

Budget docs stand for a connected item across years, with every older (code, year) redirecting to it. So links target
the connected item's doc, and back-links on a budget page cover all of its years.

## How links are made

Most certain first. Only links with confidence ≥ 0.8 are emitted.

| Method | Targets | Confidence |
|---|---|---|
| `regex:corp_number` | a valid corporation number (check digit) of an existing entity | 0.97 |
| `regex:decision:date` | "החלטת הממשלה מס' N מיום D": the decision N dated within 3 days of D | 0.98 |
| `regex:decision:month` / `date_close` / `date_government` | the date is a month, or up to a month off, or only gives the government (numbers restart per government) | 0.9 / 0.93 / 0.85 |
| `regex:decision:same_government` | an undated citation in a government decision: the latest N of the same government | 0.8 |
| `regex:decision:same_document` | an undated citation of a number already cited with a date in the same document | as that one |
| `comptroller_ref:title` | an earlier comptroller chapter the audit cites (`earlier_reports`): the report by code, or by publication title and year; the chapter by a clear title match (else `llm_resolve`) | 0.95 |
| `regex:tender` | a tender number whose publisher is a body the document is about, or is named next to it (tender publishers are bodies.csv aliases) | 0.9 |
| `regex:budget_code` | "תקנה 20-38-02-01": the item with that code in the source's years | 0.95 |
| `comptroller_code` | the comptroller's audited institutions, via `bodies.csv` | 0.95 |
| `alias` | a name listed in `bodies.csv` | 0.93 |
| `exact_name` | a unique entity with the same normalized name and a compatible kind | 0.9 |
| `llm_resolve` | the LLM chose among retrieved candidates and said "certain" | 0.85 |

The LLM (Gemini MODEL_FAST, cached) reads each document once:
- for comptroller chapters: the title, summary and findings;
- for government decisions: the full text.

It lists the organizations and the budget or support programs it names, each with a verbatim quote. A mention whose
quote isn't found in the source is dropped (`unverified`).

Candidates come from:
- organizations: name normalization plus fuzzy similarity. Private companies and partnerships are matched by number
  or exact name only.
- budget programs: embeddings of "chapter > domain > program" paths, within the budget chapters of the bodies
  involved and in the years the text refers to (else the audit period, else the publish year and the one before).
- support programs: embeddings, filtered by budget chapter and years.

A second LLM call per document picks among the candidates; anything but "certain" stays a mention.

Private individuals are never linked. Exemption committee (ועדת פטור) decisions, mostly land deals of private
people, get only the deterministic extractors.

## bodies.csv

The curated list of public bodies. It's needed because government bodies appear in `entities` under old accounting
names, as "משרד הרווחה/המשרד הראשי". Each row holds:
- `name`, plus `aliases`: the names the body went by over time;
- `entity_id`: the entity that stands for it;
- `comptroller_codes`: its codes in the comptroller's institution list;
- `budget_chapters`: its 4-digit budget chapters;
- `ubiquitous`: `yes` for bodies named in passing in most documents of a kind (the Knesset, in every bill's
  decision), which are linked only in a more specific role than `mentioned`;
- `status`: `ok` (reviewed), `draft` or `not_a_body` (a generic phrase or a private company, left to name matching).

Only `ok` rows are used. An `ok` body without an `entity_id` has no entity record (the IDF, the Ministry of Defense
HQ). Its mentions are recorded as `no_entity` rather than fuzzy-matched to a namesake, but its budget chapters still
narrow budget matching. Drafts come from `build_bodies.py`, which collects names from the sources, retrieves
candidates and lets Gemini propose; every row is then checked by hand. Re-run it when new names show up in
`related_items_mentions` with status `not_in_bodies`. Existing rows are kept, and new ones are appended as drafts.

### Bodies without an entity

Some of the most often named bodies have no record in `entities`. Their mentions are kept as `no_entity`. Counts
are from about 900 sample decisions and 32 chapters:

| Body | Mentions |
|---|---|
| משרד הביטחון (the ministry's HQ; only its units have records) | 199 |
| צבא ההגנה לישראל / צה"ל (only corps and directorates have records) | ~100 |
| המשרד לביטחון הפנים / המשרד לביטחון לאומי (HQ) | ~125 |
| מועצת מקרקעי ישראל | 80 |
| מזכירות הממשלה | 43 |
| המטה לביטחון לאומי, שירות הביטחון הכללי, המוסד | ~90 |

When such an entity is added, set `entity_id` on the body's row in bodies.csv. Its past and future mentions then
link on the next run, because LLM answers are cached and only the resolution changes.

## Adding a source

Write an adapter in `sources.py` that yields documents. See the docstring of `linker.py` for their shape:
`doc_id, kind, title, date, text, llm_text, what, parts, hints`. Then add a branch in
`link_source.documents()` and a pipeline in `pipeline-spec.yaml`.

Adding a target kind (people, activities, units) means an index class with `load()`, `doc_id()` and candidate
retrieval, plus a resolver step in `DocLinker`.

## Local runs

```
export PYTHONPATH=<repo> GEMINI_API_KEY=... OPENAI_API_KEY=... AI_CACHE_DIR=<repo>/ai-cache \
       RELATED_ITEMS_CACHE=<dir for downloaded target tables>
CHAPTERS=<chapters datapackage dir> OUT=out/sc python -m datapackage_pipelines_budgetkey.pipelines.related_items.link_source state_comptroller
DECISIONS_WHERE="policy_type='החלטות ממשלה' and md5(id) < '08'" LIMIT=50 OUT=out/gd \
    python -m datapackage_pipelines_budgetkey.pipelines.related_items.link_source gov_decisions
```

Without `DPP_DB_ENGINE`, target tables are downloaded once through next.obudget.org's query API and cached.
`DOC_IDS` selects documents. `RELATED_ITEMS_MIN_CONFIDENCE`, `RELATED_ITEMS_MODEL` and `RELATED_ITEMS_THREADS`
tune a run.

## Checking a run

`check_links.py` runs read-only sanity queries on the tables, through next.obudget.org's query API or `--engine`:
counts by kind, method and mention status; duplicate ids, self-links and malformed doc ids; links whose target is
missing from its table; the busiest sources and most linked targets; and a stable sample of 20 links to check by hand.
`--sql` prints the queries for /api/query.

## In Elasticsearch

`budgetkey/elasticsearch/add_related_items.py`, an indexer `extra-step` per doc type, adds:
- `related_items` (the doc as a source): `[{doc_id, kind, title, relation, relations, confidence, parts, year, code}]`,
  one entry per target with its links folded over the doc's parts (findings). Sorted by kind, then confidence, then title;
- `mentioned_in` (the doc as a target): `[{doc_id, kind, title, date, relation, confidence, excerpt, part_id, parts, year,
  same_item}]`, one entry per source doc, newest first, the first 20; `mentioned_in_count` counts them all.

Both are display-only (`es:index: false`). `gov_decisions` and `state_comptroller_audits` are both sources and targets: a
decision's page lists what it cites and which later decisions and audits cite it. A budget doc is a connected item, so
its own back-links cover all its years (`year` is the year cited); with `code-field: code`, links to other connected
items with the same code follow, marked `same_item: false`. `part_id` (a finding) can anchor to its place on the
audit's page. Issue pages get the links lifted from their findings, so an organization's `mentioned_in` can list both
an audit and its issue.

The step runs before `manage-revisions`, so a doc whose links change is re-indexed, and its output depends only on the
table. It reads `related_items` once per run; if the table can't be read, the fields stay empty.
