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

## Surfacing links in Elasticsearch (proposed, not implemented)

An indexer `document-step` per doc type, reading `related_items` by doc id:
- **sources** (`state_comptroller_audits`, `gov_decisions`): `related_items: [{doc_id, kind, title, relation, part_id, year}]`,
  grouped by target kind, best confidence first;
- **targets** (`entities`, `budget`, `gov_decisions`, `support_programs`): `mentioned_in: [{doc_id, kind, title, date, relation,
  part_id, excerpt}]`, the 20 most recent, plus `mentioned_in_count` per source kind.

`gov_decisions` is both a source and a target: a decision's page lists what it cites and which later decisions and
audits cite it. Budget back-links already sit on the connected item's doc, so they cover all of its years. A finding
(`source_part_id`) can link to its anchor on the audit's page. Links of a lower confidence than the page shows by
default can stay in the table for search and auditing.
