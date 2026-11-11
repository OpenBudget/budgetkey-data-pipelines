# Brief: State Comptroller item pages (דוחות מבקר המדינה)

For the frontend (budgetkey-app). Two new item types need pages. The data side is `pipelines/state_comptroller/`; the
ES documents are built in `es_documents.py`, which is the authoritative field list.

| Type | doc_id | Page | Count |
|---|---|---|---|
| `state_comptroller_audits` | `state_comptroller_audits/<chapter_id>` | one audit: a chapter of an annual or special report, with its findings | 2,170 (Hebrew chapters from 2000 on) |
| `state_comptroller_issues` | `state_comptroller_issues/<issue_id>` | one deficiency tracked over time: across audits, follow-up audits and the government's responses | about 8–9k (changes with today's re-run) |

Fetch either with `GET https://next.obudget.org/get/<doc_id>` (the document is under `value`). The indexer adds
`page_title` ("דוח מבקר המדינה - ..." / "מעקב ליקוי מדוחות מבקר המדינה - ..."), `item-url` (the short `/i/<hash>` url)
and `doc_id`. Both types also carry `related_items` / `mentioned_in` / `mentioned_in_count` (see
`pipelines/related_items/FRONTEND.md`):
- an audit's `related_items` are the organizations, budget items, decisions and earlier audits it's about;
- its `mentioned_in` lists later audits and decisions that cite it;
- an issue's `related_items` gathers the targets of all its findings.

## Audit page (`state_comptroller_audits`)

**Header**

- `title`: the chapter's title.
- `publication_title`: the report it's in.
- `report_code`: e.g. `75א`; empty for most special reports.
- `publication_type`: `annual` (דוח שנתי) or `special` (דוח מיוחד).
- `publish_date` (`YYYY-MM-DD`), plus `year`.
- `audited_bodies` (names), with `main_body` as the lead body.
- `audit_period`: free text, e.g. "מרץ 2025 - פברואר 2026". `audit_period_start_year` and `audit_period_end_year`
  give it as numbers.

**Summary**

- `audit_subject` (one line).
- `summary` (a paragraph, LLM-written from the chapter).
- `topics[]` (tags).
- `key_figures[]`: `{label, value}` pairs such as {"label": "ירידת מפלס ים המלח מאוקטובר 1976 עד יוני 2025", "value": "כ-42
  מטרים"}. They suit a row of stat cards.

**Follow-up context**

- `follow_up`:
  - `none`;
  - `includes_follow_up`: the chapter also checks earlier findings;
  - `follow_up_audit`: the chapter is a follow-up audit.
- For a follow-up, `follows_up_report`, `follows_up_year` and `follows_up_title` name the original audit. The
  original's page, when it has one, is in `related_items` with relation `follows_up`.
- `earlier_reports[]`: `{report, year, title, relation}`, every earlier report the chapter cites. `relation` is
  `followed_up` or `referenced`. These are the comptroller's own references, and not all of them have pages. Link the
  ones that appear in `related_items` (kind `state_comptroller_audits`) and show the rest as plain text.

**Findings** (`findings[]`, `n_findings`): this is the core of the page. Render each with the anchor
`id="finding-<finding_id>"`, because back-links from other pages point there (`part_id`).

| Field | |
|---|---|
| `finding_id`, `seq` | `<chapter_id>-<seq>`; order by `seq` |
| `title`, `description` | the deficiency (or a positive point) |
| `recommendation` | the comptroller's recommendation; can be null |
| `section` | the chapter section it's in (for grouping or a breadcrumb); can be null |
| `finding_type` | `deficiency` (ליקוי), `positive` (ממצא חיובי), `observation` (הערה) |
| `severity` | `high` / `medium` / `low` |
| `responsible_bodies[]` | names |
| `body_responses[]` | `{body, date, summary}`: what the audited bodies answered, as quoted in the report ("X מסר בתשובתו...") |
| `followup_status`, `followup_status_text` | on follow-ups: `rectified` (תוקן), `mostly_rectified` (תוקן במידה רבה), `partially_rectified` (תוקן חלקית), `not_rectified` (לא תוקן); `not_applicable` otherwise |
| `issue_id`, `issue_size` | link to the issue page **only when `issue_size > 1`**; a single-finding issue has no page |
| `verified` | false when the finding's quote couldn't be found in the text: show it, perhaps with a subtle mark |

`tracked_issues` counts the findings tracked elsewhere, i.e. those with an issue page. `issue_ids[]` is for search, so
use the per-finding fields instead.

**Files and source**

- `pdf_url`, `docx_url`: the full chapter, our copy.
- `abstract_pdf_url`, `abstract_docx_url`: the abstract (תקציר).
- `source_url`: the original file on the comptroller's site.
- Any of these can be null.

**Full text**

- `text` is the chapter as markdown, and long: a median of about 63k characters, 18k–166k for most chapters, at most
  1M. It suits a collapsed "הדוח המלא" section, rendered as markdown (it has tables).
- `md_url` is the same text as a file. `text_truncated` is true for one chapter only.

**Status**: `analysis_status` is `ok`, `partial-text` (part of the text couldn't be extracted, so findings may be
missing; worth a small note) or `no-findings`.

## Issue page (`state_comptroller_issues`)

An issue is one deficiency followed through every source that discusses it. The original finding, the follow-up
audits that rechecked it and the government's responses to it are each linked by the pipeline. Only issues with more
than one source get a page.

**Header**

- `title`, `description`: from the latest finding.
- `bodies[]`.
- `severity`.
- `first_reported`, `last_seen` (dates).
- `n_audits`, `n_responses`.
- `latest_status`:
  - `rectified` (תוקן), `mostly_rectified`, `partially_rectified`, `not_rectified` (לא תוקן);
  - `in_progress` (בטיפול), `disputed` (הגוף חולק על הממצא), the last two from the government's responses;
  - null.
- `chapter_ids[]` and `chapter_titles[]`: the audits involved. Their pages are `/i/state_comptroller_audits/<id>`.

**Timeline** (`timeline[]`, in date order) is the heart of the page:

| Field | |
|---|---|
| `date` | `YYYY-MM-DD` or null |
| `kind` | `finding` (first reported), `follow_up` (re-examined in a follow-up audit), `response` (the government's response book) |
| `id` | finding id, or response id |
| `chapter_id`, `source` | the audit and its title (findings and follow-ups); for responses `source` is the response book's title |
| `url` | findings: the comptroller's chapter page; responses: our copy of the response book |
| `title`, `text` | findings: title and description; responses: the item's title (often null) and the bodies' answers as `body: text` lines |
| `finding_text` | responses only (new): the finding as the response book states it; show it when `text` is empty |
| `status` | follow-ups: `followup_status` values; responses: `in_progress`, `rectified`, `disputed`, ... |

Link finding and follow-up entries to `/i/state_comptroller_audits/<chapter_id>#finding-<id>`. Some issues have many
response entries from the same book (one per row of its table). Grouping consecutive entries by `source` and date
reads better than a flat list.

## Things to know

- **When the pages ship, three switches flip:**
  - In budgetkey-app, add `'state_comptroller_audits'` and `'state_comptroller_issues'` to `LINKABLE_KINDS` in
    `item/item-related/item-related.component.ts`. Until then, the related-items sections on other pages show these
    entries as text. The `#finding-<part_id>` anchors are already wired.
  - Merge budgetkey-api PR #6 (https://github.com/OpenBudget/budgetkey-api/pull/6), which adds both types to search.
    Don't merge it earlier: the site's "הכל" search covers every API type, so results would link to pages that don't
    exist yet. Tell me if you want them grouped or labelled in search results in a particular way.
  - Add both types to the sitemaps. Tell me when the pages are live and I'll do it.
- **Data timing:**
  - The ES indexes for both types failed on their first run (fixed today, deploying with the next push).
  - The issues are being rebuilt today: an issue now holds at most one finding per audit, which splits some
    over-merged issues. Issue ids are the id of the issue's earliest finding, so most stay the same.
  - Expect the docs at `/get/` later today or tomorrow. Good test docs: audit `state_comptroller_audits/10624` (the
    Dead Sea report, 17 findings, many links), and a follow-up audit such as `state_comptroller_audits/10625`.
- **Hebrew only:** the indexed audits are the Hebrew chapters from 2000 on. Translations and older chapters aren't
  indexed. References to them stay as text (`earlier_reports`).
- **Files:** all URLs point at public files (our bucket or the comptroller's library). There's no auth.
