# Brief: related pages on item pages (פריטים קשורים / מוזכר ב...)

For the frontend (budgetkey-app). The data side is in this repo: `pipelines/related_items/` (see README.md) and the
indexer step `pipelines/budgetkey/elasticsearch/add_related_items.py`.

## What it is

The linker reads documents (State Comptroller audits, government decisions, the AI analysis pages) and finds the
budgetkey records each one is about: organizations, budget items, support programs, tenders, other decisions, earlier
audits, contracts. The links can be read in both directions, so every item page can show:

- **related items**, for a document that is a source (an audit, a decision, an analysis page): what it is about;
- **mentioned in**, for any record that is a target: the documents that discuss it. For example, a budget item's page
  can list the government decisions, comptroller audits and issues, and analysis pages that discuss it.

Both are display-only. They aren't searchable, and nothing else on the page depends on them.

## Where to get it

They're three fields on the item document you already fetch, `GET https://next.obudget.org/get/<doc_id>`, under
`value`:

| Field | When | Content |
|---|---|---|
| `related_items` | the doc is a source | one entry per target, sorted by kind, then confidence, then title |
| `mentioned_in` | the doc is a target | one entry per source document, newest first, **at most 20** |
| `mentioned_in_count` | always | how many source documents mention it in total (can be > 20) |

The types that carry them are `budget`, `entities` (org), `support_programs`, `tenders`, `gov_decisions`, `analysis`,
`contract-spending`, `supports`, `activities`, `state_comptroller_audits` and `state_comptroller_issues`. Other types
don't have the fields. Treat a missing field the same as an empty one.

### `related_items[]`

```json
{
  "doc_id": "gov_decisions/e9df0ca8-b543-4da0-8878-3a2efc1babbb",
  "kind": "gov_decisions",
  "title": "סיוע ליישובים ולרשויות באזור ים המלח להתמודדות עם נזקי הבולענים ...",
  "relation": "cites",
  "relations": ["cites"],
  "confidence": 0.9,
  "parts": ["10624-1", "10624-7"],
  "year": null,
  "code": null
}
```

- `relation`: the strongest relation of the links to this target. `relations` lists all of them.
- `parts`: the parts of the source that make the link. On an audit, these are its finding ids (`<chapter_id>-<seq>`).
- `year`, `code`: budget targets only. `year` is the year the source refers to, and `code` is the item's code in that
  year.

### `mentioned_in[]`

```json
{
  "doc_id": "state_comptroller_audits/10624",
  "kind": "state_comptroller_audits",
  "title": "היבטים בהתמודדות הממשלתית עם ירידת מפלס המים באגן הצפוני של ים המלח והשפעותיה",
  "date": "2026-06-30",
  "relation": "cites",
  "confidence": 0.9,
  "excerpt": "[10624-1] אי-הגשת המלצות סופיות ... שהוקם מכוח החלטת ממשלה 3742 מאפריל 2018 ...",
  "part_id": "10624-1",
  "parts": ["10624-1"],
  "year": null,
  "same_item": true
}
```

- `excerpt`: the passage around the mention, up to about 300 characters. Comptroller excerpts start with
  `[<finding id>] `; strip that prefix and use `part_id` instead.
- `part_id`: the finding (on an audit) where the best mention is. Use it to link to the finding on the audit's page,
  e.g. `/i/state_comptroller_audits/10624#finding-10624-1`. The audit page needs to render that anchor; see the
  comptroller brief.
- `date`: the source document's date, `YYYY-MM-DD`. It can be null.
- **Budget pages only:**
  - A budget doc stands for a connected item across years, so its back-links cover all its years. `year` is the year
    the source cited.
  - After the item's own entries come links to other connected items with the same code. They're marked
    `same_item: false` (e.g. "the code 20-38-02 in 2019, before the item was split"), and are worth showing apart or
    labelled with their year.

## Linking to an entry

Every `doc_id` is the site's item id, so the page is `/i/<doc_id>` (e.g. `/i/org/company/510000011`,
`/i/budget/0020380201/2026`, `/i/gov_decisions/<id>`). `kind` is the doc type: the prefix of `doc_id`, except that
organizations are `org/...` with kind `entities`.

Suggested Hebrew labels:

| kind | label |
|---|---|
| `entities` | גופים |
| `budget` | סעיפי תקציב |
| `support_programs` | תוכניות תמיכה |
| `supports` | תמיכות |
| `tenders` | מכרזים |
| `contract-spending` | התקשרויות |
| `gov_decisions` | החלטות ממשלה |
| `state_comptroller_audits` | דוחות מבקר המדינה |
| `state_comptroller_issues` | ליקויים במעקב (מבקר המדינה) |
| `analysis` | ניתוחים |
| `activities` | שירותים חברתיים |

| relation | label (on the target's "mentioned in") |
|---|---|
| `audited` | גוף מבוקר |
| `responsible` | גוף אחראי |
| `subject` | נושא הדוח / ההחלטה |
| `funded` | מימון |
| `contractor` | ספק / קבלן |
| `cites` | מצוטט |
| `amends` | תיקון החלטה |
| `cancels` | ביטול החלטה |
| `follows_up` | דוח המשך |
| `mentioned` | אזכור |

## Display suggestions

- **Mentioned in:** group by `kind` (decisions, audits and issues, analyses, ...), newest first within a group. Show
  the title, date and relation label, with the excerpt as a second line or tooltip. When `mentioned_in_count` > 20,
  show "ועוד N" (see below for the full list).
- **Related items:** group by kind. The list is already sorted. For an audit, the `parts` can say which findings
  mention the target.
- `confidence` is always ≥ 0.8 (nothing less certain is linked), so it doesn't need to be shown.
- Hide both sections when they're empty.

## More than 20 back-links

There's no API endpoint for the rest yet. The `related_items` table is in the public DB, so the full list can be read
with the query API:

```
GET https://next.obudget.org/api/query?query=select source_doc_id, source_kind, source_title, source_date, relation, excerpt from related_items where target_doc_id = '<doc_id>' order by source_date desc nulls last
```

(URL-encode the query. There's one row per source part, so dedupe by `source_doc_id`.) If you need this often, ask
and I'll add a dedicated field or endpoint.

## Status (2026-11-10)

- The fields exist on the docs now, but they're empty: the `related_items` table is written by
  `related_items/all`, which is running today. Each type gets its links at its indexer's next run after that, within
  hours to a day for most types.
- Once filled, a good first check is `/get/gov_decisions/e9df0ca8-b543-4da0-8878-3a2efc1babbb`. Its `mentioned_in`
  should list the comptroller audit 10624 (the Dead Sea report), which cites it.
- Scale: about 50k links from comptroller audits (mostly to organizations), about 34k from government decisions, and
  about 3.8k from the 100 analysis pages.
