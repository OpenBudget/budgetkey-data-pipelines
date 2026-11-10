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

## Test documents

All of these were checked against `https://next.obudget.org/get/<doc_id>` on 2026-11-10. The counts can move by a
few at each re-index; see the status below.

### Government decisions with `related_items`

| doc_id | title | `related_items` | also |
|---|---|---|---|
| `gov_decisions/ed4ad920-872d-42c2-a121-5cbeb46e7a92` | יישום הסכמים קואליציוניים בשנת הכספים 2025 - תיקון החלטות ממשלה | 25: 20 decisions, 2 orgs, 2 support programs, 1 budget item; relations `cites`, `amends`, `funded`, `mentioned` | `mentioned_in`: 1 decision |
| `gov_decisions/139b12aa-6bf1-4149-aa58-561048fa34b6` | תוספת תקציב בשנת 2026 – סיוע לגופים הפועלים בתחומי התרבות והספורט ... | 11: 6 decisions, 2 orgs, 2 budget items, 1 support program; all five relation kinds | `mentioned_in`: 1 analysis page |
| `gov_decisions/bfcfa076-c607-418f-800f-c9cb79c0aa20` | חיזוק החינוך הבלתי פורמאלי ותיקון החלטת ממשלה | 14: 10 decisions, 2 budget items, 1 org, 1 support program | no `mentioned_in` (the empty-section case) |
| `gov_decisions/0587fb6d-67b8-43e6-a1c7-a91b7e815f2e` | תכנית לטיפול בתופעות הפשיעה והאלימות בחברה הערבית 2022-2026 | 33: 24 orgs (mostly `responsible`), 8 decisions, 1 support program | `mentioned_in`: 18, from decisions, audits, an issue and an analysis page: **both sections full** |
| `gov_decisions/128f95fa-2c32-4159-a2fc-cbb334813d10` | הארכת סיוע המדינה לתושבי יישובי הדרום ... | 7 | `mentioned_in`: exactly 20, so the cap (`mentioned_in_count` = 20, no "ועוד") |

### Docs with back-links (`mentioned_in`)

| doc_id | page | `mentioned_in` | shows |
|---|---|---|---|
| `org/government_office/500100904` | משרד הבריאות | 20 of **2,665** | the "ועוד N" case, and the full-list query |
| `org/association/580151637` | המרכז הארצי לפתוח המקומות הקדושים | 20 of 22 | just over the cap; all four source kinds |
| `org/municipality/500205026` | מ. מ. ירכא | 19 | a municipality: 7 audits, 8 issues, 3 decisions, 1 analysis page |
| `budget/00045704/2020` | מטה ישראל דיגיטאלית | 13 | a budget item: decisions, an audit, issues |
| `budget/00242003/2026` | קופות ובתי חולים | 7 | **`same_item: false`** entries (year 2012), from a connected item with the same code |
| `budget/00206701/2026` | פעילויות ופרוייקטים | 20 of 22 | a budget item cited across 7 years (`year` varies) |
| `support_programs/0018010101_מענקאיזוןלרשויו` | מענק איזון לרשויות | 20 of 38 | a support program: decisions, audits, issues, analysis |
| `tenders/office/4000525105/83/2020` | מכרז 83/2020 (קטיעת שרשרת ההדבקה) | 1 | a tender cited by one audit (`part_id` → finding anchor) |
| `contract-spending/משרד_ירושלים_ומסורת_ישראל/4502359277/0083030402` | תכנית אגן העיר העתיקה | 3 | a contract, from analysis pages only (no excerpt `[id]` prefix) |
| `gov_decisions/e9df0ca8-b543-4da0-8878-3a2efc1babbb` | בולעני ים המלח | 11 | a decision cited by the Dead Sea audit, `state_comptroller_audits/10624` (`part_id` `10624-1`) |

### Other sources

- `analysis/ethiopian-israelis`: an analysis page with 95 `related_items` across six kinds (50 budget items, 11
  decisions, 11 orgs, 10 support programs, 10 contracts, 3 social services). The long-list case.
- `state_comptroller_audits/624`: an audit with 14 `related_items` (9 orgs, 5 earlier audits) and 20 `mentioned_in`,
  all later audits citing it.
- `state_comptroller_audits/10624`: the Dead Sea audit, 25 `related_items`, where `parts` lists the findings.
- `state_comptroller_issues/1603-1`: an issue followed through 8 audits and 2 responses (10 timeline entries).

## Status (2026-11-10, evening)

- **Filled:** the `related_items` table is complete, rebuilt at 20:17 with 113k links:
  - 52k from comptroller audits (mostly to organizations);
  - 35k from government decisions;
  - 22k from comptroller issues;
  - 4k from the analysis pages.
- **The docs lag the table.** Each type's docs carry the table as it was at that type's last index run. Most types
  were indexed from the 11:45 version, before today's issue rebuild.
- **Re-index timing:**
  - Organizations are being re-indexed tonight.
  - The comptroller types, analysis and social services (`activities`, which have no back-links yet) re-index
    tonight or tomorrow.
  - Budget, contracts, supports, support programs and tenders re-index on Friday 2026-11-13 (the weekly re-index).
  - Decisions re-index daily.
- **What changes when they catch up:**
  - Entries pointing to `state_comptroller_issues/10624-*` drop out of the e9df0ca8 decision's `mentioned_in`
    (11 → 2). Those issues now have one finding each, so they have no page.
  - Issue `1603-1`'s `related_items` drops from 31 to 8.
  - Issue `state_comptroller_issues/1085-6` (7 audits, 19 responses) gets its 6 `related_items`.
  - Until then, an entry can point to an issue whose page doesn't exist: treat a failed fetch as "no page" and skip it.
