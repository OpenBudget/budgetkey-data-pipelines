# Plan: pushing verified municipal websites back to Wikidata

Status: **planned, not started.**

The `websites` pipeline in this directory picks one website per local authority
from Wikidata, Hebrew Wikipedia infoboxes and `overrides.csv`. While building it
we found that Wikidata's `P856` (official website) is often wrong, stale or
missing for Israeli municipalities. This plan describes how to feed the verified
results back, so the sources improve and future runs need fewer overrides.

## 1. What counts as "verified"

Only push values a person has checked:

- every row in `overrides.csv`
- flagged rows (`needs_review = true`) after manual review. Each fix goes into
  `overrides.csv`, so the CSV stays the single record of human decisions.

Unflagged rows (live `.muni.il`/`.gov.il`/`.org.il` domain, all sources agree)
are likely correct but are not pushed until someone spot-checks them.

## 2. Which item gets the edit

| Authority type | Item to edit | How the pipeline finds it |
|---|---|---|
| City / local council | the settlement item (e.g. Q1218 ירושלים) | `P3466` CBS code, pinned in `wikidata-crosswalk.csv` when ambiguous |
| …that also has a municipality org item | the org item too (e.g. Q2974831 עיריית ירושלים) | `P194` / `P208` / reverse `P1001` |
| Regional council | the council item | `wikidata-crosswalk.csv` |

Wikidata convention: a city's `P856` is its municipality's site, so the
settlement item gets the `.muni.il` URL, not a tourism or community-center site.

## 3. Edits

1. **Add the correct website** as `P856` with *preferred* rank, with references:
   - `P854` reference URL → the site itself, or the data.gov.il dataset
   - `P813` retrieved → date of verification
2. **Wrong websites** (tourism sites, marina, abbey, Facebook page, archive, community center):
   - unrelated to the municipality → remove the statement
   - a real site of a different thing → *deprecated* rank
3. **Dead websites** (NXDOMAIN, e.g. `bostanalmarj.co.il`, `dabburiya.net`) → *deprecated* rank with
   `P2241` reason for deprecated rank = link rot (`Q1193907`). Keep them, don't delete.
4. **Missing websites** (e.g. Tira, Rahat, Lod, ~13 regional councils) → add as in step 1.
5. **CBS code problems found along the way**:
   - Q2777567 טמרה (יזרעאל) carries the city's code `8900` → fix to the moshav's own code
   - Q1026821 עיר הכרמל, Q2345008 שגור (dissolved mergers) still carry codes `494`, `516`
     → add `P582` end time qualifiers
   - Q1000887 יהוד and Q1948533 יהוד-מונוסון both carry `9400` → leave on the authority item only, or qualify

## 4. Tooling

- **Generate the batch from the pipeline output**: add a step (or a standalone script) that compares
  the chosen `website` to the current Wikidata `P856` for verified rows only, and writes:
  - `wikidata-quickstatements.txt`: adds and removals (QuickStatements v1 syntax)
  - `wikidata-rank-changes.csv`: deprecations, which QuickStatements cannot do
- **Apply**:
  - adds/removals → [QuickStatements](https://quickstatements.toolforge.org/) under a personal
    Wikidata account (~100 edits, no bot flag needed), with a clear edit summary
  - rank changes → Pywikibot script or manual edits (only a handful)
- **Recurring/automatic pushes** would need a bot account approved at
  *Wikidata:Requests for permissions/Bot*. Not planned for now.

Example QuickStatements lines:

```
Q1218|P856|"https://www.jerusalem.muni.il/"|S854|"https://www.jerusalem.muni.il/"|S813|+2026-10-11T00:00:00Z/11
-Q1218|P856|"https://www.gov.il/en/pages/jerusalem"
```

## 5. Before and after

- Optional: post a short note at *Wikidata:WikiProject Israel* before the batch.
- After applying, the next pipeline run should show fewer `sources-disagree` rows, and more rows with
  `website_source = wikidata` or `wikidata-org`. Overrides that Wikidata now agrees with can then be
  removed from `overrides.csv`.
- Hebrew Wikipedia infoboxes hold their own values. Fixing them is manual (no batch tool) and out of
  scope here, but the same verified list can drive it.
