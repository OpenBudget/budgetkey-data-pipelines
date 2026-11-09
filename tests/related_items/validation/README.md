# Precision validation (2026-11-08)

Hand-checked samples of links from local runs on:
- 32 Hebrew comptroller chapters (the 9- and 28-chapter samples);
- 345 government decisions (`md5(id) < '08'`);
- 312 budget-heavy decisions (`md5(id)` in `08`–`40`, text with מלש"ח / מיליון ש"ח).

Samples are one row per (source, target), spread over methods (`sample_links.py draw`). The `verdict` column is mine:
y / n / ?.

- `sc_links.csv`: comptroller sample.
- `gd_links.csv`: decisions sample.
- `bs_links.csv`: budget items and support programs from all runs.
- `blind.jsonl` + `blind_verdicts.jsonl`: the same 229 links, judged again by an independent reviewer without my verdicts.
  It agreed on every link, except two I had left unclear (sc:4, gd:162), which it judged correct.
- `bodies_review.jsonl`: an independent review of every reviewed bodies.csv row that has an entity. It found 1 wrong row
  (Shamir Medical Center → a private company), 3 aliases that named other bodies, 1 wrong budget chapter and some missing
  ones. All are fixed in bodies.csv.

Score a sample with `python -m datapackage_pipelines_budgetkey.pipelines.related_items.sample_links score <csv>`.

- `tenders.csv`: every tender link from the comptroller samples and the 569 decisions that mention a tender (4 links,
  all correct). A number alone is often several offices' tenders; the other 11 citations stayed unlinked: 6 not in
  the tenders data, 3 ambiguous, 2 with no matching publisher.

## Against the prod chapters (2026-11-08)

- `chapter_refs_prod.csv`: every chapter→chapter link from the 62 sample audits, resolved against the full prod
  `state_comptroller/reports` (5,657 chapters): 42 links, all correct (Wilson 95% lower bound 0.92); 36 by a clear title match (2 of them through
  the new fallbacks: an old annual report by its bare number, a generically named report by a clear title match in
  its year) and 6 chosen by the LLM. Of the other 34 references, most name a report or chapter that isn't in the prod data (single-subject reports
  of the 90s, local-government reports) or one too vaguely to choose.
- `bodies_prod_review.jsonl`: an independent review of the 70 new bodies.csv rows (from the 684 prod audited-body
  codes) whose entity name isn't the body's own: 68 ok, 2 doubtful (one entity cleared, one kept). The other new rows
  were checked by hand; 26 entities were found by a direct registry search where the drafts had none.

## All prod audits (2026-11-09)

Every `earlier_reports` reference in the prod `state_comptroller/chapters` run (2,290 audits; 1,739 indexed audits
cite 5,280 earlier chapters), resolved against the prod `state_comptroller/reports` (5,657 chapters):
- 3,551 links (2,849 `comptroller_ref:title`, 289 `title_in_year`, 413 `llm_resolve`). 585 more references resolve
  to a chapter without a page (`not_indexed`: before 2000, or a translation). The rest are `no_report` (526: local
  government and other reports that aren't scraped), `no_title_match` (451), and the LLM's `no_match` / `ambiguous`
  (163).
- `chapter_refs_prod_full.csv`: 40 links per method, drawn before the `not_indexed` gate (so some targets have no
  page; `has_page`). I checked them by hand, and an independent reviewer checked them blind (`reviewer`). Neither found
  a wrong link: 118 y, 2 ? (both `llm_resolve`; the cited title is narrower than the chapter's).
- `chapter_refs_year_fallback.csv`: every link added by the fallback for reports named without a code ("דוח מיוחד
  (2019)" fuzzy-matches the title of every special report that year). There are 58, 57 y and 1 ?.
- Without the gate, 481 of the 3,969 links pointed at chapters with no page: 461 to chapters from before 2000 and
  19 to Hebrew-titled chapters that were analyzed from their English abstract.
