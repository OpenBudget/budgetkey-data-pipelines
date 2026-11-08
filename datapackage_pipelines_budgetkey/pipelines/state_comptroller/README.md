# State Comptroller reports (דוחות מבקר המדינה)

Structured data from the State Comptroller's annual and special reports, and from the government's responses to
them, for tracking an audit issue over time: when it was reported, what was found, who was audited, what they
answered, and whether later follow-up audits found it fixed.

| Pipeline | Module | Writes |
|---|---|---|
| `reports` (weekly) | `mevaker_scraper.py` | `state_comptroller_publications`, `state_comptroller_chapters`: every annual/special report and its chapters, all years, with the audited bodies and links to the files. After the first full scrape, a run re-scrapes only the last 3 years' publications and any new ones, and keeps the rest from the previous run (`STATE_COMPTROLLER_FULL=1` scrapes everything again) |
| `chapters` | `chapter_analysis.py` | `state_comptroller_audits` (a chapter with its summary, audit period, follow-up reference, topics, key figures and full markdown text), `state_comptroller_findings` (one row per finding) - from 2000 on |
| `pm_responses` (weekly) | `pm_responses.py` | `state_comptroller_response_docs`, `state_comptroller_responses`: the PM's comments books and the follow-up (מעקב) books from gov.il, one row per finding answered |
| `issues` | `issue_linking.py` | `state_comptroller_links` (every link made, with its method), `state_comptroller_issues` (findings and responses grouped into issues, each with a timeline), `state_comptroller_issue_members` |

Sources:
- library.mevaker.gov.il, a SharePoint site. Publications are listed with its anonymous search (CSOM).
  Chapters and audited bodies come from its JSON service, and file links from the server-rendered pages.
- gov.il publications of אגף המפקח הכללי לענייני ביקורת המדינה. These come from the PublicationApi list and the
  openapi content-pages endpoint for files.
  gov.il blocks non-Israeli and non-whitelisted IPs. Prod is whitelisted. For local runs, set `GOVIL_PROXY`, or
  pass `DOCS_JSON` to `pm_responses.py` to work offline on stored files.

All files are backed up to `budgetkey-files` under `state-comptroller/reports/<publication id>/` and
`state-comptroller/pm-responses/<UrlName>/`. Derived results are cached next to each source file:
- `.md` and `.text.json`: markdown, keyed on `chapter_text.TEXT_VERSION`
- `.analysis.json`: Gemini results, keyed on `ANALYZER_VERSION` and the text or file hash

Bump the relevant version to recompute.

The documents' formatting (headings, section splits, tables, page layouts) is inconsistent, even within one file.
So nothing relies on it:
- Chapters are read whole, or in overlapping windows if very long.
- Response books are read in overlapping page windows, with context carried over and items stitched across windows.
- Most response books up to 2018 are published both as a PDF and as a Word copy; only the Word copy is read (the PDF
  only if it can't be). Some books are also published again under another gov.il page: a file most of whose text is
  in another file (or, if its text can't be read, a file of another book with the same title) isn't read again. So
  no response is counted twice.
- Pages whose text layer can't be used are found by their characters, not by assumptions about fonts: legacy-encoded
  letters, symbols of other scripts (Latin-1, Hebrew shifted into the IPA block), missing final letters, or no text
  at all (scans). In chapters they are transcribed by Gemini; in response books Gemini reads the page image anyway,
  and items found only on such pages are marked as not checkable (`verified` is null) instead of failed.
- Gemini blocks some PDF pages outright (block_reason OTHER) while reading the same page as an image, and now and
  then returns nothing: windows that fail are retried, split, and read page by page, and single pages as PNGs.
- Chapters in English or Arabic (translations in recent reports) are listed but not analyzed (`not-hebrew`).
- Follow-up audits: `follow_up` tells a follow-up audit (ממצאי מעקב) and a new audit that also follows up earlier
  findings from one that only cites earlier reports; `earlier_reports` lists every earlier report a chapter refers
  to, with the relation.
- Every extracted finding or response is checked against the source text (`verified`; response books are checked
  again on every run, cached results included), and files get a coverage status rather than being trusted blindly.

Local runs: each module has a `__main__` block that takes `LIMIT`, `OUT` and selection env vars
(`PUB_IDS`, `CHAPTER_IDS`, `NAMES`), plus `REPORTS`/`CHAPTERS`/`RESPONSES` for input datapackages.
`STATE_COMPTROLLER_MAX_WINDOWS` limits each response book to its first N windows, for trying prompts.
