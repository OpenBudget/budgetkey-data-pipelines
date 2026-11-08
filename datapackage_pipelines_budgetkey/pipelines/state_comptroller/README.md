# State Comptroller reports (דוחות מבקר המדינה)

Structured data from the State Comptroller's annual and special reports, and from the government's responses to
them, for tracking an audit issue over time: when it was reported, what was found, who was audited, what they
answered, and whether later follow-up audits found it fixed.

| Pipeline | Module | Writes |
|---|---|---|
| `reports` (weekly) | `mevaker_scraper.py` | `state_comptroller_publications`, `state_comptroller_chapters`: every annual/special report and its chapters, all years, with the audited bodies and links to the files |
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
  only if it can't be), so no response is counted twice.
- Pages set in old fonts that pdftotext can't read (legacy-encoded letters, missing final letters) are transcribed
  by Gemini from the PDF.
- Every extracted finding or response is checked against the source text (`verified`; response books are checked
  again on every run, cached results included), and files get a coverage status rather than being trusted blindly.

Local runs: each module has a `__main__` block that takes `LIMIT`, `OUT` and selection env vars
(`PUB_IDS`, `CHAPTER_IDS`, `NAMES`), plus `REPORTS`/`CHAPTERS`/`RESPONSES` for input datapackages.
`STATE_COMPTROLLER_MAX_WINDOWS` limits each response book to its first N windows, for trying prompts.
