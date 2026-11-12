"""State Comptroller audits and issues as Elasticsearch documents.

Builds the two resources the indexer loads (`budgetkey/elasticsearch/budgetkey.source-spec.yaml`), each named after
its ES type:
- state_comptroller_audits: one document per analyzed Hebrew chapter, with its findings, the bodies audited, the
  follow-up information, links to the files and its full text.
- state_comptroller_issues: issues found in more than one source (several audits, or an audit and the government's
  responses), each with its timeline. An issue of a single finding is already a finding of its audit's document.

Links to other budgetkey records (related_items, mentioned_in) are added by the indexer's add_related_items step.
"""
import collections
import datetime
import logging
import os

import dataflows as DF

import sc_common as sc  # also raises the csv field size limit, for the chapters' full texts

CHAPTERS_PATH = os.environ.get('CHAPTERS', '/var/datapackages/state_comptroller/chapters')
ISSUES_PATH = os.environ.get('ISSUES', '/var/datapackages/state_comptroller/issues')
TAXONOMY_PATH = os.environ.get('TAXONOMY', '/var/datapackages/state_comptroller/taxonomy')
OUT_PATH = '/var/datapackages/state_comptroller/documents'

AUDITS = 'state_comptroller_audits'
ISSUES = 'state_comptroller_issues'

NOT_INDEXED = {'es:index': False}
OBJECTS = {'es:itemType': 'object', 'es:index': False}
STRINGS = {'es:itemType': 'string'}
# The search API sorts an empty query by each type's score column and multiplies a term's relevance by its square
# root, so every searchable type needs one. Other types: analysis pages 10, decisions 1, budget items their amount.
SCORE = {'es:score-column': True}

FINDING_SCHEMA = {'fields': [
    {'name': n, 'type': t} for n, t in (
        ('finding_id', 'string'), ('seq', 'integer'), ('title', 'string'), ('description', 'string'),
        ('recommendation', 'string'), ('responsible_bodies', 'array'), ('finding_type', 'string'),
        ('severity', 'string'), ('followup_status', 'string'), ('followup_status_text', 'string'),
        ('section', 'string'), ('body_responses', 'array'), ('verified', 'boolean'), ('issue_id', 'string'),
        ('issue_size', 'integer'))
]}

# The audit document's fields: (name, type, ES hints); existing fields keep their type when None
AUDIT_FIELDS = [
    ('chapter_id', 'integer', {}),
    ('title', 'string', {'es:title': True}),
    ('publication_title', 'string', {'es:boost': True}),
    ('publication_type', 'string', {'es:keyword': True}),
    ('report_code', 'string', {'es:keyword': True}),
    ('publish_date', 'date', {}),
    ('year', 'integer', {}),
    ('page_url', 'string', NOT_INDEXED),
    ('main_body', 'string', {'es:boost': True}),
    ('report_group', 'string', {'es:keyword': True}),
    ('audited_bodies', 'array', dict(STRINGS, **{'es:boost': True})),
    ('audited_body_codes', 'array', dict(STRINGS, **{'es:keyword': True})),
    ('audit_subject', 'string', {'es:boost': True}),
    ('summary', 'string', {'es:hebrew': True}),
    ('audit_period', 'string', NOT_INDEXED),
    ('audit_period_start_year', 'integer', {}),
    ('audit_period_end_year', 'integer', {}),
    ('topics', 'array', dict(STRINGS, **{'es:boost': True})),
    # The closed taxonomy (taxonomy.py): keys the search page filters on
    ('domain', 'string', {'es:keyword': True}),
    ('domains', 'array', dict(STRINGS, **{'es:keyword': True})),
    ('lenses', 'array', dict(STRINGS, **{'es:keyword': True})),
    ('event', 'string', {'es:keyword': True}),
    ('key_figures', 'array', OBJECTS),
    ('follow_up', 'string', {'es:keyword': True}),
    ('follows_up_report', 'string', {}),
    ('follows_up_year', 'integer', {}),
    ('follows_up_title', 'string', {}),
    ('earlier_reports', 'array', OBJECTS),
    ('findings', 'array', dict(OBJECTS, **{'es:schema': FINDING_SCHEMA})),
    ('n_findings', 'integer', {}),
    ('findings_text', 'string', {'es:hebrew': True}),
    ('issue_ids', 'array', dict(STRINGS, **{'es:keyword': True})),
    ('tracked_issues', 'integer', {}),
    ('text', 'string', {'es:hebrew': True}),
    ('text_truncated', 'boolean', {}),
    ('pdf_url', 'string', NOT_INDEXED),
    ('docx_url', 'string', NOT_INDEXED),
    ('abstract_pdf_url', 'string', NOT_INDEXED),
    ('abstract_docx_url', 'string', NOT_INDEXED),
    ('md_url', 'string', NOT_INDEXED),
    ('source_url', 'string', NOT_INDEXED),
    ('analysis_status', 'string', {'es:keyword': True}),
    ('score', 'number', SCORE),
]

TIMELINE_SCHEMA = {'fields': [
    {'name': n, 'type': t} for n, t in (
        ('date', 'string'), ('kind', 'string'), ('id', 'string'), ('chapter_id', 'integer'), ('source', 'string'),
        ('url', 'string'), ('title', 'string'), ('status', 'string'), ('text', 'string'),
        ('finding_text', 'string'), ('audit_period', 'string'), ('date_text', 'string'), ('body', 'string'))
]}

ISSUE_FIELDS = [
    ('issue_id', 'string', {'es:keyword': True}),
    ('title', 'string', {'es:title': True}),
    ('description', 'string', {'es:hebrew': True}),
    ('bodies', 'array', dict(STRINGS, **{'es:boost': True})),
    ('severity', 'string', {'es:keyword': True}),
    ('first_reported', 'date', {}),
    ('last_seen', 'date', {}),
    ('n_audits', 'integer', {}),
    ('n_responses', 'integer', {}),
    ('latest_status', 'string', {'es:keyword': True}),
    ('chapter_ids', 'array', {'es:itemType': 'integer'}),
    ('chapter_titles', 'array', dict(STRINGS, **{'es:boost': True})),
    ('domains', 'array', dict(STRINGS, **{'es:keyword': True})),
    ('lenses', 'array', dict(STRINGS, **{'es:keyword': True})),
    ('timeline', 'array', dict(OBJECTS, **{'es:schema': TIMELINE_SCHEMA})),
    ('timeline_text', 'string', {'es:hebrew': True}),
    ('score', 'number', SCORE),
]

INDEXED_STATUSES = ('ok', 'low-verification', 'partial-text', 'no-findings')
# The indexer's load_big reads fields of up to 2M characters; a chapter whose file was a whole book can be longer.
# The full text stays at md_url.
MAX_TEXT = 1_000_000


def read_resource(path, name):
    return DF.Flow(DF.load(os.path.join(path, 'datapackage.json'), resources=[name])).results()[0][0]


def issue_index(issues_path):
    """{finding id: (issue id, number of members)}, from the issues' members."""
    try:
        members = read_resource(issues_path, 'issue_members')
    except Exception:
        logging.warning('No issues at %s, audits are indexed without them', issues_path)
        return {}
    sizes = collections.Counter(m['issue_id'] for m in members)
    return {m['member_id']: (m['issue_id'], sizes[m['issue_id']]) for m in members if m['member_type'] == 'finding'}


def taxonomy_index(taxonomy_path):
    """{chapter id: its taxonomy row} (taxonomy.py's audit_taxonomy)."""
    try:
        rows = read_resource(taxonomy_path, 'audit_taxonomy')
    except Exception:
        logging.warning('No taxonomy at %s, documents are indexed without domains and lenses', taxonomy_path)
        return {}
    return {r['chapter_id']: r for r in rows}


def body_names(taxonomy_path):
    """{body name: its canonical name} (taxonomy.py's body_names): spelling variants and old names of renamed bodies."""
    try:
        rows = read_resource(taxonomy_path, 'body_names')
    except Exception:
        logging.warning('No body names at %s, bodies are indexed as written', taxonomy_path)
        return {}
    return {r['name']: r['canonical'] for r in rows if r.get('canonical') and r['canonical'] != r['name']}


def audit_tags(chapter_id, taxonomy):
    t = taxonomy.get(chapter_id) or {}
    return dict(domain=t.get('domain'), domains=t.get('domains') or [], lenses=t.get('lenses') or [],
                event=t.get('event'))


def issue_tags(timeline, taxonomy):
    """The union of the domains and lenses of the audits an issue's findings come from."""
    tags = [taxonomy[e['chapter_id']] for e in timeline if e.get('chapter_id') in taxonomy]
    return dict(domains=list(dict.fromkeys(k for t in tags for k in t.get('domains') or [])),
                lenses=list(dict.fromkeys(k for t in tags for k in t.get('lenses') or [])))


def audit_document(row, findings, issues, bodies=None):
    own = sorted(findings.get(row['chapter_id'], []), key=lambda f: f.get('seq') or 0)
    doc_findings = []
    for f in own:
        issue_id, size = issues.get(f['finding_id'], (None, None))
        doc_findings.append(dict(
            finding_id=f['finding_id'], seq=f.get('seq'), title=f.get('title'), description=f.get('description'),
            recommendation=f.get('recommendation'),
            responsible_bodies=sc.canonical_bodies(f.get('responsible_bodies'), bodies or {}),
            finding_type=f.get('finding_type'), severity=f.get('severity'),
            followup_status=f.get('followup_status'), followup_status_text=f.get('followup_status_text'),
            section=f.get('section'), body_responses=body_responses(f.get('body_responses')),
            verified=f.get('verified'),
            issue_id=issue_id, issue_size=size,
        ))
    bodies = row.get('audited_bodies') or []
    names = [b.get('name') for b in bodies if isinstance(b, dict) and b.get('name')]
    names += [b.get('name') for b in row.get('llm_audited_bodies') or [] if isinstance(b, dict) and b.get('name')]
    main_body, report_group = lead_body(row)
    tracked = sorted({f['issue_id'] for f in doc_findings if f['issue_id'] and (f['issue_size'] or 0) > 1})
    return dict(
        chapter_id=row['chapter_id'],
        title=row.get('title'),
        publication_title=row.get('publication_title'),
        publication_type=row.get('publication_type'),
        report_code=row.get('report_code'),
        publish_date=row.get('publish_date'),
        year=row['publish_date'].year if row.get('publish_date') else None,
        page_url=row.get('page_url'),
        main_body=main_body,
        report_group=report_group,
        audited_bodies=list(dict.fromkeys(([main_body] if main_body else []) + names)),
        audited_body_codes=[str(b.get('code')) for b in bodies if isinstance(b, dict) and b.get('code')],
        audit_subject=row.get('audit_subject'),
        summary=row.get('summary'),
        audit_period=row.get('audit_period'),
        audit_period_start_year=row.get('audit_period_start_year'),
        audit_period_end_year=row.get('audit_period_end_year'),
        topics=row.get('topics') or [],
        key_figures=row.get('key_figures') or [],
        follow_up=row.get('follow_up'),
        follows_up_report=row.get('follows_up_report'),
        follows_up_year=row.get('follows_up_year'),
        follows_up_title=row.get('follows_up_title'),
        earlier_reports=row.get('earlier_reports') or [],
        findings=doc_findings,
        n_findings=len(doc_findings),
        findings_text='\n\n'.join('%s\n%s' % (f['title'] or '', f['description'] or '') for f in doc_findings),
        issue_ids=sorted({f['issue_id'] for f in doc_findings if f['issue_id']}),
        tracked_issues=len(tracked),
        text=(row.get('text_md') or '')[:MAX_TEXT] or None,
        text_truncated=len(row.get('text_md') or '') > MAX_TEXT,
        pdf_url=row.get('full_pdf_stored'),
        docx_url=row.get('full_docx_stored'),
        abstract_pdf_url=row.get('abstract_pdf_stored'),
        abstract_docx_url=row.get('abstract_docx_stored'),
        md_url=row.get('md_url'),
        source_url=row.get('full_pdf_url') or row.get('full_docx_url'),
        analysis_status=row.get('analysis_status'),
        score=audit_score(row.get('publish_date')),
    )


def body_responses(responses):
    """The bodies' responses quoted in a finding, with `date` normalized (YYYY-MM-DD, YYYY-MM, YYYY or None) and the
    report's own wording kept in `date_text`."""
    return [dict(r, date=sc.normalize_date(r.get('date')), date_text=r.get('date')) if isinstance(r, dict) else r
            for r in responses or []]


_body_key = sc.body_key


def lead_body(row):
    """(main body, report group). The scraper's main_body is the comptroller site's main institution, but when the site
    names none it is the chapter's group in the report's table of contents ("דוח ביקורת מיוחד", "מטלות רוחב", "רשויות
    מקומיות"), which is not a body. That's the case exactly when it isn't one of the site's audited bodies: it then
    becomes report_group, and the main body is the one the chapter analysis marked main (in the site's spelling when
    the site lists it), or none: a cross-cutting audit may have no lead body."""
    site = [b.get('name') for b in row.get('audited_bodies') or [] if isinstance(b, dict) and b.get('name')]
    given = row.get('main_body')
    if not given or given in site:
        return given or None, None
    by_key = {_body_key(n): n for n in site}
    for b in row.get('llm_audited_bodies') or []:
        if isinstance(b, dict) and b.get('role') == 'main' and b.get('name'):
            return by_key.get(_body_key(b['name']), b['name']), given
    return None, given


def audit_score(publish_date):
    """10 for an audit from 1989 (the first analysed), rising evenly to 20 for one from 2026: newest first on an
    empty query, and level with the analysis pages (10) in the all-types search."""
    if not publish_date:
        return 10.0
    years = (publish_date - datetime.date(1989, 1, 1)).days / 365.25
    return round(min(20.0, max(10.0, 10 + years * 10 / 37)), 3)


def issue_score(n_audits, n_responses):
    """1 + the number of sources that track the issue, at most 10 (an issue has 2 or more, so 3-10): never above
    an audit."""
    return float(1 + min(9, (n_audits or 0) + (n_responses or 0)))


def indexed_audit(row):
    return (row.get('language') == 'he' and row.get('analysis_status') in INDEXED_STATUSES
            and bool(row.get('text_md')))


def issue_document(row, bodies=None):
    timeline = row.get('timeline') or []
    chapters = {}
    for e in timeline:
        if e.get('chapter_id') and e['chapter_id'] not in chapters:
            chapters[e['chapter_id']] = e.get('source')
    return dict(
        issue_id=row['issue_id'], title=row.get('title'), description=row.get('description'),
        bodies=sc.canonical_bodies(row.get('bodies'), bodies or {}), severity=row.get('severity'),
        first_reported=row.get('first_reported'),
        last_seen=row.get('last_seen'), n_audits=row.get('n_audits'), n_responses=row.get('n_responses'),
        latest_status=row.get('latest_status'), chapter_ids=sorted(chapters),
        chapter_titles=[chapters[c] for c in sorted(chapters) if chapters[c]], timeline=timeline,
        timeline_text='\n\n'.join('%s %s: %s' % (e.get('date') or '', e.get('title') or '', e.get('text') or '')
                                  for e in timeline),
        score=issue_score(row.get('n_audits'), row.get('n_responses')),
    )


def tracked_issue(row):
    """More than one finding or response. The bodies' responses quoted in the report don't count: a single finding
    with them is still just a finding of its audit's document."""
    return sum(1 for e in row.get('timeline') or [] if e.get('kind') != 'body_response') > 1


def ensure_fields(resource, fields):
    """Adds the fields a resource doesn't have (their values are set by the row step that follows)."""
    def step(package):
        for res in package.pkg.descriptor['resources']:
            if res['name'] == resource:
                names = {f['name'] for f in res['schema']['fields']}
                res['schema']['fields'] += [dict(name=n, type=t) for n, t, _ in fields if n not in names]
        yield package.pkg
        yield from package
    return step


def typed(resource, fields):
    """Steps that give a resource exactly these fields, with their types and ES hints."""
    steps = [DF.select_fields([n for n, _, _ in fields], resources=[resource])]
    for name, type_, hints in fields:
        steps.append(DF.set_type(name, resources=[resource], type=type_, **hints))
    return steps


def audits_flow(chapters_path, issues, taxonomy, bodies=None):
    findings = collections.defaultdict(list)
    for f in read_resource(chapters_path, 'findings'):
        findings[f['chapter_id']].append(f)
    logging.info('%d findings in %d chapters', sum(len(v) for v in findings.values()), len(findings))
    return DF.Flow(
        DF.load(os.path.join(chapters_path, 'datapackage.json'), resources=['audits']),
        DF.filter_rows(indexed_audit, resources=['audits']),
        ensure_fields('audits', AUDIT_FIELDS),
        lambda row: row.update(audit_document(row, findings, issues, bodies), **audit_tags(row['chapter_id'], taxonomy))
        if 'chapter_id' in row else None,
        DF.update_resource('audits', name=AUDITS, path=AUDITS + '.csv'),
        *typed(AUDITS, AUDIT_FIELDS),
    )


def issues_flow(issues_path, taxonomy, bodies=None):
    return DF.Flow(
        DF.load(os.path.join(issues_path, 'datapackage.json'), resources=['issues']),
        DF.filter_rows(tracked_issue, resources=['issues']),
        ensure_fields('issues', ISSUE_FIELDS),
        lambda row: row.update(issue_document(row, bodies), **issue_tags(row.get('timeline') or [], taxonomy))
        if 'issue_id' in row else None,
        DF.update_resource('issues', name=ISSUES, path=ISSUES + '.csv'),
        *typed(ISSUES, ISSUE_FIELDS),
    )


def documents_flow(chapters_path=CHAPTERS_PATH, issues_path=ISSUES_PATH, taxonomy_path=TAXONOMY_PATH):
    issues = issue_index(issues_path)
    taxonomy = taxonomy_index(taxonomy_path)
    bodies = body_names(taxonomy_path)
    steps = [audits_flow(chapters_path, issues, taxonomy, bodies)]
    if issues:
        steps.append(issues_flow(issues_path, taxonomy, bodies))
    return DF.Flow(*steps)


def flow(*_):
    return DF.Flow(
        documents_flow(),
        DF.update_resource(None, **{'dpp:streaming': True}),
        DF.dump_to_path(OUT_PATH),
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    DF.Flow(documents_flow(), DF.printer(num_rows=1, fields=['chapter_id', 'issue_id', 'title', 'n_findings']),
            DF.dump_to_path(os.environ.get('OUT', 'state_comptroller_documents_out'))).process()
