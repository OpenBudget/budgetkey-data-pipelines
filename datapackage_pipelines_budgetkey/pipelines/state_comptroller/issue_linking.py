"""Links findings, follow-up audits and government responses into issues that can be tracked over time.

Links are made in two levels, each first narrowed with embeddings (OpenAI, cached) and then decided by Gemini:
1. Chapter level:
   - each section of a government response book (the items under one comptroller chapter title) -> the report
     chapter it answers;
   - each follow-up audit -> the earlier chapter it follows up on.
2. Item level, within each linked pair:
   - response items -> the findings they answer;
   - follow-up findings -> the original findings.
Connected findings and responses form an issue (union-find); its timeline lists every audit and response.
Every link is kept in state_comptroller_links with its method and score, so it can be audited.
"""
import datetime
import difflib
import json
import logging
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import dataflows as DF
import numpy as np

import sc_common as sc

from datapackage_pipelines_budgetkey.common import llm
from datapackage_pipelines_budgetkey.common.cached_openai import embed_many

REPORTS_PATH = '/var/datapackages/state_comptroller/reports'
CHAPTERS_PATH = '/var/datapackages/state_comptroller/chapters'
RESPONSES_PATH = '/var/datapackages/state_comptroller/pm_responses'
OUT_PATH = '/var/datapackages/state_comptroller/issues'
THREADS = int(os.environ.get('STATE_COMPTROLLER_THREADS') or 6)
CANDIDATES = 8
PRESELECT = 30
SECTIONS_PER_CALL = 15
# A response book answers reports published up to a few years before it (follow-up books) or just after it
# (the PM's comments are sometimes dated a few days before the report itself).
WINDOW_BEFORE = datetime.timedelta(days=365 * 6)
WINDOW_AFTER = datetime.timedelta(days=120)

USAGE = llm.Usage()


def load(path, resource):
    return DF.Flow(DF.load(os.path.join(path, 'datapackage.json'), resources=[resource])).results()[0][0]


def as_date(d):
    if isinstance(d, datetime.date):
        return d
    try:
        return datetime.date.fromisoformat(str(d)[:10])
    except ValueError:
        return None


def unit_rows(vectors):
    m = np.array(vectors, dtype=np.float32)
    norms = np.linalg.norm(m, axis=1, keepdims=True)
    norms[norms == 0] = 1
    return m / norms


def top_candidates(similarities, allowed, k=PRESELECT):
    """Indices of the k most similar allowed chapters, most similar first."""
    sims = np.where(allowed, similarities, -np.inf)
    k = min(k, int(allowed.sum()))
    if k <= 0:
        return []
    idx = np.argpartition(-sims, k - 1)[:k]
    return [int(i) for i in idx[np.argsort(-sims[idx])]]


def ratio(a, b):
    return difflib.SequenceMatcher(None, sc.normalize_for_match(a), sc.normalize_for_match(b)).ratio()


def chapter_label(c):
    return '%s | %s' % (c.get('title') or '', c.get('main_body') or '')


# --- response sections -> chapters ---------------------------------------------------------------------------

def response_sections(responses):
    """Groups the response items of each file by the comptroller chapter they're listed under."""
    sections = {}
    for r in responses:
        title = (r.get('chapter_title') or '').strip()
        if not title:
            continue
        key = (r['url_name'], r['file_name'], sc.normalize_for_match(title))
        s = sections.setdefault(key, dict(url_name=r['url_name'], file_name=r['file_name'], title=title,
                                          ministry=r.get('ministry'), publish_date=as_date(r.get('publish_date')),
                                          report_code=r.get('report_code'), doc_title=r.get('doc_title'),
                                          items=[]))
        s['items'].append(r)
        s['ministry'] = s['ministry'] or r.get('ministry')
    return list(sections.values())


SECTION_SCHEMA = {
    'type': 'object',
    'properties': {'matches': {'type': 'array', 'items': {
        'type': 'object',
        'properties': {
            'section': {'type': 'integer'},
            'candidate': {'type': 'integer'},
            'confidence': {'type': 'string', 'enum': ['high', 'medium', 'low']},
        },
        'required': ['section', 'candidate', 'confidence'],
    }}},
    'required': ['matches'],
}

SECTION_PROMPT = '''The book "{doc_title}" (published {date}) holds the Israeli government's responses to reports
of the State Comptroller. Each section below is a heading of the book naming the report chapter (audit) it
answers, with the ministry it is listed under and the first items it answers. For each section, choose the report
chapter it answers from its candidates (published reports of the comptroller). Answer candidate 0 if none of them
is that chapter. Titles in the book may be abbreviated or reworded.

{sections}'''


def match_sections(sections, chapters, chapter_vectors, publications):
    """Links each response section to a report chapter. Returns {section index: (chapter, score, method)}."""
    if not sections:
        return {}
    vectors = unit_rows(embed_many(['%s | %s' % (s['title'], s.get('ministry') or '') for s in sections]))
    similarities = vectors @ chapter_vectors.T
    dates = np.array([c['_date'].toordinal() if c['_date'] else 0 for c in chapters])
    pub_codes = {p['pub_id']: p.get('report_code') for p in publications}
    matches, ask = {}, defaultdict(list)
    for i, s in enumerate(sections):
        date = s['publish_date']
        if not date:
            continue
        allowed = (dates >= (date - WINDOW_BEFORE).toordinal()) & (dates <= (date + WINDOW_AFTER).toordinal())
        codes = set(sc.report_codes(s.get('doc_title') or '', s.get('report_code') or ''))
        codes |= {r['item_report_code'] for r in s['items'] if r.get('item_report_code')}
        scored = []
        for j in top_candidates(similarities[i], allowed):
            c = chapters[j]
            score = float(similarities[i, j]) + 0.25 * ratio(s['title'], c['title'])
            if codes and pub_codes.get(c['pub_id']) in codes:
                score += 0.1
            scored.append((score, j))
        scored.sort(reverse=True)
        top = scored[:CANDIDATES]
        if not top:
            continue
        best_score, best = top[0]
        if ratio(s['title'], chapters[best]['title']) > 0.92 and (len(top) == 1 or best_score - top[1][0] > 0.05):
            matches[i] = (chapters[best], round(best_score, 3), 'title')
        else:
            ask[(s['url_name'], s['file_name'])].append((i, top))

    def decide(batch):
        lines = []
        for n, (i, top) in enumerate(batch, 1):
            s = sections[i]
            items = '; '.join((r.get('title') or r.get('finding_text') or '')[:80] for r in s['items'][:3])
            lines.append('Section %d: "%s" (ministry: %s; first items: %s)' % (n, s['title'], s.get('ministry') or '-', items))
            for k, (_, j) in enumerate(top, 1):
                c = chapters[j]
                lines.append('  candidate %d: "%s" - %s, in "%s" (%s)' % (
                    k, c['title'], c.get('main_body') or '', c.get('publication_title') or '', c['_date']))
        s0 = sections[batch[0][0]]
        prompt = SECTION_PROMPT.format(doc_title=s0['doc_title'], date=s0['publish_date'], sections='\n'.join(lines))
        try:
            _, out = llm.complete(prompt, schema=SECTION_SCHEMA, model=llm.MODEL_FAST, temperature=0, usage=USAGE,
                                  thinking='low')
        except Exception as e:
            logging.warning('Section matching failed for %s: %s', s0['doc_title'], e)
            return []
        result = []
        for m in out.get('matches') or []:
            n, k = m.get('section'), m.get('candidate')
            if not (1 <= (n or 0) <= len(batch)) or not (1 <= (k or 0) <= len(batch[n - 1][1])):
                continue
            if m.get('confidence') == 'low':
                continue
            i, top = batch[n - 1]
            score, j = top[k - 1]
            result.append((i, chapters[j], round(score, 3), 'llm-' + m.get('confidence', 'medium')))
        return result

    batches = []
    for pending in ask.values():
        for b in range(0, len(pending), SECTIONS_PER_CALL):
            batches.append(pending[b:b + SECTIONS_PER_CALL])
    with ThreadPoolExecutor(THREADS) as executor:
        for result in executor.map(decide, batches):
            for i, chapter, score, method in result:
                matches[i] = (chapter, score, method)
    return matches


# --- items -> findings ---------------------------------------------------------------------------------------

MAP_SCHEMA = {
    'type': 'object',
    'properties': {'links': {'type': 'array', 'items': {
        'type': 'object',
        'properties': {'item': {'type': 'integer'}, 'findings': {'type': 'array', 'items': {'type': 'integer'}}},
        'required': ['item', 'findings'],
    }}},
    'required': ['links'],
}

RESPONSE_MAP_PROMPT = '''Below are the findings of the Israeli State Comptroller's audit "{chapter}" (published
{chapter_date}), numbered F1..F{nf}, and the items of the government's response to it, from "{doc}" ({doc_date}),
numbered 1..{ni}. For each response item, list the numbers of the findings it addresses (usually one; none if it
addresses none of them). Match on substance, not wording.

FINDINGS:
{findings}

RESPONSE ITEMS:
{items}'''

FOLLOWUP_MAP_PROMPT = '''An Israeli State Comptroller audit, "{original}" (published {original_date}), was later
followed up by the audit "{followup}" (published {followup_date}). Below are the findings of the original audit,
numbered F1..F{nf}, and the findings of the follow-up audit, numbered 1..{ni}. For each follow-up finding, list the
numbers of the original findings it follows up on (usually one; none if it is a new finding). Match on substance.

ORIGINAL FINDINGS:
{findings}

FOLLOW-UP FINDINGS:
{items}'''


def finding_line(n, f):
    return 'F%d. %s: %s' % (n, f.get('title') or '', (f.get('description') or '')[:400])


def map_items(prompt, n_items, n_findings):
    try:
        _, out = llm.complete(prompt, schema=MAP_SCHEMA, model=llm.MODEL_FAST, temperature=0, usage=USAGE,
                              thinking='low')
    except Exception as e:
        logging.warning('Item mapping failed: %s', e)
        return None
    pairs = []
    for link in out.get('links') or []:
        i = link.get('item')
        if not 1 <= (i or 0) <= n_items:
            continue
        for f in link.get('findings') or []:
            if 1 <= f <= n_findings:
                pairs.append((i - 1, f - 1))
    return pairs


def map_response_section(section, chapter, findings):
    items = section['items']
    prompt = RESPONSE_MAP_PROMPT.format(
        chapter=chapter['title'], chapter_date=chapter['_date'], nf=len(findings), doc=section['doc_title'],
        doc_date=section['publish_date'], ni=len(items),
        findings='\n'.join(finding_line(n, f) for n, f in enumerate(findings, 1)),
        items='\n'.join('%d. %s: %s' % (n, r.get('title') or '', (r.get('finding_text') or '')[:400])
                        for n, r in enumerate(items, 1)))
    return map_items(prompt, len(items), len(findings))


def map_followup(original, original_findings, followup, followup_findings):
    prompt = FOLLOWUP_MAP_PROMPT.format(
        original=original['title'], original_date=original['_date'], followup=followup['title'],
        followup_date=followup['_date'], nf=len(original_findings), ni=len(followup_findings),
        findings='\n'.join(finding_line(n, f) for n, f in enumerate(original_findings, 1)),
        items='\n'.join('%d. %s: %s' % (n, f.get('title') or '', (f.get('description') or '')[:400])
                        for n, f in enumerate(followup_findings, 1)))
    return map_items(prompt, len(followup_findings), len(original_findings))


# --- follow-up audits -> original audits ----------------------------------------------------------------------

FOLLOWUP_SCHEMA = {
    'type': 'object',
    'properties': {
        'candidate': {'type': 'integer'},
        'confidence': {'type': 'string', 'enum': ['high', 'medium', 'low']},
    },
    'required': ['candidate', 'confidence'],
}

FOLLOWUP_PROMPT = '''The Israeli State Comptroller audit "{title}" (published {date}, audited bodies: {bodies}) is a
follow-up audit. It says it follows up on: report "{report}", year {year}, chapter "{prev_title}" (as extracted
automatically; may be partial). Its summary: {summary}

Which of these earlier report chapters is the audit it follows up on? Answer candidate 0 if none of them is.
{candidates}'''


def match_followups(audits, chapters, chapter_vectors):
    """Links each follow-up audit to the earlier chapter it follows up on. Returns {chapter_id: (chapter, score, method)}."""
    followups = [a for a in audits if a.get('is_follow_up')]
    texts = ['%s | %s | %s' % (a.get('follows_up_title') or a['title'], a['title'], a.get('main_body') or '')
             for a in followups]
    if not followups:
        return {}
    similarities = unit_rows(embed_many(texts)) @ chapter_vectors.T
    dates = np.array([c['_date'].toordinal() if c['_date'] else 0 for c in chapters])

    def one(args):
        a, row = args
        date = as_date(a.get('publish_date'))
        if not date:
            return a, None
        allowed = (dates > 0) & (dates < date.toordinal())
        scored = []
        for j in top_candidates(row, allowed):
            c = chapters[j]
            if c['chapter_id'] == a['chapter_id']:
                continue
            score = float(row[j]) + 0.25 * ratio(a.get('follows_up_title') or a['title'], c['title'])
            year = a.get('follows_up_year')
            if year and abs(c['_date'].year - year) <= 1:
                score += 0.1
            scored.append((score, j))
        scored.sort(reverse=True)
        top = scored[:CANDIDATES]
        if not top:
            return a, None
        lines = ['candidate %d: "%s" - %s, in "%s" (%s)' % (k, chapters[j]['title'], chapters[j].get('main_body') or '',
                                                            chapters[j].get('publication_title') or '', chapters[j]['_date'])
                 for k, (_, j) in enumerate(top, 1)]
        prompt = FOLLOWUP_PROMPT.format(
            title=a['title'], date=a.get('publish_date'), bodies=a.get('main_body') or '', report=a.get('follows_up_report') or '',
            year=a.get('follows_up_year') or '', prev_title=a.get('follows_up_title') or '', summary=(a.get('summary') or '')[:800],
            candidates='\n'.join(lines))
        try:
            _, out = llm.complete(prompt, schema=FOLLOWUP_SCHEMA, model=llm.MODEL_FAST, temperature=0, usage=USAGE,
                                  thinking='low')
        except Exception as e:
            logging.warning('Follow-up matching failed for %s: %s', a['chapter_id'], e)
            return a, None
        k = out.get('candidate') or 0
        if not 1 <= k <= len(top) or out.get('confidence') == 'low':
            return a, None
        score, j = top[k - 1]
        return a, (chapters[j], round(score, 3), 'llm-' + out.get('confidence', 'medium'))

    matches = {}
    with ThreadPoolExecutor(THREADS) as executor:
        for a, m in executor.map(one, list(zip(followups, similarities))):
            if m:
                matches[a['chapter_id']] = m
    return matches


# --- issues ----------------------------------------------------------------------------------------------------

class UnionFind:
    def __init__(self):
        self.parent = {}

    def find(self, x):
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[max(ra, rb)] = min(ra, rb)


class IssueGroups(UnionFind):
    """Union-find in which a group holds at most one finding per audit: a link that would put two findings of the
    same chapter in one issue is refused. Without it, one response row or follow-up finding mapped to several findings
    of an audit glues them together, and chains of those build issues of dozens of findings across many audits."""

    def __init__(self):
        super().__init__()
        self.chapters = {}

    def add(self, node, chapter_id=None):
        root = self.find(node)
        self.chapters.setdefault(root, set())
        if chapter_id is not None:
            self.chapters[root].add(chapter_id)

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return True
        ca, cb = self.chapters.get(ra, set()), self.chapters.get(rb, set())
        if ca & cb:
            return False
        super().union(a, b)
        root = self.find(a)
        self.chapters[root] = ca | cb
        self.chapters.pop(rb if root == ra else ra, None)
        return True


STATUS_ORDER = {'rectified': 0, 'mostly_rectified': 1, 'partially_rectified': 2, 'in_progress': 3,
                'not_rectified': 4, 'disputed': 4}


def build_issues(findings, responses, links):
    """Groups linked findings and response items into issues, at most one finding per audit (see IssueGroups); the
    follow-up links, which say a finding was audited again, are applied before the response links. Only components
    with a finding are issues."""
    uf = IssueGroups()
    nodes = {}
    for f in findings:
        key = ('finding', f['finding_id'])
        nodes[key] = f
        uf.add(key, f.get('chapter_id'))
    response_by_id = {r['response_id']: r for r in responses}
    refused = defaultdict(int)
    item_links = [link for link in links if link['level'] == 'item']
    item_links.sort(key=lambda link: link.get('kind') != 'follow_up')
    for link in item_links:
        a, b = (link['from_type'], link['from_id']), (link['to_type'], link['to_id'])
        for node in (a, b):
            if node[0] == 'response' and node not in nodes and node[1] in response_by_id:
                nodes[node] = response_by_id[node[1]]
                uf.add(node)
        if a in nodes and b in nodes and not uf.union(a, b):
            refused[link.get('kind')] += 1
    logging.info('Item links not applied (they would join two findings of one audit): %s', dict(refused))
    groups = defaultdict(list)
    for node in nodes:
        groups[uf.find(node)].append(node)
    issues, members = [], []
    for root, group in groups.items():
        group_findings = [nodes[n] for n in group if n[0] == 'finding']
        if not group_findings:
            continue
        group_findings.sort(key=lambda f: (as_date(f.get('publish_date')) or datetime.date.min, f['finding_id']))
        issue_id = group_findings[0]['finding_id']
        timeline = []
        for n in group:
            x = nodes[n]
            date = as_date(x.get('publish_date'))
            if n[0] == 'finding':
                status = x.get('followup_status') if x.get('followup_status') not in (None, 'not_applicable') else None
                timeline.append(dict(date=str(date) if date else None, kind='follow_up' if status else 'finding',
                                     id=x['finding_id'], chapter_id=x.get('chapter_id'), source=x.get('chapter_title'),
                                     url=x.get('page_url'), title=x.get('title'), status=status,
                                     text=x.get('description')))
            else:
                timeline.append(dict(date=str(date) if date else None, kind='response', id=x['response_id'],
                                     source=x.get('doc_title'), url=x.get('stored_url'), title=x.get('title'),
                                     status=x.get('status'), finding_text=x.get('finding_text'),
                                     text='\n'.join('%s: %s' % (r.get('body'), r.get('text'))
                                                    for r in x.get('responses') or [])))
            members.append(dict(issue_id=issue_id, member_type=n[0], member_id=n[1],
                                date=date))
        timeline.sort(key=lambda e: (e['date'] or '', e['kind'] != 'finding'))
        statuses = [e for e in timeline if e.get('status')]
        latest = group_findings[-1]
        bodies = sorted({b for f in group_findings for b in f.get('responsible_bodies') or []})
        issues.append(dict(
            issue_id=issue_id,
            title=latest.get('title'),
            description=latest.get('description'),
            bodies=bodies,
            severity=max((f.get('severity') for f in group_findings), key=lambda s: {'high': 2, 'medium': 1}.get(s, 0)),
            first_reported=as_date(group_findings[0].get('publish_date')),
            last_seen=max(as_date(e['date']) for e in timeline if e['date']) if any(e['date'] for e in timeline) else None,
            n_audits=len({f.get('chapter_id') for f in group_findings}),
            n_responses=sum(1 for n in group if n[0] == 'response'),
            latest_status=statuses[-1]['status'] if statuses else None,
            timeline=timeline,
        ))
    return issues, members


def link_all(chapters, publications, audits, findings, responses):
    for c in chapters:
        c['_date'] = as_date(c.get('publish_date'))
    chapter_by_id = {c['chapter_id']: c for c in chapters}
    findings_by_chapter = defaultdict(list)
    for f in sorted(findings, key=lambda f: (f['chapter_id'], f['seq'])):
        findings_by_chapter[f['chapter_id']].append(f)
    chapter_vectors = unit_rows(embed_many([chapter_label(c) for c in chapters]))
    links = []

    # 1. response sections -> chapters, then items -> findings
    sections = response_sections(responses)
    section_matches = match_sections(sections, chapters, chapter_vectors, publications)
    logging.info('Matched %d of %d response sections to chapters', len(section_matches), len(sections))
    pairs = []
    for i, (chapter, score, method) in section_matches.items():
        s = sections[i]
        for r in s['items']:
            links.append(dict(level='chapter', kind='response', from_type='response', from_id=r['response_id'],
                              to_type='chapter', to_id=str(chapter['chapter_id']), score=score, method=method))
        if findings_by_chapter.get(chapter['chapter_id']):
            pairs.append((s, chapter))

    def response_items(pair):
        s, chapter = pair
        fs = findings_by_chapter[chapter['chapter_id']]
        return s, fs, map_response_section(s, chapter, fs)

    with ThreadPoolExecutor(THREADS) as executor:
        for s, fs, mapping in executor.map(response_items, pairs):
            for i, j in mapping or []:
                links.append(dict(level='item', kind='response', from_type='response',
                                  from_id=s['items'][i]['response_id'], to_type='finding',
                                  to_id=fs[j]['finding_id'], score=None, method='llm'))

    # 2. follow-up audits -> original audits, then findings -> findings
    audit_rows = []
    for a in audits:
        c = chapter_by_id.get(a['chapter_id'])
        if c:
            audit_rows.append(dict(c, **{k: v for k, v in a.items() if v is not None}))
    followup_matches = match_followups(audit_rows, chapters, chapter_vectors)
    logging.info('Matched %d follow-up audits to earlier chapters', len(followup_matches))
    fpairs = []
    for chapter_id, (original, score, method) in followup_matches.items():
        links.append(dict(level='chapter', kind='follow_up', from_type='chapter', from_id=str(chapter_id),
                          to_type='chapter', to_id=str(original['chapter_id']), score=score, method=method))
        if findings_by_chapter.get(original['chapter_id']) and findings_by_chapter.get(chapter_id):
            fpairs.append((chapter_by_id[chapter_id], original))

    def followup_items(pair):
        followup, original = pair
        ofs, ffs = findings_by_chapter[original['chapter_id']], findings_by_chapter[followup['chapter_id']]
        return ofs, ffs, map_followup(original, ofs, followup, ffs)

    with ThreadPoolExecutor(THREADS) as executor:
        for ofs, ffs, mapping in executor.map(followup_items, fpairs):
            for i, j in mapping or []:
                links.append(dict(level='item', kind='follow_up', from_type='finding', from_id=ffs[i]['finding_id'],
                                  to_type='finding', to_id=ofs[j]['finding_id'], score=None, method='llm'))

    issues, members = build_issues(findings, responses, links)
    logging.info('%d links, %d issues (%d with more than one member)', len(links), len(issues),
                 sum(1 for i in issues if len(i['timeline']) > 1))
    logging.info('Gemini usage: %s', json.dumps(USAGE.by_model))
    return links, issues, members


LINK_FIELDS = [('level', 'string'), ('kind', 'string'), ('from_type', 'string'), ('from_id', 'string'),
               ('to_type', 'string'), ('to_id', 'string'), ('score', 'number'), ('method', 'string')]
ISSUE_FIELDS = [('issue_id', 'string'), ('title', 'string'), ('description', 'string'), ('bodies', 'array'),
                ('severity', 'string'), ('first_reported', 'date'), ('last_seen', 'date'), ('n_audits', 'integer'),
                ('n_responses', 'integer'), ('latest_status', 'string'), ('timeline', 'array')]
MEMBER_FIELDS = [('issue_id', 'string'), ('member_type', 'string'), ('member_id', 'string'), ('date', 'date')]


def output_flow(links, issues, members):
    for i, link in enumerate(links):
        link['link_id'] = i + 1
    return DF.Flow(
        sc.typed_resource(links, 'links', [('link_id', 'integer')] + LINK_FIELDS, ['link_id']),
        sc.typed_resource(issues, 'issues', ISSUE_FIELDS, ['issue_id']),
        sc.typed_resource(members, 'issue_members', MEMBER_FIELDS, ['member_type', 'member_id']),
    )


def run(reports_path, chapters_path, responses_path):
    chapters = load(reports_path, 'chapters')
    publications = load(reports_path, 'publications')
    audits = load(chapters_path, 'audits')
    findings = load(chapters_path, 'findings')
    responses = load(responses_path, 'responses')
    return link_all(chapters, publications, audits, findings, responses)


def flow(*_):
    from datapackage_pipelines_budgetkey.common.dump_to_sql_atomic import dump_to_sql_atomic
    links, issues, members = run(REPORTS_PATH, CHAPTERS_PATH, RESPONSES_PATH)
    return DF.Flow(
        output_flow(links, issues, members),
        DF.update_resource(None, **{'dpp:streaming': True}),
        DF.dump_to_path(OUT_PATH),
        dump_to_sql_atomic(dict(
            state_comptroller_links={'resource-name': 'links'},
            state_comptroller_issues={'resource-name': 'issues'},
            state_comptroller_issue_members={'resource-name': 'issue_members'},
        )),
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    for noisy in ('httpx', 'httpcore', 'google_genai', 'urllib3', 'openai'):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    links, issues, members = run(os.environ.get('REPORTS', REPORTS_PATH), os.environ.get('CHAPTERS', CHAPTERS_PATH),
                                 os.environ.get('RESPONSES', RESPONSES_PATH))
    DF.Flow(output_flow(links, issues, members), DF.dump_to_path(os.environ.get('OUT', 'state_comptroller_issues_out'))).process()
