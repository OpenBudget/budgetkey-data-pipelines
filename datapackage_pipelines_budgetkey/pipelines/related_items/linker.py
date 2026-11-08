"""The generic related-items linker: finds which budgetkey records a source document is about, and links them.

A source supplies documents as dicts:
    doc_id, kind, title, date (datetime.date), text (full text; deterministic extractors run on it),
    llm_text (what the LLM reads, default text), what (a phrase describing the document, for the prompts),
    parts ([{part_id, text}]: sub-items such as a report's findings, so links can point at them),
    hints: bodies ([{code, name}] known to be the document's subject, e.g. the comptroller's audited bodies),
           years ((start, end) the document is about), llm (run the LLM steps), same_government_default
           (undated decision citations refer to the source's own government), exclude_decision (its own id)

and gets back two lists of rows: links (LINK_FIELDS) and mentions (MENTION_FIELDS) - every reference found,
linked or not, with why, for auditing and for improving recall later.
"""
import datetime
import hashlib
import logging
import os
from concurrent.futures import ThreadPoolExecutor

from datapackage_pipelines_budgetkey.common import llm

from datapackage_pipelines_budgetkey.pipelines.related_items import ri_budget as RB
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_comptroller as RC
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_entities as E
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_llm as L
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_procurement as RP
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_text as T
from datapackage_pipelines_budgetkey.pipelines.related_items.ri_decisions import DecisionIndex

VERSION = '1'
THREADS = int(os.environ.get('RELATED_ITEMS_THREADS') or 6)

LINK_FIELDS = [
    ('link_id', 'string'), ('source_doc_id', 'string'), ('source_kind', 'string'), ('source_part_id', 'string'),
    ('source_title', 'string'), ('source_date', 'date'), ('target_doc_id', 'string'), ('target_kind', 'string'),
    ('target_title', 'string'), ('target_year', 'integer'), ('target_code', 'string'), ('relation', 'string'),
    ('mention', 'string'), ('excerpt', 'string'), ('confidence', 'number'), ('method', 'string'),
    ('linker_version', 'string'),
]
MENTION_FIELDS = [
    ('source_doc_id', 'string'), ('source_kind', 'string'), ('source_part_id', 'string'),
    ('mention_kind', 'string'), ('mention', 'string'), ('status', 'string'), ('method', 'string'),
    ('target_doc_id', 'string'), ('confidence', 'number'), ('details', 'object'), ('excerpt', 'string'),
]

# Which relation wins when one target is linked several ways from the same place.
RELATION_ORDER = ['audited', 'subject', 'follows_up', 'amends', 'cancels', 'cites', 'responsible', 'funded',
                  'contractor', 'mentioned']

ORG_KINDS = {
    'ministry': {'government_office', 'law_mandated_organization'},
    'government_unit': {'government_office', 'law_mandated_organization'},
    'statutory_body': {'law_mandated_organization', 'government_office', 'company', 'association'},
    'government_company': {'company', 'law_mandated_organization'},
    'municipality': {'municipality', 'conurbation', 'drainage_authority', 'local_planning_committee',
                     'municipal_precinct'},
    'hospital': {'government_office', 'association', 'company', 'ottoman-association', 'health_service',
                 'law_mandated_organization'},
    'university': {'university', 'association', 'law_mandated_organization', 'company'},
    'health_fund': {'health_service', 'ottoman-association', 'association'},
    'ngo': {'association', 'ottoman-association', 'endowment', 'cooperative'},
    'company': {'company', 'cooperative', 'partnership', 'foreign_company'},
}
ALL_KINDS = set().union(*ORG_KINDS.values()) | E.FUZZY_KINDS

# Confidence by method; only links at or above MIN_CONFIDENCE are emitted.
CONFIDENCE = {
    'regex:corp_number': 0.97, 'comptroller_code': 0.95, 'alias': 0.93, 'exact_name': 0.9,
    'llm_resolve': 0.85, 'regex:budget_code': 0.95, 'comptroller_ref:title': 0.95, 'comptroller_ref:title_in_year': 0.9, 'regex:tender': 0.9,
}
MIN_CONFIDENCE = float(os.environ.get('RELATED_ITEMS_MIN_CONFIDENCE') or 0.8)


class Targets:
    """The target indexes, loaded once per run."""

    def __init__(self, entities=None, decisions=None, budget=None, supports=None, chapters=None, tenders=None):
        self.entities = entities
        self.decisions = decisions
        self.budget = budget
        self.supports = supports
        self.chapters = chapters
        self.tenders = tenders

    @classmethod
    def load(cls, kinds=('entities', 'decisions', 'budget', 'supports', 'chapters', 'tenders')):
        return cls(
            entities=E.EntityIndex.load() if 'entities' in kinds else None,
            decisions=DecisionIndex.load() if 'decisions' in kinds else None,
            budget=RB.BudgetIndex.load() if 'budget' in kinds else None,
            supports=RB.SupportProgramIndex.load() if 'supports' in kinds else None,
            chapters=RC.ChapterIndex.load() if 'chapters' in kinds else None,
            tenders=RP.TenderIndex.load() if 'tenders' in kinds else None,
        )

    def prepare(self):
        """Builds the lazily computed parts of the indexes (embeddings), before the linker's threads share them."""
        for index in (self.budget, self.supports):
            if index is not None:
                index.prepare()


class DocLinker:
    def __init__(self, doc, targets, usage):
        self.doc = doc
        self.t = targets
        self.usage = usage
        self.hints = doc.get('hints') or {}
        self.links = {}
        self.mentions = []
        self.bodies = []  # bodies.csv rows resolved for this document, for budget chapters

    # --- output ---

    def link(self, target_doc_id, target_kind, target_title, relation, mention, excerpt, method, confidence,
             part_id=None, target_year=None, target_code=None):
        if confidence < MIN_CONFIDENCE:
            return False
        key = (part_id or '', target_doc_id)
        row = dict(
            source_doc_id=self.doc['doc_id'], source_kind=self.doc['kind'], source_part_id=part_id,
            source_title=self.doc.get('title'), source_date=self.doc.get('date'), target_doc_id=target_doc_id,
            target_kind=target_kind, target_title=target_title, target_year=target_year, target_code=target_code,
            relation=relation, mention=mention, excerpt=excerpt, confidence=round(confidence, 3), method=method,
            linker_version=VERSION,
        )
        row['link_id'] = hashlib.sha1('|'.join([row['source_doc_id'], part_id or '', target_doc_id])
                                      .encode('utf8')).hexdigest()[:16]
        old = self.links.get(key)
        if old is None:
            self.links[key] = row
        else:
            # Keep the strongest evidence, and the most specific relation.
            if RELATION_ORDER.index(relation) < RELATION_ORDER.index(old['relation']):
                old['relation'] = relation
            if confidence > old['confidence']:
                for k in ('mention', 'excerpt', 'confidence', 'method'):
                    old[k] = row[k]
        return True

    def mention(self, kind, mention, status, method=None, target=None, confidence=None, details=None,
                excerpt=None, part_id=None):
        self.mentions.append(dict(
            source_doc_id=self.doc['doc_id'], source_kind=self.doc['kind'], source_part_id=part_id,
            mention_kind=kind, mention=mention, status=status, method=method, target_doc_id=target,
            confidence=confidence, details=details or {}, excerpt=excerpt,
        ))

    # --- segments ---

    def segments(self):
        """(part_id, text) pairs the deterministic extractors scan: each part, then the full text. A reference
        found in a part is linked from the part; one found only in the full text, from the document."""
        for part in self.doc.get('parts') or []:
            yield part['part_id'], part.get('text') or ''
        yield None, self.doc.get('text') or ''

    # --- deterministic ---

    def corporation_numbers(self):
        if not self.t.entities:
            return
        seen = set()
        for part_id, text in self.segments():
            for m in T.corporation_numbers(text):
                if (part_id, m['number']) in seen or (part_id is None and m['number'] in {n for _, n in seen}):
                    continue
                seen.add((part_id, m['number']))
                excerpt = T.excerpt(text, m['start'], m['end'])
                entity = self.t.entities.by_number(m['number'])
                if entity is None:
                    self.mention('corp_number', m['mention'], 'not_found', 'regex:corp_number', excerpt=excerpt,
                                 part_id=part_id)
                    continue
                target = E.doc_id(entity)
                self.link(target, 'entities', entity['name'], 'mentioned', m['mention'], excerpt,
                          'regex:corp_number', CONFIDENCE['regex:corp_number'], part_id)
                self.mention('corp_number', m['mention'], 'linked', 'regex:corp_number', target,
                             CONFIDENCE['regex:corp_number'], excerpt=excerpt, part_id=part_id)

    def decisions(self):
        if not self.t.decisions:
            return
        found = []
        for part_id, text in self.segments():
            for c in T.decision_citations(text):
                found.append((part_id, text, c))
        # Resolve dated citations first; an undated citation of a number already resolved in this document
        # (usually "החלטה 1306" after "החלטת הממשלה מס' 1306 מיום 14.01.2024") means the same decision.
        found.sort(key=lambda x: x[2]['date'] is None)
        resolved = {}
        seen = set()
        for part_id, text, c in found:
            number = str(int(c['number']))
            excerpt = T.excerpt(text, c['start'], c['end'])
            details = dict(number=number, date=c['date'].isoformat() if c['date'] else None,
                           date_precision=c['date_precision'], government=c['government'])
            if c['date'] is None and c['government'] is None and number in resolved:
                entry, confidence, how = resolved[number]
                how = 'same_document'
            else:
                entry, confidence, how = self.t.decisions.resolve(
                    c, self.doc.get('date'), self.hints.get('same_government_default', False),
                    self.hints.get('exclude_decision'))
                if entry and c['date']:
                    resolved.setdefault(number, (entry, confidence, how))
            if (part_id, number, entry and entry['id']) in seen:
                continue
            seen.add((part_id, number, entry and entry['id']))
            details['how'] = how
            if entry is None:
                self.mention('decision', c['mention'], how, 'regex:decision', details=details, excerpt=excerpt,
                             part_id=part_id)
                continue
            target = DecisionIndex.doc_id(entry)
            self.link(target, 'gov_decisions', entry['title'], c['relation'], c['mention'], excerpt,
                      'regex:decision:' + how, confidence, part_id)
            self.mention('decision', c['mention'], 'linked', 'regex:decision:' + how, target, confidence,
                         details=details, excerpt=excerpt, part_id=part_id)

    def budget_codes(self):
        if not self.t.budget:
            return
        years = RB.source_years(self.doc.get('date'), *(self.hints.get('years') or (None, None)))
        seen = set()
        for part_id, text in self.segments():
            for m in T.budget_codes(text):
                if (part_id, m['code']) in seen:
                    continue
                seen.add((part_id, m['code']))
                excerpt = T.excerpt(text, m['start'], m['end'])
                # The code as of the source's years; budget codes are reused for other things over time.
                item, year = self.t.budget.by_code(m['code'], list(reversed(years)))
                if item is None:
                    self.mention('budget_code', m['mention'], 'not_found', 'regex:budget_code',
                                 details=dict(code=m['code'], years=years), excerpt=excerpt, part_id=part_id)
                    continue
                target = RB.BudgetIndex.doc_id(item)
                code, title = item['years'][year]
                self.link(target, 'budget', title, 'mentioned', m['mention'], excerpt, 'regex:budget_code',
                          CONFIDENCE['regex:budget_code'], part_id, target_year=year, target_code=code)
                self.mention('budget_code', m['mention'], 'linked', 'regex:budget_code', target,
                             CONFIDENCE['regex:budget_code'], details=dict(code=m['code'], year=year),
                             excerpt=excerpt, part_id=part_id)

    def hint_bodies(self):
        """The source's own list of bodies it is about (the comptroller's audited institutions)."""
        if not self.t.entities:
            return
        for b in self.hints.get('bodies') or []:
            name = (b.get('name') or '').strip()
            body = self.t.entities.body_for_code(b['code']) if b.get('code') else None
            method = 'comptroller_code'
            if body is None:
                bodies = self.t.entities.bodies_for_name(name)
                body = bodies[0] if len(bodies) == 1 else None
                method = 'alias'
            if body is not None and body.get('status') == 'ok':
                self.bodies.append(body)
                if body['entity']:
                    target = E.doc_id(body['entity'])
                    self.link(target, 'entities', body['entity']['name'], b.get('relation') or 'audited', name,
                              None, method, CONFIDENCE[method])
                    self.mention('org', name, 'linked', method, target, CONFIDENCE[method],
                                 details=dict(code=b.get('code'), body=body['name']))
                else:
                    self.mention('org', name, 'no_entity', method, details=dict(code=b.get('code'),
                                                                                body=body['name']))
                continue
            entities = self.t.entities.exact(name)
            entities = [e for e in entities if e['kind'] in ALL_KINDS]
            if len(entities) == 1:
                target = E.doc_id(entities[0])
                self.link(target, 'entities', entities[0]['name'], b.get('relation') or 'audited', name, None,
                          'exact_name', CONFIDENCE['exact_name'])
                self.mention('org', name, 'linked', 'exact_name', target, CONFIDENCE['exact_name'],
                             details=dict(code=b.get('code')))
            else:
                self.mention('org', name, 'not_in_bodies', None, details=dict(code=b.get('code'),
                                                                             exact=len(entities)))

    # --- LLM ---

    def llm_mentions(self):
        source = self.doc.get('llm_text') or self.doc.get('text') or ''
        if not self.hints.get('llm') or not source.strip():
            return
        parts_hint = self.doc.get('parts_hint') or 'leave empty.'
        orgs, programs = L.extract(self.doc.get('what') or 'a document', source, parts_hint, self.usage)
        norm_source = L._norm_ws(source)
        part_ids = {p['part_id'] for p in self.doc.get('parts') or []}

        def where(item):
            excerpt = T.excerpt(norm_source, item['position'], item['position'] + len(item['quote'])) \
                if item['verified'] else None
            parts = [p for p in item.get('parts') or [] if p in part_ids] or [None]
            return excerpt, parts

        to_decide = []
        for item in orgs:
            excerpt, parts = where(item)
            if not item['verified']:
                self.mention('org', item['name'], 'unverified', 'llm', details=dict(quote=item['quote']))
                continue
            relation = item['role'] if item['role'] in RELATION_ORDER else 'mentioned'
            target, method = self.resolve_org(item)
            if method == 'no_entity':
                # A known public body that has no entity record: don't let fuzzy matching pick a namesake.
                self.mention('org', item['name'], 'no_entity', 'alias', details=dict(org_type=item['org_type']),
                             excerpt=excerpt)
                continue
            if target is not None and relation == 'mentioned' and E.doc_id(target) in self.t.entities.ubiquitous:
                self.mention('org', item['name'], 'ubiquitous', method, E.doc_id(target), excerpt=excerpt)
                continue
            if target is not None:
                for part_id in parts:
                    self.link(E.doc_id(target), 'entities', target['name'], relation, item['name'], excerpt,
                              method, CONFIDENCE[method], part_id)
                self.mention('org', item['name'], 'linked', method, E.doc_id(target), CONFIDENCE[method],
                             details=dict(org_type=item['org_type']), excerpt=excerpt)
                continue
            kinds = ORG_KINDS.get(item['org_type'], ALL_KINDS)
            candidates = [(e, s, how) for e, s, how in self.t.entities.candidates(item['name'], limit=8, kinds=kinds)]
            if not candidates:
                self.mention('org', item['name'], 'not_found', 'llm', details=dict(org_type=item['org_type']),
                             excerpt=excerpt)
                continue
            to_decide.append(dict(
                kind='org', item=item, excerpt=excerpt, parts=parts, relation=relation,
                mention=item['name'], hint=item['org_type'] + (', of ' + item['parent'] if item.get('parent') else ''),
                context=excerpt or item['quote'],
                candidates=[(E.doc_id(e), '{} [{}]'.format(e['name'], e['kind_he'] or e['kind'])) for e, _, _ in
                            candidates],
                entities={E.doc_id(e): e for e, _, _ in candidates},
            ))
        self.decide(to_decide, L.ORG_GUIDANCE)
        self.programs(programs, where)

    def resolve_org(self, item):
        """A certain match without the LLM: a curated body, or a unique entity with the same name."""
        names = [item['name']]
        if item.get('parent'):
            names.append('{}/{}'.format(item['parent'], item['name']))
        for name in names:
            bodies = [b for b in self.t.entities.bodies_for_name(name) if b.get('status') == 'ok']
            if len(bodies) == 1:
                self.bodies.append(bodies[0])
                return (bodies[0]['entity'], 'alias') if bodies[0]['entity'] else (None, 'no_entity')
        kinds = ORG_KINDS.get(item['org_type'], ALL_KINDS)
        exact = [e for e in self.t.entities.exact(item['name']) if e['kind'] in kinds]
        if len(exact) == 1:
            return exact[0], 'exact_name'
        return None, None

    def decide(self, to_decide, guidance):
        if not to_decide:
            return
        answers = L.decide(self.doc.get('what') or 'A document', self.doc.get('title'), self.doc.get('date'),
                           guidance, to_decide, self.usage)
        for n, d in enumerate(to_decide):
            choice, certainty = answers.get(n, (None, None))
            details = dict(candidates=[c for c, _ in d['candidates']], certainty=certainty,
                           choice=choice, **d.get('details', {}))
            if choice is None or certainty != 'certain':
                self.mention(d['kind'], d['mention'], 'ambiguous' if choice else 'no_match', 'llm_resolve',
                             choice, details=details, excerpt=d['excerpt'])
                continue
            for part_id in d['parts']:
                if 'emit' in d:
                    d['emit'](choice, part_id)
                else:
                    self.link(choice, 'entities', d['entities'][choice]['name'], d['relation'], d['mention'],
                              d['excerpt'], 'llm_resolve', CONFIDENCE['llm_resolve'], part_id)
            self.mention(d['kind'], d['mention'], 'linked', 'llm_resolve', choice, CONFIDENCE['llm_resolve'],
                         details=details, excerpt=d['excerpt'])

    def programs(self, programs, where):
        if not programs or not (self.t.budget or self.t.supports):
            return
        doc_years = RB.source_years(self.doc.get('date'), *(self.hints.get('years') or (None, None)))
        queries = [p for p in programs if p['verified']]
        for p in programs:
            if not p['verified']:
                self.mention(p['kind'], p['name'], 'unverified', 'llm', details=dict(quote=p['quote']))
        vectors = RB.embed_queries(['{} - {}'.format(p['name'], p.get('ministry') or '') for p in queries])
        to_decide = []
        for p, vector in zip(queries, vectors):
            excerpt, parts = where(p)
            chapters = set(RB.chapters_of(self.bodies))
            if p.get('ministry'):
                for b in self.t.entities.bodies_for_name(p['ministry']) if self.t.entities else []:
                    chapters.update(b.get('budget_chapters') or [])
            years = sorted({y for y in p.get('years') or [] if RB.MIN_YEAR <= y <= RB.MAX_YEAR}) or doc_years
            base = dict(kind=p['kind'], mention=p['name'], excerpt=excerpt, parts=parts, context=excerpt,
                        hint=p.get('ministry') or '', details=dict(years=years, chapters=sorted(chapters)))
            if not chapters:
                self.mention(p['kind'], p['name'], 'no_ministry', 'llm', details=base['details'], excerpt=excerpt)
                continue
            # What the text calls a program may be a budget item or a support program (or neither): offer both,
            # the kind the model gave first.
            budget = self.t.budget.candidates(vector, chapters, years, limit=8) if self.t.budget else []
            supports = self.t.supports.candidates(vector, chapters, years, limit=5) if self.t.supports else []
            budget_labels = [(RB.BudgetIndex.doc_id(i),
                              'budget item: ' + RB.BudgetIndex.label(i, self.year_of(i, years))) for i, _ in budget]
            support_labels = [(RB.SupportProgramIndex.doc_id(s),
                               'support program: ' + RB.SupportProgramIndex.label(s)) for s, _ in supports]
            labels = support_labels + budget_labels if p['kind'] == 'support_program' else budget_labels + support_labels
            if not labels:
                self.mention(p['kind'], p['name'], 'not_found', 'llm', details=base['details'], excerpt=excerpt)
                continue
            budget_emit = self.budget_emitter(base, {RB.BudgetIndex.doc_id(i): i for i, _ in budget}, years)
            support_emit = self.support_emitter(base, {RB.SupportProgramIndex.doc_id(s): s for s, _ in supports})
            to_decide.append(dict(base, candidates=labels, emit=lambda doc_id, part_id, b=budget_emit, s=support_emit:
                                  (s if doc_id.startswith('support_programs/') else b)(doc_id, part_id)))
        self.decide(to_decide, L.PROGRAM_GUIDANCE)

    @staticmethod
    def year_of(item, years):
        present = [y for y in years if y in item['years']]
        return max(present) if present else item['year']

    def budget_emitter(self, base, items, years):
        def emit(doc_id, part_id):
            item = items[doc_id]
            year = self.year_of(item, years)
            code, title = item['years'].get(year, (item['code'], item['title']))
            self.link(doc_id, 'budget', title, 'mentioned', base['mention'], base['excerpt'], 'llm_resolve',
                      CONFIDENCE['llm_resolve'], part_id, target_year=year, target_code=code)
        return emit

    def support_emitter(self, base, items):
        def emit(doc_id, part_id):
            program = items[doc_id]
            self.link(doc_id, 'support_programs', program['title'], 'mentioned', base['mention'], base['excerpt'],
                      'llm_resolve', CONFIDENCE['llm_resolve'], part_id)
        return emit

    def earlier_reports(self):
        """The comptroller's references to its own earlier reports (hints['earlier_reports'])."""
        refs = self.hints.get('earlier_reports') or []
        if not refs or not self.t.chapters:
            return
        to_decide = []
        for ref in refs:
            mention = '{} ({}): {}'.format(ref.get('report'), ref.get('year'), ref.get('title'))
            relation = 'follows_up' if ref.get('relation') == 'followed_up' else 'cites'
            details = dict(report=ref.get('report'), year=ref.get('year'), title=ref.get('title'),
                           relation=ref.get('relation'))
            entry, result, how = self.t.chapters.resolve(ref, self.doc.get('date'))
            if entry is not None:
                target = RC.ChapterIndex.doc_id(entry)
                if target == self.doc['doc_id']:
                    continue
                method = 'comptroller_ref:' + how
                self.link(target, 'state_comptroller_audits', entry['title'], relation, mention, None, method,
                          CONFIDENCE[method])
                self.mention('comptroller_report', mention, 'linked', method, target, CONFIDENCE[method],
                             details=details)
            elif how == 'llm':
                entries = {RC.ChapterIndex.doc_id(c): c for c, _ in result}
                to_decide.append(dict(
                    kind='comptroller_report', mention=mention, excerpt=None, parts=[None], context=mention,
                    hint=ref.get('relation'), details=details,
                    candidates=[(RC.ChapterIndex.doc_id(c), RC.ChapterIndex.label(c)) for c, _ in result],
                    emit=lambda doc_id, part_id, entries=entries, mention=mention, relation=relation: self.link(
                        doc_id, 'state_comptroller_audits', entries[doc_id]['title'], relation, mention, None,
                        'llm_resolve', CONFIDENCE['llm_resolve'], part_id)))
            else:
                self.mention('comptroller_report', mention, how, 'comptroller_ref', details=details)
        self.decide(to_decide, L.COMPTROLLER_GUIDANCE)

    def tenders(self):
        if not self.t.tenders or not self.t.entities:
            return
        # The bodies the document is about: curated ones resolved so far, and any whose entity got linked.
        linked = {l['target_doc_id'] for l in self.links.values()}
        bodies = self.bodies + [b for b in self.t.entities.bodies if b['entity'] and b.get('status') == 'ok'
                                and E.doc_id(b['entity']) in linked]
        seen = set()
        for part_id, text in self.segments():
            for m in T.tender_numbers(text):
                key = (part_id, m['number'])
                if key in seen or (part_id is None and any(k[1] == m['number'] for k in seen)):
                    continue
                seen.add(key)
                context = text[max(0, m['start'] - 250):m['end'] + 250]
                tender, how = self.t.tenders.resolve(m['number'], self.t.entities, bodies, context,
                                                     self.doc.get('date'))
                excerpt = T.excerpt(text, m['start'], m['end'])
                if tender is None:
                    self.mention('tender', m['mention'], how, 'regex:tender', details=dict(number=m['number']),
                                 excerpt=excerpt, part_id=part_id)
                    continue
                target = RP.TenderIndex.doc_id(tender)
                self.link(target, 'tenders', tender['title'], 'mentioned', m['mention'], excerpt, 'regex:tender',
                          CONFIDENCE['regex:tender'], part_id)
                self.mention('tender', m['mention'], 'linked', 'regex:tender', target, CONFIDENCE['regex:tender'],
                             details=dict(number=m['number'], publisher=tender['publisher']), excerpt=excerpt,
                             part_id=part_id)

    def run(self):
        self.corporation_numbers()
        self.decisions()
        self.budget_codes()
        self.hint_bodies()
        self.earlier_reports()
        self.llm_mentions()
        self.tenders()  # after the bodies the document is about are known
        return list(self.links.values()), self.mentions


def link_documents(docs, targets, threads=THREADS):
    """Links every document; returns (links, mentions, usage)."""
    usage = llm.Usage()
    targets.prepare()

    def one(doc):
        try:
            return DocLinker(doc, targets, usage).run()
        except Exception:
            logging.exception('Failed linking %s', doc['doc_id'])
            raise

    links, mentions = [], []
    with ThreadPoolExecutor(threads) as pool:
        for n, (l, m) in enumerate(pool.map(one, docs), 1):
            links.extend(l)
            mentions.extend(m)
            if n % 100 == 0:
                logging.info('Linked %d documents: %d links, %d mentions', n, len(links), len(mentions))
    return links, mentions, usage


def as_date(d):
    if isinstance(d, datetime.datetime):
        return d.date()
    if isinstance(d, datetime.date):
        return d
    try:
        return datetime.date.fromisoformat(str(d)[:10])
    except (TypeError, ValueError):
        return None

