"""Organizations: budgetkey's entities (companies, associations, municipalities, government bodies...).

Resolution order, most certain first:
1. corporation numbers in the text -> the entity with that id;
2. bodies.csv, the curated list of public bodies (ministries, authorities, statutory corporations, government
   companies) with their names over time, the comptroller's institution codes and the entity that stands for
   each - government bodies appear in entities under old accounting names ("משרד הרווחה/המשרד הראשי"), so they
   can't be matched by name;
3. a unique entity with the same normalized name and a compatible kind;
4. candidates by fuzzy name similarity (never private companies or partnerships: there are 750k of them and most
   names are generic), decided by the LLM.
"""
import csv
import os
import threading
from collections import defaultdict
from pathlib import Path

from datapackage_pipelines_budgetkey.pipelines.related_items import ri_db
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_text as T

BODIES_CSV = Path(__file__).parent / 'bodies.csv'

# Kinds searched by fuzzy similarity; the rest (companies, partnerships, foreign companies, municipal party
# lists, house committees, provident funds) only by number or exact name.
FUZZY_KINDS = {
    'government_office', 'municipality', 'law_mandated_organization', 'association', 'ottoman-association',
    'cooperative', 'endowment', 'local_community_committee', 'religion_service', 'local_planning_committee',
    'conurbation', 'professional_association', 'health_service', 'drainage_authority', 'university',
    'municipal_precinct', 'religious_court_sacred_property', 'west_bank_corporation',
}
EXACT_ONLY_KINDS = {'company', 'partnership', 'foreign_company', 'provident_fund'}
NEVER_KINDS = {'municipal_parties', 'house_committee'}


def doc_id(entity):
    return 'org/{}/{}'.format(entity['kind'], entity['id'])


def split_list(s):
    return [x.strip() for x in (s or '').split('|') if x.strip()]


def load_bodies(path=BODIES_CSV):
    if not os.path.exists(path):
        return []
    with open(path, encoding='utf8') as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        row['aliases'] = split_list(row.get('aliases'))
        row['comptroller_codes'] = split_list(row.get('comptroller_codes'))
        row['budget_chapters'] = split_list(row.get('budget_chapters'))
    return rows


class EntityIndex:
    KIND = 'entities'

    def __init__(self, rows, bodies=None):
        self.by_id = {}
        self.by_norm = defaultdict(list)
        self.fuzzy_names = []
        self.fuzzy_entities = []
        self._company_names = None
        self._lock = threading.Lock()
        for row in rows:
            if row['kind'] in NEVER_KINDS or not row.get('name'):
                continue
            entity = dict(id=row['id'], name=row['name'], kind=row['kind'], kind_he=row.get('kind_he'))
            self.by_id[row['id']] = entity
            norm = T.normalize_name(row['name'])
            if norm:
                self.by_norm[norm].append(entity)
                if row['kind'] in FUZZY_KINDS:
                    self.fuzzy_names.append(norm)
                    self.fuzzy_entities.append(entity)
        self.bodies = []
        self.ubiquitous = set()  # doc ids of entities that are only linked as more than a passing mention
        self.body_by_code = {}
        self.body_by_norm = defaultdict(list)
        for body in bodies if bodies is not None else load_bodies():
            entity = self.by_id.get(body.get('entity_id') or '')
            body = dict(body, entity=entity)
            self.bodies.append(body)
            if entity and (body.get('ubiquitous') or '').strip().lower() in ('yes', 'true', '1'):
                self.ubiquitous.add(doc_id(entity))
            for code in body['comptroller_codes']:
                self.body_by_code[code] = body
            for name in [body['name']] + body['aliases']:
                norm = T.normalize_name(name)
                if norm and body not in self.body_by_norm[norm]:
                    self.body_by_norm[norm].append(body)

    @classmethod
    def load(cls, bodies=None):
        return cls(ri_db.query('select id, name, kind, kind_he from entities', ['id', 'name', 'kind', 'kind_he']),
                   bodies)

    # --- lookups ---

    def by_number(self, number):
        return self.by_id.get(number)

    def body_for_code(self, code):
        return self.body_by_code.get(str(code))

    def bodies_for_name(self, name):
        return self.body_by_norm.get(T.normalize_name(name), [])

    def exact(self, name):
        return self.by_norm.get(T.normalize_name(name), [])

    def _companies(self):
        with self._lock:  # build_bodies asks from several threads
            if self._company_names is None:
                entities = [e for e in self.by_id.values() if e['kind'] in EXACT_ONLY_KINDS]
                self._company_names = ([T.normalize_name(e['name']) for e in entities], entities)
        return self._company_names

    def candidates(self, name, limit=12, kinds=None, include_companies=False):
        """Entities whose names look like `name`: exact normalized matches of any kind first, then fuzzy matches
        among the FUZZY_KINDS (and companies too with include_companies, for curating bodies.csv - too noisy for
        automatic linking). Each candidate is (entity, score 0-100, how)."""
        from rapidfuzz import fuzz, process
        norm = T.normalize_name(name)
        if not norm:
            return []
        out, seen = [], set()
        for body in self.body_by_norm.get(norm, []):
            if body['entity'] and body['entity']['id'] not in seen:
                seen.add(body['entity']['id'])
                out.append((body['entity'], 100, 'body'))
        for entity in self.by_norm.get(norm, []):
            if entity['id'] not in seen and (kinds is None or entity['kind'] in kinds):
                seen.add(entity['id'])
                out.append((entity, 100, 'exact'))
        for _, score, i in process.extract(norm, self.fuzzy_names, scorer=fuzz.WRatio, limit=limit * 3,
                                           score_cutoff=70):
            entity = self.fuzzy_entities[i]
            if entity['id'] in seen or (kinds is not None and entity['kind'] not in kinds):
                continue
            seen.add(entity['id'])
            out.append((entity, score, 'fuzzy'))
            if len(out) >= limit:
                break
        if include_companies:
            names, entities = self._companies()
            for _, score, i in process.extract(norm, names, scorer=fuzz.token_set_ratio, limit=6, score_cutoff=80):
                if entities[i]['id'] not in seen:
                    seen.add(entities[i]['id'])
                    out.append((entities[i], score, 'fuzzy_company'))
            return out[:limit + 6]
        return out[:limit]

