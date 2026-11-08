"""Drafts bodies.csv, the curated list of public bodies, for review. A development tool, not a pipeline step.

Collects the names public bodies go by in the sources (the comptroller's institution list, the bodies its audits and
findings name, the offices of government decisions), retrieves entity candidates for each, and asks Gemini which
entity stands for the body and which budget chapters fund it. Rows already in bodies.csv are kept as they are
(they were reviewed); new names are appended with status=draft, to be checked by hand and set to ok/none.

Usage:
  python -m datapackage_pipelines_budgetkey.pipelines.related_items.build_bodies \\
      --chapters <reports chapters.csv>... --audits <audits.csv>... --findings <findings.csv>... [--decisions]
"""
import argparse
import csv
import json
import logging
import re
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor

from datapackage_pipelines_budgetkey.common import llm

from datapackage_pipelines_budgetkey.pipelines.related_items import ri_db
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_entities as E
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_text as T

FIELDS = ['name', 'aliases', 'entity_id', 'entity_name', 'entity_kind', 'comptroller_codes', 'budget_chapters',
          'ubiquitous', 'status', 'note']
BATCH = 8

SCHEMA = {
    'type': 'object',
    'properties': {
        'bodies': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'n': {'type': 'integer'},
                    'is_public_body': {'type': 'boolean'},
                    'canonical_name': {'type': 'string'},
                    'entity_id': {'type': ['string', 'null']},
                    'budget_chapters': {'type': 'array', 'items': {'type': 'string'}},
                    'confidence': {'type': 'string', 'enum': ['high', 'medium', 'low']},
                    'note': {'type': 'string'},
                },
                'required': ['n', 'is_public_body', 'canonical_name', 'entity_id', 'budget_chapters', 'confidence',
                             'note'],
            },
        },
    },
    'required': ['bodies'],
}

PROMPT = '''You are matching names of Israeli public bodies, as written in State Comptroller reports and government
decisions, to records of budgetkey's organization registry, and to chapters of the state budget.

Registry conventions:
- Government ministries and their units are recorded under old accounting names, as "<ministry>/<unit>", e.g.
  "משרד הרווחה/המשרד הראשי" is the welfare ministry's headquarters, "משרד האוצר/אגף התקציבים" is the Budget
  Department. Ministry names in the registry are often outdated (משרד הכלכלה = today's משרד הכלכלה והתעשייה;
  המשרד לאיכות הסביבה = המשרד להגנת הסביבה; משרד לבטחון פנים = המשרד לביטחון לאומי; משרד רוה"מ = משרד ראש
  הממשלה; צה"ל = צבא ההגנה לישראל).
- For a ministry as a whole, pick its headquarters record ("/המשרד הראשי") when there is one; otherwise the record
  that stands for the whole body (not a branch, a district, an accounting unit "חשבות", or a hospital of it).
- For a unit (a department, an authority inside a ministry), pick the record of that unit, if listed.
- Government companies, statutory corporations, municipalities, universities and NGOs have their own records.
- If no candidate is clearly the same body, entity_id is null. Never pick a similar-sounding but different body
  (another ministry's unit, a different city, a branch).

Budget chapters (4-digit codes; titles over the years):
{chapters}

For each name below give:
- is_public_body: false for generic phrases ("משרדי הממשלה", "הרשויות המקומיות", "גופים שונים") or people;
- canonical_name: the body's current official Hebrew name;
- entity_id: the id of the matching record from its candidates, or null;
- budget_chapters: the budget chapter codes whose budget is this body's own (a ministry's chapter and its
  development chapter, e.g. 0020 and 0060 for education; for a unit, its ministry's chapter). Empty for bodies
  outside the state budget (municipalities, companies, NGOs, Bank of Israel);
- confidence and a short note (in English) on anything uncertain.

Names:
{names}'''


def budget_chapters_text():
    rows = ri_db.query('select code, year, title from budget where depth = 1', ['code', 'year', 'title'])
    titles = OrderedDict()
    for r in sorted(rows, key=lambda r: (r['code'], int(r['year']))):
        titles.setdefault(r['code'], [])
        if r['title'] not in titles[r['code']]:
            titles[r['code']].append(r['title'])
    return '\n'.join('{}: {}'.format(code, ' / '.join(t)) for code, t in titles.items())


def collect(args):
    """{normalized name: dict(names=Counter, codes=set)}"""
    found = OrderedDict()

    def add(name, code=None, n=1):
        norm = T.normalize_name(name)
        if not norm or len(norm) < 3:
            return
        entry = found.setdefault(norm, dict(names=Counter(), codes=set()))
        entry['names'][name.strip()] += n
        if code:
            entry['codes'].add(str(code))

    csv.field_size_limit(1 << 30)
    for path in args.chapters or []:
        for row in csv.DictReader(open(path, encoding='utf8')):
            for b in json.loads(row.get('audited_bodies') or '[]'):
                add(b.get('name') or '', b.get('code'))
    for path in args.audits or []:
        for row in csv.DictReader(open(path, encoding='utf8')):
            for b in json.loads(row.get('audited_bodies') or '[]'):
                add(b.get('name') or '', b.get('code'))
            for b in json.loads(row.get('llm_audited_bodies') or '[]'):
                add(b.get('name') or '')
    for path in args.findings or []:
        for row in csv.DictReader(open(path, encoding='utf8')):
            for b in json.loads(row.get('responsible_bodies') or '[]'):
                add(b)
    if args.decisions:
        for row in ri_db.query('select office, count(*) as n from government_decisions group by 1', ['office', 'n']):
            if row['office']:
                add(row['office'], n=int(row['n']))
    return found


def merge_known(bodies, found):
    """Extends reviewed rows instead of drafting new ones, for collected names that already identify a body:
    - a name whose comptroller code exactly one row has becomes that row's alias;
    - a row named by the name (its own name first, else a single row with it as an alias) gains its codes.
    Names that point at more than one row are left for review. Returns ({norm: (body, change)}, [conflicts])."""
    by_code, by_name, by_alias = {}, {}, {}
    for body in bodies:
        for code in body['comptroller_codes']:
            by_code.setdefault(code, []).append(body)
        by_name.setdefault(T.normalize_name(body['name']), []).append(body)
        for alias in body['aliases']:
            by_alias.setdefault(T.normalize_name(alias), []).append(body)
    merged, conflicts = {}, []
    for norm, entry in found.items():
        owners = {id(b): b for code in entry['codes'] for b in by_code.get(code, [])}
        named = by_name.get(norm) or by_alias.get(norm) or []
        named = list({id(b): b for b in named}.values())
        if len(owners) > 1 or (owners and named and set(owners) != {id(b) for b in named}) or len(named) > 1:
            conflicts.append((norm, entry, list(owners.values()) + named))
            continue
        if not owners and not named:
            continue
        body = next(iter(owners.values())) if owners else named[0]
        changes = []
        if not named:
            name = entry['names'].most_common(1)[0][0]
            body['aliases'].append(name)
            changes.append('alias ' + name)
        new = sorted(set(entry['codes']) - set(body['comptroller_codes']))
        if new:
            body['comptroller_codes'] = sorted(set(body['comptroller_codes']) | set(new))
            changes.append('codes ' + '|'.join(new))
        merged[norm] = (body, ', '.join(changes) or 'known')
    return merged, conflicts


def candidate_lines(index, names):
    seen, lines = set(), []
    for name in names:
        for entity, score, how in index.candidates(name, limit=10, include_companies=True):
            if entity['id'] not in seen:
                seen.add(entity['id'])
                lines.append('    {} | {} | {}'.format(entity['id'], entity['name'], entity['kind']))
    return lines


def institution_aliases(bodies, path):
    """Aliases for reviewed bodies from the comptroller's institution list (AuditedInstitutions: one string per
    institution, its names separated by commas and semicolons - and names have commas of their own). A part becomes
    an alias of a body when another part of the same entry is already one of the body's names, and the part is
    either an acronym (נתג"ז) or shares most of its words with the body's name - so name fragments split at a comma
    ("חברה להנדסה בע"מ") are not taken. Returns {body name: [new aliases]}."""
    entries = json.load(open(path, encoding='utf8'))
    by_norm = {}
    for body in bodies:
        if body.get('status') != 'ok':
            continue
        for name in [body['name']] + body['aliases']:
            by_norm.setdefault(T.normalize_name(name), body)
    added = {}
    for entry in entries:
        parts = [p.strip() for p in re.split(r'[,;]', entry) if p.strip()]
        norms = {T.normalize_name(p): p for p in parts}
        bodies_here = {id(by_norm[n]): by_norm[n] for n in norms if n in by_norm}
        if len(bodies_here) != 1:
            continue
        body = next(iter(bodies_here.values()))
        known = {T.normalize_name(n) for n in [body['name']] + body['aliases']}
        words = set(T.normalize_name(body['name']).split())
        for norm, part in norms.items():
            if not norm or norm in known:
                continue
            is_acronym = re.fullmatch(r'[\u05d0-\u05ea]+"[\u05d0-\u05ea]', part.strip()) is not None
            shared = len(set(norm.split()) & words) / max(len(norm.split()), 1)
            if is_acronym or (shared >= 0.6 and len(norm.split()) >= 2):
                body['aliases'].append(part)
                known.add(norm)
                added.setdefault(body['name'], []).append(part)
    return added


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    parser = argparse.ArgumentParser()
    parser.add_argument('--chapters', nargs='*')
    parser.add_argument('--audits', nargs='*')
    parser.add_argument('--findings', nargs='*')
    parser.add_argument('--decisions', action='store_true')
    parser.add_argument('--out', default=str(E.BODIES_CSV))
    parser.add_argument('--institution-names', help='AuditedInstitutions JSON: add aliases to reviewed bodies')
    parser.add_argument('--dry-run', action='store_true', help='only merge names and codes into reviewed rows')
    args = parser.parse_args()

    existing = E.load_bodies(args.out)
    if args.institution_names:
        for name, aliases in institution_aliases(existing, args.institution_names).items():
            print('ALIASES', name, '<-', ' | '.join(aliases))
        write(args.out, existing, [])
        return
    found = collect(args)
    merged, conflicts = merge_known(existing, found)
    for norm, (body, change) in merged.items():
        if change != 'known':
            print('MERGED', body['name'], '<-', change)
    for norm, entry, bodies in conflicts:
        print('CONFLICT', ' = '.join(entry['names']), sorted(entry['codes']), '->', ' | '.join(b['name'] for b in bodies))
    handled = set(merged) | {norm for norm, _, _ in conflicts}
    todo = [(norm, entry) for norm, entry in found.items() if norm not in handled]
    if args.dry_run:
        todo = todo[:0]
    logging.info('%d names, %d merged into reviewed rows, %d conflicts, %d new', len(found),
                 sum(1 for _, c in merged.values() if c != 'known'), len(conflicts), len(todo))
    if not todo:
        write(args.out, existing, [])
        return

    index = E.EntityIndex.load(bodies=existing)
    chapters = budget_chapters_text()
    # The ministries' headquarters records, offered with every ministry-like name.
    hq = ['    {} | {} | {}'.format(e['id'], e['name'], e['kind']) for e in index.by_id.values()
          if e['kind'] == 'government_office' and 'המשרד הראשי' in e['name']]
    usage = llm.Usage()

    def decide(batch):
        blocks = []
        for n, (norm, entry) in enumerate(batch, 1):
            names = [name for name, _ in entry['names'].most_common()]
            lines = candidate_lines(index, names)
            if any('משרד' in name for name in names):
                lines += [l for l in hq if l not in lines]
            blocks.append('{}. {}{}\n  candidates:\n{}'.format(
                n, ' = '.join(names[:5]),
                ' (comptroller codes {})'.format(', '.join(sorted(entry['codes']))) if entry['codes'] else '',
                '\n'.join(lines) or '    (none)'))
        _, result = llm.complete(PROMPT.format(chapters=chapters, names='\n\n'.join(blocks)), schema=SCHEMA,
                                 usage=usage)
        rows = []
        answers = {b['n']: b for b in result['bodies']}
        for n, (norm, entry) in enumerate(batch, 1):
            a = answers.get(n)
            names = [name for name, _ in entry['names'].most_common()]
            entity = index.by_id.get((a or {}).get('entity_id') or '')
            rows.append(dict(
                name=(a or {}).get('canonical_name') or names[0],
                aliases='|'.join(x for x in names if x != (a or {}).get('canonical_name')),
                entity_id=entity['id'] if entity else '',
                entity_name=entity['name'] if entity else '',
                entity_kind=entity['kind'] if entity else '',
                comptroller_codes='|'.join(sorted(entry['codes'])),
                budget_chapters='|'.join((a or {}).get('budget_chapters') or []),
                status='draft' if a and a['is_public_body'] else 'not_a_body',
                note='{}: {}'.format(a['confidence'], a['note']) if a else 'no answer',
            ))
        return rows

    batches = [todo[i:i + BATCH] for i in range(0, len(todo), BATCH)]
    with ThreadPoolExecutor(4) as pool:
        drafted = [row for rows in pool.map(decide, batches) for row in rows]
    write(args.out, existing, drafted)
    logging.info('Wrote %d new rows to %s; usage %s', len(drafted), args.out, usage.by_model)


def write(path, existing, drafted):
    with open(path, 'w', encoding='utf8', newline='') as f:
        w = csv.DictWriter(f, FIELDS, extrasaction='ignore')
        w.writeheader()
        for body in existing:
            w.writerow(dict(body, aliases='|'.join(body['aliases']),
                            comptroller_codes='|'.join(body['comptroller_codes']),
                            budget_chapters='|'.join(body['budget_chapters'])))
        for row in drafted:
            w.writerow(row)


if __name__ == '__main__':
    main()
