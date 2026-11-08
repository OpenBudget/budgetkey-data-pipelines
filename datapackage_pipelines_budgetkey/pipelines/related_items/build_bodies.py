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
from collections import Counter, OrderedDict
from concurrent.futures import ThreadPoolExecutor

from datapackage_pipelines_budgetkey.common import llm

from datapackage_pipelines_budgetkey.pipelines.related_items import ri_db
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_entities as E
from datapackage_pipelines_budgetkey.pipelines.related_items import ri_text as T

FIELDS = ['name', 'aliases', 'entity_id', 'entity_name', 'entity_kind', 'comptroller_codes', 'budget_chapters',
          'status', 'note']
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


def candidate_lines(index, names):
    seen, lines = set(), []
    for name in names:
        for entity, score, how in index.candidates(name, limit=10, include_companies=True):
            if entity['id'] not in seen:
                seen.add(entity['id'])
                lines.append('    {} | {} | {}'.format(entity['id'], entity['name'], entity['kind']))
    return lines


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    parser = argparse.ArgumentParser()
    parser.add_argument('--chapters', nargs='*')
    parser.add_argument('--audits', nargs='*')
    parser.add_argument('--findings', nargs='*')
    parser.add_argument('--decisions', action='store_true')
    parser.add_argument('--out', default=str(E.BODIES_CSV))
    args = parser.parse_args()

    existing = E.load_bodies(args.out)
    known = set()
    for body in existing:
        for name in [body['name']] + body['aliases']:
            known.add(T.normalize_name(name))
    found = collect(args)
    todo = [(norm, entry) for norm, entry in found.items() if norm not in known]
    logging.info('%d names, %d new', len(found), len(todo))
    if not todo:
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
    with open(args.out, 'w', encoding='utf8', newline='') as f:
        w = csv.DictWriter(f, FIELDS)
        w.writeheader()
        for body in existing:
            w.writerow(dict(body, aliases='|'.join(body['aliases']),
                            comptroller_codes='|'.join(body['comptroller_codes']),
                            budget_chapters='|'.join(body['budget_chapters'])))
        for row in drafted:
            w.writerow(row)
    logging.info('Wrote %d new rows to %s; usage %s', len(drafted), args.out, usage.by_model)


if __name__ == '__main__':
    main()
