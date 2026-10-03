"""Discovers topics for analysis pages.

    context (SQL) -> propose (Gemini Pro) -> closest budget items -> gate (Gemini Flash) -> registry

Each run adds at most MAX_NEW_TOPICS to the `analysis_topics` registry. Topics already in
the registry are never renamed or removed here; blocking is done in topic_overrides.yaml.
"""
import datetime
import difflib
import json
import os
import re
from pathlib import Path

import yaml

from datapackage_pipelines_budgetkey.common import llm
from datapackage_pipelines_budgetkey.common.dump_to_sql_atomic import dump_to_sql_atomic
from datapackage_pipelines_budgetkey.pipelines.analysis.query import query

ROOT = Path(__file__).parent
REGISTRY_TABLE = 'analysis_topics'

MAX_CANDIDATES = 40
MAX_NEW_TOPICS = 20
TARGET_TOPICS = 200
MIN_INTEREST = 3
CLOSEST_ITEMS = 5

CATEGORIES = [
    'health', 'education', 'welfare', 'elderly-and-disability', 'employment-and-economy',
    'housing', 'transport-and-infrastructure', 'energy-water-environment', 'agriculture-and-food',
    'science-and-technology', 'culture-sport-heritage', 'religion', 'security-and-emergency',
    'law-enforcement-and-justice', 'local-government-and-periphery', 'immigration-and-integration',
    'government-and-public-finance', 'foreign-relations',
]

# Ordinary and development budgets of one policy area count as one office when judging
# whether a topic is really just a single ministry (pairs from the hackathon's budget_reference.py).
OFFICE_GROUPS = {
    code: area
    for area, codes in {
        'health': ['24', '67', '92', '93', '94'], 'education': ['20', '60'], 'transport': ['40', '79'],
        'housing': ['29', '70', '42', '51'], 'economy': ['38', '76'], 'tourism': ['37', '78'],
        'water': ['41', '73'], 'energy': ['34', '35', '83'], 'internal-security': ['07', '52'],
        'pmo-and-finance': ['04', '05', '89'],
    }.items()
    for code in codes
}

DEFAULT_RELEVANCE_MONTHS = 24
MIN_RELEVANCE_MONTHS = 6
MAX_RELEVANCE_MONTHS = 36

QUESTION_PREFIXES = ('מה הממשלה עושה בנושא', 'מה הממשלה עושה עבור', 'איך הממשלה')
SLUG_RE = re.compile(r'^[a-z0-9]+(-[a-z0-9]+){0,5}$')

PROPOSE_SCHEMA = {
    'type': 'object',
    'properties': {
        'topics': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'slug': {'type': 'string'},
                    'topic_he': {'type': 'string'},
                    'question_he': {'type': 'string'},
                    'description_he': {'type': 'string'},
                    'category': {'type': 'string', 'enum': CATEGORIES},
                    'why': {'type': 'string'},
                    'budget_codes': {'type': 'array', 'items': {'type': 'string'}},
                    'evidence': {'type': 'array', 'items': {'type': 'string'}},
                    'temporal': {'type': 'boolean'},
                    'relevance_months': {'type': 'integer'},
                },
                'required': ['slug', 'topic_he', 'question_he', 'description_he', 'category', 'why',
                             'budget_codes', 'evidence', 'temporal', 'relevance_months'],
            },
        },
    },
    'required': ['topics'],
}

GATE_SCHEMA = {
    'type': 'object',
    'properties': {
        'verdicts': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'slug': {'type': 'string'},
                    'neutral': {'type': 'boolean'},
                    'suggested_question_he': {'type': 'string'},
                    'cross_cutting': {'type': 'boolean'},
                    'duplicate_of': {'type': 'string'},
                    'answerable': {'type': 'boolean'},
                    'interest': {'type': 'integer'},
                    'temporal': {'type': 'boolean'},
                    'major_issue': {'type': 'boolean'},
                    'reason': {'type': 'string'},
                },
                'required': ['slug', 'neutral', 'cross_cutting', 'duplicate_of', 'answerable', 'interest',
                             'temporal', 'major_issue', 'reason'],
            },
        },
    },
    'required': ['verdicts'],
}


def load_prompt(name, **values):
    text = (ROOT / 'prompts' / name).read_text()
    for k, v in values.items():
        text = text.replace('{{%s}}' % k, str(v))
    unfilled = re.findall(r'\{\{[A-Z_]+\}\}', text)
    assert not unfilled, 'Unfilled placeholders %s in prompt %s' % (unfilled, name)
    return text


def millions(amount):
    return '{:,.0f}'.format((amount or 0) / 1e6)


# ------------------------------------------------------------------ context

def latest_year():
    return query('SELECT max(year) AS year FROM budget_items_data WHERE level = 1 AND amount_allocated > 0')[0]['year']


def budget_titles(year):
    """Levels 1-3 of the expense budget for `year`, in code order."""
    return query('''
        SELECT code, title, level, amount_allocated FROM budget_items_data
        WHERE year = %d AND level IN (1, 2, 3) AND amount_allocated > 0 AND LEFT(code, 2) <> '00'
        ORDER BY code
    ''' % year)


def budget_tree(rows):
    return '\n'.join(
        '%s%s %s (%s)' % ('  ' * (r['level'] - 1), r['code'], r['title'], millions(r['amount_allocated']))
        for r in rows
    )


def functional_classes(year):
    rows = query('''
        SELECT functional_class_top_level AS top, functional_class_detailed AS detailed,
               SUM(amount_allocated) AS amount
        FROM budget_items_data
        WHERE year = %d AND level = 4 AND functional_class_detailed IS NOT NULL
        GROUP BY 1, 2 ORDER BY 3 DESC
    ''' % year)
    return '\n'.join('- %s / %s (%s)' % (r['top'], r['detailed'], millions(r['amount'])) for r in rows)


def support_programs(year):
    rows = query('''
        SELECT purpose, supporting_ministry FROM support_programs_data
        WHERE max_year >= %d ORDER BY total_approved DESC NULLS LAST LIMIT 300
    ''' % (year - 1))
    return '\n'.join('- %s | %s' % (r['purpose'], r['supporting_ministry']) for r in rows)


def social_services(year):
    rows = query('''
        SELECT activity_name, office FROM social_services_data
        WHERE last_activity_year >= %d ORDER BY current_budget DESC NULLS LAST
    ''' % (year - 2))
    return '\n'.join('- %s | %s' % (r['activity_name'], r['office']) for r in rows)


def decisions():
    rows = query('''
        SELECT title FROM government_decisions_data
        WHERE publication_type = 'החלטות ממשלה' AND publication_date > now() - interval '12 months'
        ORDER BY publication_date DESC LIMIT 500
    ''')
    return '\n'.join('- %s' % re.sub(r'\s+', ' ', r['title'] or '')[:150] for r in rows)


def existing_topics_text(existing):
    if not existing:
        return '(none yet)'
    return '\n'.join(
        '- `%s` [%s] %s' % (t['slug'], t.get('status', 'active'), t['question'])
        for t in existing
    )


# ------------------------------------------------------------------ steps

def propose(year, titles, existing, max_candidates):
    prompt = load_prompt(
        'topics_propose.md',
        LATEST_YEAR=year,
        MAX_CANDIDATES=max_candidates,
        CATEGORIES=', '.join('`%s`' % c for c in CATEGORIES),
        EXISTING_TOPICS=existing_topics_text(existing),
        BUDGET_TREE=budget_tree(titles),
        FUNCTIONAL_CLASSES=functional_classes(year),
        SUPPORT_PROGRAMS=support_programs(year),
        SOCIAL_SERVICES=social_services(year),
        DECISIONS=decisions(),
    )
    print('PROPOSE prompt: {:,} chars'.format(len(prompt)))
    _, result = llm.complete(prompt, schema=PROPOSE_SCHEMA, model=llm.MODEL, temperature=0.7, use_cache=False)
    return result['topics']


def office_groups(codes):
    return sorted({OFFICE_GROUPS.get(code[:2], code[:2]) for code in codes})


def check_candidate(c, taken_slugs, known_codes):
    """Mechanical checks the model can get wrong. Returns a list of problems.

    Also normalises the cited budget codes to ones that really exist and derives
    the office groups they fall under.
    """
    problems = []
    c['budget_codes'] = [code for code in c['budget_codes'] if code in known_codes]
    c['office_groups'] = office_groups(c['budget_codes'])
    if not c['budget_codes']:
        problems.append('no valid budget codes cited')
    if not SLUG_RE.match(c['slug']):
        problems.append('bad slug')
    if c['slug'] in taken_slugs:
        problems.append('slug already taken')
    if not c['question_he'].strip().startswith(QUESTION_PREFIXES):
        problems.append('question not in one of the fixed forms')
    if c['category'] not in CATEGORIES:
        problems.append('unknown category')
    return problems


def _normalize(text):
    return re.sub(r'[^\w\s]', ' ', text or '').strip()


def _cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(y * y for y in b) ** 0.5
    return dot / (na * nb) if na and nb else 0.0


def closest_budget_items(candidates, titles, k=CLOSEST_ITEMS):
    """Attaches the k level 2-3 budget titles nearest each candidate topic.

    Uses OpenAI embeddings when available; falls back to string similarity, which is
    enough to catch a topic that is just a budget item's title.
    """
    items = [t for t in titles if t['level'] in (2, 3)]
    labels = ['%s %s' % (t['code'], t['title']) for t in items]
    if os.environ.get('OPENAI_API_KEY'):
        from datapackage_pipelines_budgetkey.common.cached_openai import embed_many
        item_vecs = embed_many([t['title'] for t in items])
        topic_vecs = embed_many(['%s: %s' % (c['topic_he'], c['description_he']) for c in candidates])
        for c, vec in zip(candidates, topic_vecs):
            scored = sorted(zip((_cosine(vec, v) for v in item_vecs), labels), reverse=True)
            c['closest_budget_items'] = ['%s (%.2f)' % (l, s) for s, l in scored[:k]]
    else:
        print('OPENAI_API_KEY not set - using string similarity for closest budget items')
        norm_titles = [_normalize(t['title']) for t in items]
        for c in candidates:
            topic = _normalize(c['topic_he'])
            scored = sorted(
                ((difflib.SequenceMatcher(None, topic, t).ratio(), l) for t, l in zip(norm_titles, labels)),
                reverse=True
            )
            c['closest_budget_items'] = ['%s (%.2f)' % (l, s) for s, l in scored[:k]]
    return candidates


def gate(candidates, existing):
    shown = [
        {k: c[k] for k in ('slug', 'topic_he', 'question_he', 'description_he', 'category', 'temporal',
                           'budget_codes', 'office_groups', 'evidence', 'closest_budget_items')}
        for c in candidates
    ]
    prompt = load_prompt(
        'topics_gate.md',
        EXISTING_TOPICS=existing_topics_text(existing),
        CANDIDATES=json.dumps(shown, ensure_ascii=False, indent=1),
    )
    _, result = llm.complete(prompt, schema=GATE_SCHEMA, model=llm.MODEL_FAST, temperature=0, use_cache=False)
    return {v['slug']: v for v in result['verdicts']}


def accepted(verdict):
    return (
        verdict is not None
        and verdict['neutral'] and verdict['cross_cutting'] and verdict['answerable'] and verdict['major_issue']
        and not verdict['duplicate_of']
        and verdict['interest'] >= MIN_INTEREST
    )


def discover(existing, max_candidates=MAX_CANDIDATES, max_new=MAX_NEW_TOPICS):
    """Returns (new_topics, all_candidates_with_verdicts)."""
    room = min(max_new, TARGET_TOPICS - len([t for t in existing if t.get('status') != 'blocked']))
    if room <= 0:
        print('Registry is full ({} topics), nothing to discover'.format(len(existing)))
        return [], []

    year = latest_year()
    titles = budget_titles(year)
    candidates = propose(year, titles, existing, max_candidates)
    print('PROPOSED {} candidates'.format(len(candidates)))

    taken = {t['slug'] for t in existing}
    known_codes = {t['code'] for t in titles}
    for c in candidates:
        c['problems'] = check_candidate(c, taken, known_codes)
        taken.add(c['slug'])
    closest_budget_items(candidates, titles)
    verdicts = gate([c for c in candidates if not c['problems']], existing)

    for c in candidates:
        c['verdict'] = verdicts.get(c['slug'])
        c['accepted'] = not c['problems'] and accepted(c['verdict'])
    winners = sorted((c for c in candidates if c['accepted']), key=lambda c: -c['verdict']['interest'])[:room]

    now = datetime.datetime.now()
    run_id = now.strftime('%Y-%m-%d')
    new_topics = [
        dict(
            slug=c['slug'], topic=c['topic_he'], question=c['question_he'], description=c['description_he'],
            category=c['category'], why=c['why'], budget_codes=c['budget_codes'], evidence=c['evidence'],
            interest=c['verdict']['interest'], status='active',
            temporal=temporal(c), expires_at=expires_at(c, now),
            created_at=now, discovery_run=run_id,
        )
        for c in winners
    ]
    return new_topics, candidates


def temporal(c):
    # Either side flagging it is enough: a temporal topic that slips through as durable never expires.
    return bool(c['temporal'] or (c['verdict'] or {}).get('temporal'))


def expires_at(c, now):
    """Temporal topics stop being regenerated (status 'expired') after their relevance window."""
    if not temporal(c):
        return None
    months = c.get('relevance_months') or DEFAULT_RELEVANCE_MONTHS
    months = max(MIN_RELEVANCE_MONTHS, min(MAX_RELEVANCE_MONTHS, months))
    return (now + datetime.timedelta(days=round(months * 30.4))).date()


def expire(registry, today=None):
    today = today or datetime.date.today()
    for t in registry:
        if t.get('status') == 'active' and t.get('expires_at') and t['expires_at'] < today:
            t['status'] = 'expired'
            print('EXPIRED topic', t['slug'])


def load_overrides():
    return yaml.safe_load((ROOT / 'topic_overrides.yaml').read_text()) or {}


# ------------------------------------------------------------------ pipeline

FIELDS = [
    ('slug', 'string'), ('topic', 'string'), ('question', 'string'), ('description', 'string'),
    ('category', 'string'), ('why', 'string'), ('budget_codes', 'array'), ('evidence', 'array'),
    ('interest', 'integer'), ('temporal', 'boolean'), ('expires_at', 'date'), ('status', 'string'),
    ('created_at', 'datetime'), ('discovery_run', 'string'),
]


def load_registry():
    try:
        rows = query('SELECT * FROM %s' % REGISTRY_TABLE)
    except Exception as e:
        print('No existing registry ({}), starting fresh'.format(e))
        return []
    for row in rows:
        for name, type_ in FIELDS:
            if type_ == 'array' and isinstance(row.get(name), str):
                row[name] = json.loads(row[name])
            if type_ == 'date' and isinstance(row.get(name), datetime.datetime):
                row[name] = row[name].date()
    return rows


def registry_flow(registry):
    import dataflows as DF
    rows = [{name: t.get(name) for name, _ in FIELDS} for t in registry]
    return DF.Flow(
        rows,
        DF.update_resource(-1, name='analysis_topics', path='analysis_topics.csv'),
        *[DF.set_type(name, type=type_) for name, type_ in FIELDS],
        DF.set_primary_key(['slug']),
        DF.dump_to_path('/var/datapackages/analysis/topics'),
        dump_to_sql_atomic(dict(analysis_topics={'resource-name': 'analysis_topics'}), engine='env://DPP_DB_ENGINE'),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )


def flow(parameters, *_):
    overrides = load_overrides()
    blocked = set(overrides.get('blocked') or [])
    registry = load_registry()
    print('REGISTRY has {} topics'.format(len(registry)))
    for extra in overrides.get('extra') or []:
        if extra['slug'] not in {t['slug'] for t in registry}:
            registry.append(dict(extra, status='active', created_at=datetime.datetime.now(), discovery_run='manual'))
    for t in registry:
        if t['slug'] in blocked:
            t['status'] = 'blocked'
    expire(registry)

    new_topics, candidates = discover(registry, max_new=parameters.get('max-new', MAX_NEW_TOPICS))
    for c in candidates:
        v = c.get('verdict') or {}
        print('CANDIDATE {} {} interest={} problems={} reason={}'.format(
            'ACCEPTED' if c['accepted'] else 'REJECTED', c['slug'], v.get('interest'), c['problems'], v.get('reason')))
    print('ADDED {} new topics'.format(len(new_topics)))
    return registry_flow(registry + new_topics)


# ------------------------------------------------------------------ CLI

def main():
    import argparse
    parser = argparse.ArgumentParser(description='Dry-run topic discovery and print the results')
    parser.add_argument('--out', default='topics-dry-run.json')
    parser.add_argument('--max-candidates', type=int, default=MAX_CANDIDATES)
    parser.add_argument('--max-new', type=int, default=MAX_NEW_TOPICS)
    args = parser.parse_args()

    new_topics, candidates = discover([], max_candidates=args.max_candidates, max_new=args.max_new)
    with open(args.out, 'w') as f:
        json.dump(dict(new_topics=new_topics, candidates=candidates), f, ensure_ascii=False, indent=2, default=str)
    print('\nACCEPTED {} / {} candidates (written to {})'.format(len(new_topics), len(candidates), args.out))


if __name__ == '__main__':
    main()
