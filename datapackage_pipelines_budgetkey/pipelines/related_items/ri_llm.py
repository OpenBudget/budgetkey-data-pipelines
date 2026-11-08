"""LLM steps of the linker: extracting mentions from a source document, and choosing among candidate targets.

Both go through common.llm (Gemini MODEL_FAST, JSON schema, cached on the prompt), so re-runs are free unless a
prompt or its input changed. Every extracted mention carries a short verbatim quote; mentions whose quote isn't
found in the source are dropped, so a link's excerpt always comes from the source itself.
"""
import json
import os
import re

from datapackage_pipelines_budgetkey.common import llm

from datapackage_pipelines_budgetkey.pipelines.related_items import ri_text as T

MODEL = os.environ.get('RELATED_ITEMS_MODEL') or llm.MODEL_FAST
THINKING = os.environ.get('RELATED_ITEMS_THINKING') or 'low'
MAX_SOURCE_CHARS = 60000

ORG_TYPES = ['ministry', 'government_unit', 'statutory_body', 'government_company', 'municipality', 'hospital',
             'university', 'health_fund', 'ngo', 'company', 'other']

EXTRACT_SCHEMA = {
    'type': 'object',
    'properties': {
        'organizations': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'name': {'type': 'string'},
                    'org_type': {'type': 'string', 'enum': ORG_TYPES},
                    'parent': {'type': ['string', 'null']},
                    'role': {'type': 'string', 'enum': ['subject', 'responsible', 'funded', 'contractor', 'mentioned']},
                    'quote': {'type': 'string'},
                    'parts': {'type': 'array', 'items': {'type': 'string'}},
                },
                'required': ['name', 'org_type', 'parent', 'role', 'quote', 'parts'],
            },
        },
        'programs': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'name': {'type': 'string'},
                    'kind': {'type': 'string', 'enum': ['budget_program', 'support_program']},
                    'ministry': {'type': ['string', 'null']},
                    'years': {'type': 'array', 'items': {'type': 'integer'}},
                    'quote': {'type': 'string'},
                    'parts': {'type': 'array', 'items': {'type': 'string'}},
                },
                'required': ['name', 'kind', 'ministry', 'years', 'quote', 'parts'],
            },
        },
    },
    'required': ['organizations', 'programs'],
}

EXTRACT_PROMPT = '''Below is {what}. List the specific organizations and the specific budget or support programs it
is about or names, so they can be linked to records of the Israeli state budget and organization registry.

Organizations: government ministries and their units, authorities, statutory bodies, government companies,
municipalities and regional councils, hospitals, universities, health funds, NGOs (עמותות) and companies.
- Only organizations named explicitly: "משרד הבריאות", "עיריית חיפה", "חברת נתיבי ישראל", "עמותת לתת". Not generic
  references ("משרדי הממשלה", "הרשויות המקומיות", "העירייה", "גופים ציבוריים", "בתי החולים").
- Never private persons, and not the State Comptroller's office itself when it only reports.
- name: the full name as it would appear in a registry, expanded from the text ("עיריית חיפה" for "עיריית חיפה"
  or "חיפה" when the text means the municipality). parent: the ministry a unit belongs to ("משרד האוצר" for
  "אגף התקציבים"), else null.
- role: subject (audited, or what the decision is about), responsible (must act on a finding or carry out the
  decision), funded (receives money or a budget), contractor (supplies goods or services), mentioned (other).

Programs: named state budget programs or allocations ("תכנית החומש לחברה הערבית", "סל קליטה", "תקציב פיתוח
הרשויות המקומיות") and support programs/grants (תמיכות, מענקים, "קול קורא") that the text discusses.
- Not general topics ("חינוך", "בריאות"), laws, or the comptroller's recommendations.
- ministry: the ministry whose budget it is, if known. years: the budget years the text refers to for it, if stated.

For every item give a quote: 3 to 20 consecutive words copied exactly from the text, containing the name.
parts: {parts_hint}
Return at most 40 organizations and 15 programs, the most significant ones.

{source}'''

DECIDE_SCHEMA = {
    'type': 'object',
    'properties': {
        'decisions': {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    'n': {'type': 'integer'},
                    'choice': {'type': ['string', 'null']},
                    'certainty': {'type': 'string', 'enum': ['certain', 'likely', 'unsure']},
                },
                'required': ['n', 'choice', 'certainty'],
            },
        },
    },
    'required': ['decisions'],
}

DECIDE_PROMPT = '''{what} names the items below. For each, choose the record it refers to from its candidates,
or null if none of them is that same item.

{guidance}

Answer "certain" only when the record is clearly the item meant (same body or program, not a related one, a
branch, a parent, or a namesake); "likely" when probably; "unsure" otherwise. A wrong choice is much worse than
null.

Source: {source_title} ({source_date})

{items}'''

ORG_GUIDANCE = '''The records are from budgetkey's organization registry. Government bodies are recorded under old
accounting names, as "<ministry>/<unit>" ("משרד הרווחה/המשרד הראשי" is the welfare ministry's headquarters, which
stands for the ministry as a whole; ministry names there are often outdated). A ministry's unit, branch, district or
hospital is a different record from the ministry. Municipalities: "עירית"/"עיריית" is a city, "מ. מ." a local
council, "מ. א." a regional council.'''

PROGRAM_GUIDANCE = '''The records are items of the Israeli state budget (chapter > domain > program, with the code)
and support programs (תמיכות) of government ministries, with their years. Choose the budget item whose budget is
the program meant, at the level that matches it best (a domain if the text means the whole domain), or the support
program that is the grant or support meant.'''


def _norm_ws(s):
    return re.sub(r'\s+', ' ', T.clean_quotes(s or '')).strip()


def find_quote(text_norm, quote):
    """Index of the quote in the (whitespace- and quote-normalized) text, tolerating a trimmed end; or -1."""
    q = _norm_ws(quote).strip(' .,;:"\'')
    if len(q) < 4:
        return -1
    i = text_norm.find(q)
    if i >= 0:
        return i
    words = q.split(' ')
    if len(words) >= 5:
        # Models sometimes extend a quote by a word or fix its punctuation; accept a long verbatim core.
        core = ' '.join(words[1:-1])
        return text_norm.find(core)
    return -1


def extract(what, source_text, parts_hint, usage=None):
    """Mentions in the source: (organizations, programs), each with its quote verified against the text."""
    prompt = EXTRACT_PROMPT.format(what=what, parts_hint=parts_hint, source=source_text[:MAX_SOURCE_CHARS])
    _, result = llm.complete(prompt, schema=EXTRACT_SCHEMA, model=MODEL, thinking=THINKING, usage=usage)
    text_norm = _norm_ws(source_text)
    out = {}
    for key in ('organizations', 'programs'):
        items = []
        for item in result.get(key) or []:
            if not (item.get('name') or '').strip():
                continue
            pos = find_quote(text_norm, item.get('quote') or '')
            item['verified'] = pos >= 0
            item['position'] = pos
            items.append(item)
        out[key] = items
    return out['organizations'], out['programs']


def decide(what, source_title, source_date, guidance, items, usage=None):
    """items: [dict(mention, context, candidates=[(key, label)])]. Returns {index: (key, certainty)} with the
    chosen candidate's key (None when the model chose none)."""
    if not items:
        return {}
    blocks = []
    for n, item in enumerate(items, 1):
        lines = ['  {}. {}'.format(chr(ord('a') + k), label) for k, (_, label) in enumerate(item['candidates'])]
        blocks.append('{}. "{}"{}\n  context: "{}"\n  candidates:\n{}'.format(
            n, item['mention'], ' ({})'.format(item['hint']) if item.get('hint') else '', item['context'],
            '\n'.join(lines)))
    prompt = DECIDE_PROMPT.format(what=what, guidance=guidance, source_title=source_title, source_date=source_date,
                                  items='\n\n'.join(blocks)) + \
        '\n\nAnswer with the candidate letter as choice (or null).'
    _, result = llm.complete(prompt, schema=DECIDE_SCHEMA, model=MODEL, thinking=THINKING, usage=usage)
    out = {}
    for d in result.get('decisions') or []:
        n = d.get('n')
        if not isinstance(n, int) or not 1 <= n <= len(items):
            continue
        choice = (d.get('choice') or '').strip().lower().rstrip('.')
        candidates = items[n - 1]['candidates']
        k = ord(choice[0]) - ord('a') if len(choice) == 1 else -1
        out[n - 1] = (candidates[k][0] if 0 <= k < len(candidates) else None, d.get('certainty'))
    return out


def dumps(x):
    return json.dumps(x, ensure_ascii=False, sort_keys=True)
