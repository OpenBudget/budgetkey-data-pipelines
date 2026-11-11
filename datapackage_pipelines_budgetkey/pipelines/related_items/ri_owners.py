"""Structural links: a ministry or authority -> its own budget chapters (relation `budget_owner`).

No document says "chapter 0024 is the Ministry of Health's budget", so the linker never finds it; bodies.csv does:
its `budget_chapters` are, per body, the chapters whose budget is the body's own. But for a unit - a government
hospital, a police district, an office within the PMO - they're its ministry's chapters, so a chapter is linked only
from its owner:
1. the bodies whose entity is a headquarters ("משרד הבריאות/המשרד הראשי") and list the chapter;
2. else those whose name matches the chapter's title ("הכנסת", "רשות האוכלוסין", "הועדה לאנרגיה אטומית");
3. else the one body listing it, when its entity is not a unit of another ("ועדת הבחירות המרכזית לכנסת");
4. else a body that owns another chapter by 1-3 and lists this one too (an authority's development chapter).
And the reviewed co-owners in CO_OWNERS, for a chapter two ministries share that the rules give to one.
A chapter none of these fit has no owner: shared chapters (0054 רשויות פיקוח), the government hospitals' (0092-0094),
and the ministries whose headquarters has no entity of its own (Defense, National Security).

The link goes from the body's entity (source_kind `entities`, no date) to the chapter's budget doc: the connected item
the chapter's latest (code, year) belongs to, as for budget codes found in text.
"""
import hashlib
from collections import defaultdict

from datapackage_pipelines_budgetkey.pipelines.related_items import ri_text as T
from datapackage_pipelines_budgetkey.pipelines.related_items.linker import VERSION

RELATION = 'budget_owner'
METHOD = 'structural:bodies'
TITLE_MATCH = 90  # token_set_ratio of a body's name and a chapter's title (0-100)
# Reviewed (Adam, 2026-11-11): {chapter: [entity ids]} owning it besides the rules' owners.
CO_OWNERS = {
    '0019': ['500106646'],  # מדע, תרבות וספורט: משרד התרבות והספורט, with the science ministry
    '0036': ['500107537'],  # תעסוקה: משרד העבודה, with משרד הכלכלה והתעשייה
}


def is_headquarters(entity_name):
    # Registry names are cut at 32 characters: "משרד המדע והטכנולוגיה/המשרד הראש".
    return 'המשרד הראש' in (entity_name or '')


def is_unit(entity_name):
    """A registry entity named under another: "משרד האוצר/רשות המסים", "צה"ל/מפקדת פיקוד העורף"."""
    return '/' in (entity_name or '') and not is_headquarters(entity_name)


def _matches(body, title):
    from rapidfuzz import fuzz
    title = T.normalize_name(title)
    names = [body['name'], (body.get('entity_name') or '').split('/')[-1]]
    return bool(title) and any(fuzz.token_set_ratio(T.normalize_name(n), title) >= TITLE_MATCH for n in names if n)


def owners(bodies, titles, co_owners=None):
    """{chapter: [body]} for the chapters in `titles` ({chapter code: title}); `bodies` as ri_entities.load_bodies
    gives them. One body per entity."""
    co_owners = CO_OWNERS if co_owners is None else co_owners
    listing = defaultdict(list)
    seen = set()
    by_entity = {}
    for body in bodies:
        if body.get('status') != 'ok' or not body.get('entity_id'):
            continue
        by_entity.setdefault(body['entity_id'], body)
        for chapter in body.get('budget_chapters') or []:
            if chapter in titles and (chapter, body['entity_id']) not in seen:
                seen.add((chapter, body['entity_id']))
                listing[chapter].append(body)
    out = {}
    for chapter, bodies_ in listing.items():
        found = [b for b in bodies_ if is_headquarters(b.get('entity_name'))] or \
            [b for b in bodies_ if _matches(b, titles[chapter])] or \
            [b for b in bodies_ if len(bodies_) == 1 and not is_unit(b.get('entity_name'))]
        if found:
            out[chapter] = found
    owning = {b['entity_id'] for found in out.values() for b in found}
    for chapter, bodies_ in listing.items():
        if chapter not in out:
            found = [b for b in bodies_ if b['entity_id'] in owning]
            if found:
                out[chapter] = found
    for chapter, entity_ids in co_owners.items():
        if chapter not in titles:
            continue
        found = out.setdefault(chapter, [])
        for entity_id in entity_ids:
            if entity_id in by_entity and all(b['entity_id'] != entity_id for b in found):
                found.append(by_entity[entity_id])
    return {chapter: found for chapter, found in out.items() if found}


def chapter_items(budget):
    """{chapter code: (item, year)}: the connected item of each chapter's latest year, from a ri_budget.BudgetIndex."""
    latest = {}
    for code, year in budget.by_code_year:
        if len(code) == 4 and year > latest.get(code, -1):
            latest[code] = year
    return {code: (budget.by_code_year[(code, year)], year) for code, year in latest.items()}


def owner_links(bodies, budget):
    """related_items rows: each chapter's owners -> the chapter's budget doc."""
    chapters = chapter_items(budget)
    titles = {code: item['years'].get(year, (code, item['title']))[1] for code, (item, year) in chapters.items()}
    rows = {}
    for chapter, found in sorted(owners(bodies, titles).items()):
        item, year = chapters[chapter]
        target = 'budget/{}/{}'.format(item['code'], item['year'])
        for body in found:
            source = 'org/{}/{}'.format(body['entity_kind'], body['entity_id'])
            if (source, target) in rows:
                continue
            rows[(source, target)] = dict(
                link_id=hashlib.sha1('|'.join([source, '', target]).encode('utf8')).hexdigest()[:16],
                source_doc_id=source, source_kind='entities', source_part_id=None, source_title=body['name'],
                source_date=None, target_doc_id=target, target_kind='budget', target_title=titles[chapter],
                target_year=year, target_code=chapter, relation=RELATION, mention=None, excerpt=None,
                confidence=1.0, method=METHOD, linker_version=VERSION)
    return list(rows.values())
