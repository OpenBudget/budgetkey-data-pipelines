"""A closed taxonomy for the State Comptroller's audits: each analyzed chapter's policy domains and audit lenses.

The chapter analysis gives every audit free-text `topics` (thousands of distinct strings), which serve search ranking
but can't be filtered on or counted. This step tags each audit with two fixed lists:
- domains: the policy area the audit is about (health, defence, local government, ...): a primary one and an optional
  secondary one;
- lenses: what kind of problem it examines (procurement, integrity, IT and cyber, ...): one to three.

Gemini Flash classifies the chapters one publication at a time (a published report's chapters don't change, so a
batch's prompt, and its cached answer, stay the same from run to run). Unknown keys from the model are dropped.

Special reports are also named and classified, one call per publication: an event (the Carmel fire, a war), a
theme, a recurring report or a follow-up. Reports about the same event (the many on the 7 October war) are then
grouped by one more call, and every special report's audits get the group's key (`event`), which the search page
filters on.

Outputs: `audit_taxonomy` (one row per audit, with the metadata the dashboard needs), `special_reports` (each special
publication's name, kind and event group) and `taxonomy` (the labels, which the search page's filter menus read).
"""
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor

import dataflows as DF

import sc_common as sc
from datapackage_pipelines_budgetkey.common import llm

REPORTS_PATH = os.environ.get('REPORTS', '/var/datapackages/state_comptroller/reports')
CHAPTERS_PATH = os.environ.get('CHAPTERS', '/var/datapackages/state_comptroller/chapters')
OUT_PATH = '/var/datapackages/state_comptroller/taxonomy'
SEARCH_URL = 'https://next.obudget.org/s?q=&dd=state_comptroller&{param}={key}'
INDEXED_STATUSES = ('ok', 'low-verification', 'partial-text', 'no-findings')
BATCH = 30
USAGE = llm.Usage()

# key: (label, what it covers). The order is the display order.
DOMAINS = {
    'defense': ('ביטחון וצבא', 'צה"ל, משרד הביטחון, התעשיות הביטחוניות, שירות צבאי ומילואים'),
    'police_justice': ('משטרה, משפט ואכיפה', 'משטרה, שירות בתי הסוהר, בתי המשפט, הפרקליטות, אכיפת החוק'),
    'emergency': ('חירום ועורף', 'היערכות לחירום, עורף, כבאות והצלה, מלחמות ואסונות, פינוי אוכלוסייה'),
    'foreign': ('יחסי חוץ', 'משרד החוץ, נציגויות, הסברה, יחסים בין-לאומיים'),
    'health': ('בריאות', 'משרד הבריאות, בתי חולים, קופות חולים, בריאות הציבור'),
    'welfare': ('רווחה וביטחון סוציאלי', 'רווחה, ביטוח לאומי, קשישים, אנשים עם מוגבלות, ניצולי שואה, אוכלוסיות פגיעות'),
    'education': ('חינוך והשכלה גבוהה', 'מערכת החינוך, השכלה גבוהה, מדע ומחקר'),
    'culture_sport_religion': ('תרבות, ספורט ודת', 'תרבות, ספורט, שירותי דת, מורשת, קבורה'),
    'transport': ('תחבורה', 'כבישים, רכבות, תחבורה ציבורית, תעופה, ספנות, בטיחות בדרכים'),
    'energy_water': ('אנרגיה ומים', 'חשמל, גז, דלק, מים וביוב'),
    'environment': ('איכות הסביבה', 'זיהום, פסולת, שמירת טבע, אקלים'),
    'housing_land_planning': ('דיור, קרקע ותכנון', 'דיור, מקרקעי ישראל, תכנון ובנייה, התחדשות עירונית'),
    'agriculture': ('חקלאות ומזון', 'חקלאות, ביטחון תזונתי, וטרינריה'),
    'economy_finance': ('כלכלה, מסים ופיננסים', 'מסים, תקציב המדינה, האוצר, בנקים ושוק ההון, תעשייה, תעסוקה, תחרות'),
    'local_gov': ('שלטון מקומי', 'רשויות מקומיות, מועצות אזוריות, תאגידים עירוניים'),
    'gov_companies': ('חברות ממשלתיות', 'חברות ממשלתיות ותאגידים סטטוטוריים'),
    'communications_digital': ('תקשורת ודיגיטל', 'תקשורת, דואר, שידור, דיגיטל ממשלתי'),
    'immigration_population': ('עלייה, אוכלוסין והגירה', 'עלייה וקליטה, רשות האוכלוסין, עובדים זרים, מבקשי מקלט'),
    'minorities_periphery': ('מיעוטים ופריפריה',
                             'החברה הערבית, הבדואים, הדרוזים, הפריפריה, מזרח ירושלים, יהודה ושומרון'),
    'governance': ('מטה הממשלה ושירות המדינה', 'משרד ראש הממשלה, נציבות שירות המדינה, הכנסת, בחירות ומפלגות'),
}
LENSES = {
    'procurement': ('מכרזים ורכש', 'מכרזים, התקשרויות, רכש, פטור ממכרז'),
    'integrity': ('טוהר המידות וניגוד עניינים', 'טוהר המידות, ניגוד עניינים, מינויים פוליטיים, שחיתות'),
    'budget_finance': ('ניהול תקציבי וכספי', 'ניהול התקציב, הוצאה, גבייה, דיווח כספי'),
    'hr': ('כוח אדם ושכר', 'כוח אדם, שכר, תנאי העסקה, מינויים'),
    'it_cyber': ('מערכות מידע, סייבר ופרטיות', 'מערכות מידע, הגנת סייבר, אבטחת מידע, הגנת הפרטיות'),
    'regulation_enforcement': ('רגולציה ואכיפה', 'רגולציה, פיקוח ואכיפה על גורמים מחוץ לממשלה'),
    'service_rights': ('שירות לציבור ומיצוי זכויות', 'השירות לציבור, מיצוי זכויות, נגישות'),
    'infrastructure_projects': ('פרויקטים ותשתיות', 'תכנון וביצוע של פרויקטי תשתית ובינוי'),
    'emergency_readiness': ('היערכות לחירום', 'מוכנות לחירום, לאסון ולמלחמה'),
    'policy_planning': ('מדיניות ותכנון', 'תכנון אסטרטגי, קביעת מדיניות ויעדים, הערכת תוצאות'),
    'governance_mgmt': ('ממשל תאגידי ובקרה', 'ממשל תאגידי, דירקטוריונים, מינהל תקין, ביקורת ובקרה פנימית'),
    'equality_social': ('שוויון וצמצום פערים', 'צמצום פערים, שוויון, אוכלוסיות מוחלשות'),
    'climate_sustainability': ('אקלים וקיימות', 'משבר האקלים, קיימות, אנרגיה מתחדשת'),
    'data_transparency': ('שקיפות ונתונים', 'שקיפות, איסוף נתונים, חופש המידע'),
}
# The search page's filter menu (URL parameter) for each axis, and the ES field it filters on.
# Special reports are filtered with the 'event' menu, on the audits' `event` field.
AXES = {'domain': (DOMAINS, 'domains'), 'lens': (LENSES, 'lenses')}

SCHEMA = {'type': 'object', 'properties': {'items': {'type': 'array', 'items': {'type': 'object', 'properties': {
    'id': {'type': 'integer'},
    'domain': {'type': 'string', 'enum': list(DOMAINS)},
    'domain2': {'type': 'string', 'enum': list(DOMAINS) + ['none']},
    'lenses': {'type': 'array', 'items': {'type': 'string', 'enum': list(LENSES)}},
}, 'required': ['id', 'domain', 'domain2', 'lenses']}}}, 'required': ['items']}

PROMPT = '''לפניך פרקים מדוח של מבקר המדינה. סווג כל פרק לפי שתי רשימות סגורות:
- domain: התחום המדיני העיקרי שבו עוסק הפרק (אחד בלבד). domain2: תחום משני, אם יש כזה, אחרת none.
- lenses: אחת עד שלוש "עדשות" ביקורת: סוג הסוגיה שהביקורת בוחנת. בחר רק עדשות שהן מרכזיות בפרק.
סווג לפי מה שהביקורת בודקת בפועל ולא לפי שם הגוף המבוקר בלבד
(ביקורת על מכרזים במשרד הבריאות: domain=health, lenses כוללות procurement).
החזר פריט אחד לכל פרק, עם ה-id שלו.

תחומים:
{domains}

עדשות:
{lenses}

הפרקים (JSON):
{chapters}'''


def _listing(labels):
    return '\n'.join('{}: {} ({})'.format(k, label, about) for k, (label, about) in labels.items())


def indexed(row):
    return row.get('language') == 'he' and row.get('analysis_status') in INDEXED_STATUSES


def batches(audits):
    """The audits in batches of one publication's chapters, in their order, so a batch never changes."""
    by_pub = {}
    for a in audits:
        by_pub.setdefault(a.get('pub_id'), []).append(a)
    for pub in sorted(by_pub, key=lambda p: (p is None, p)):
        chapters = sorted(by_pub[pub], key=lambda a: (a.get('chapter_order') or 0, a['chapter_id']))
        for i in range(0, len(chapters), BATCH):
            yield chapters[i:i + BATCH]


def chapter_brief(a):
    return dict(id=a['chapter_id'], title=a.get('title'), report=a.get('publication_title'),
                body=a.get('main_body'), subject=a.get('audit_subject'),
                summary=(a.get('summary') or '')[:500], topics=a.get('topics') or [])


def classify_batch(batch):
    prompt = PROMPT.format(domains=_listing(DOMAINS), lenses=_listing(LENSES),
                           chapters=json.dumps([chapter_brief(a) for a in batch], ensure_ascii=False))
    try:
        _, out = llm.complete(prompt, schema=SCHEMA, model=llm.MODEL_FAST, temperature=0, usage=USAGE, thinking='low')
    except Exception:
        logging.exception('Taxonomy: classification failed for chapters %s', [a['chapter_id'] for a in batch])
        return {}
    ids = {a['chapter_id'] for a in batch}
    return {it['id']: it for it in (out or {}).get('items') or [] if isinstance(it, dict) and it.get('id') in ids}


def tags(item):
    """(domain, domains, lenses) from the model's answer, keeping only known keys."""
    if not item:
        return None, [], []
    domain = item.get('domain') if item.get('domain') in DOMAINS else None
    domains = [d for d in (domain, item.get('domain2')) if d in DOMAINS]
    lenses = [k for k in item.get('lenses') or [] if k in LENSES]
    return domain, list(dict.fromkeys(domains)), list(dict.fromkeys(lenses))[:3]


EVENT_KINDS = {
    'event': 'אירוע: אסון, מלחמה, מבצע צבאי, משבר או פרשה מסוימת',
    'thematic': 'נושא שנבדק לעומק, שאינו אירוע מסוים',
    'recurring': 'דוח שגרתי שמתפרסם מדי תקופה (מימון מפלגות, בחירות, שלטון מקומי)',
    'follow_up': 'ביקורת מעקב על דוחות קודמים',
    'other': 'אחר',
}
REPORT_SCHEMA = {'type': 'object', 'properties': {
    'kind': {'type': 'string', 'enum': list(EVENT_KINDS)},
    'name': {'type': 'string'},
    'when': {'type': 'string'},
    'description': {'type': 'string'},
}, 'required': ['kind', 'name', 'when', 'description']}
REPORT_PROMPT = '''לפניך דוח מיוחד של מבקר המדינה: כותרתו, מועד פרסומו וכותרות פרקיו.
- kind: סוג הדוח, אחד מאלה: {kinds}
- name: שם קצר וברור לנושא הדוח, כפי שהציבור מכיר אותו
  (למשל "השרפה בכרמל", "מגפת הקורונה", "מלחמת חרבות ברזל"), בלי "דוח מיוחד".
  בדוח על אירוע, השם הוא שם האירוע עצמו.
- when: אם הדוח עוסק באירוע, מתי התרחש (למשל "דצמבר 2010"); אחרת מחרוזת ריקה.
- description: משפט אחד על מה שנבדק.

{report}'''
GROUP_SCHEMA = {'type': 'object', 'properties': {'groups': {'type': 'array', 'items': {'type': 'object', 'properties': {
    'name': {'type': 'string'}, 'when': {'type': 'string'}, 'description': {'type': 'string'},
    'ids': {'type': 'array', 'items': {'type': 'integer'}},
}, 'required': ['name', 'when', 'description', 'ids']}}}, 'required': ['groups']}
GROUP_PROMPT = '''לפניך דוחות מיוחדים של מבקר המדינה שעוסקים באירועים. קבץ יחד דוחות שעוסקים באותו אירוע
(למשל כל הדוחות על מלחמת חרבות ברזל וטבח 7 באוקטובר, או כל הדוחות על מגפת הקורונה). דוח שאירועו אינו משותף לאחרים
הוא קבוצה בפני עצמו. כל id מופיע בקבוצה אחת בדיוק.
לכל קבוצה: name (שם האירוע), when (מתי התרחש), description (משפט אחד על האירוע ועל מה שהמבקר בדק בו), ids.

{reports}'''


def name_report(pub, chapters):
    report = json.dumps(dict(title=pub.get('title'), published=str(pub.get('publish_date') or ''),
                             chapters=[a.get('title') for a in chapters][:40]), ensure_ascii=False)
    try:
        _, info = llm.complete(REPORT_PROMPT.format(kinds=json.dumps(EVENT_KINDS, ensure_ascii=False), report=report),
                               schema=REPORT_SCHEMA, model=llm.MODEL_FAST, temperature=0, usage=USAGE, thinking='low')
    except Exception:
        logging.exception('Taxonomy: naming special report %s failed', pub['pub_id'])
        info = {}
    return info or {}


def group_events(reports):
    """{pub_id: its group (ids, name, when, description)} for the reports about events. A report the model leaves out
    is a group of its own."""
    events = sorted((r for r in reports if r['kind'] == 'event'), key=lambda r: r['pub_id'])
    groups = {}
    if len(events) > 1:
        listing = json.dumps([dict(id=r['pub_id'], name=r['name'], when=r['when'], title=r['title'])
                              for r in events], ensure_ascii=False)
        try:
            _, out = llm.complete(GROUP_PROMPT.format(reports=listing), schema=GROUP_SCHEMA, model=llm.MODEL_FAST,
                                  temperature=0, usage=USAGE, thinking='low')
        except Exception:
            logging.exception('Taxonomy: grouping the event reports failed')
            out = {}
        known = {r['pub_id'] for r in events}
        for g in (out or {}).get('groups') or []:
            ids = [i for i in g.get('ids') or [] if i in known and i not in groups]
            for i in ids:
                groups[i] = dict(ids=ids, name=g.get('name'), when=g.get('when'), description=g.get('description'))
    return groups


def special_reports(publications, audits):
    """The special publications that have analysed audits: named, classified and keyed by their event group."""
    by_pub = {}
    for a in audits:
        by_pub.setdefault(a.get('pub_id'), []).append(a)
    pubs = [p for p in publications if p.get('publication_type') == 'special' and p['pub_id'] in by_pub]
    with ThreadPoolExecutor(4) as ex:
        infos = list(ex.map(lambda p: name_report(p, sorted(by_pub[p['pub_id']], key=lambda a: a['chapter_id'])), pubs))
    reports = []
    for p, info in zip(pubs, infos):
        reports.append(dict(pub_id=p['pub_id'], title=p.get('title'), publish_date=p.get('publish_date'),
                            page_url=p.get('page_url'),
                            kind=info.get('kind') if info.get('kind') in EVENT_KINDS else 'other',
                            name=info.get('name') or p.get('title'), when=info.get('when') or None,
                            description=info.get('description')))
    groups = group_events(reports)
    for r in reports:
        g = groups.get(r['pub_id'])
        # The key is the group's first publication: stable as long as that publication stays in the group
        r['event'] = str(min(g['ids'])) if g else str(r['pub_id'])
        r['event_name'] = (g or {}).get('name') or r['name']
        r['event_when'] = (g or {}).get('when') or r['when']
        r['event_description'] = (g or {}).get('description') or r['description']
    logging.info('Taxonomy: %d special reports in %d groups', len(reports), len({r['event'] for r in reports}))
    return reports


def classify(audits, events=None, workers=4):
    """The audits' taxonomy rows. events: {pub_id: event key} of the special reports."""
    events = events or {}
    items = {}
    with ThreadPoolExecutor(workers) as ex:
        for answer in ex.map(classify_batch, list(batches(audits))):
            items.update(answer)
    rows = []
    for a in audits:
        domain, domains, lenses = tags(items.get(a['chapter_id']))
        d = a.get('publish_date')
        rows.append(dict(
            chapter_id=a['chapter_id'], pub_id=a.get('pub_id'), title=a.get('title'),
            publication_title=a.get('publication_title'), publication_type=a.get('publication_type'),
            follow_up=a.get('follow_up'), publish_date=d, year=d.year if d else None,
            n_findings=a.get('n_findings'), domain=domain, domains=domains, lenses=lenses,
            event=events.get(a.get('pub_id')),
        ))
    missing = sum(1 for r in rows if not r['domain'])
    logging.info('Taxonomy: %d audits, %d without a domain; usage %s', len(rows), missing, USAGE.by_model)
    return rows


def label_rows():
    rows = []
    for axis, (labels, field) in AXES.items():
        for order, (key, (label, about)) in enumerate(labels.items()):
            rows.append(dict(axis=axis, key=key, label=label, description=about, field=field, display_order=order,
                             search_url=SEARCH_URL.format(param=axis, key=key)))
    return rows


AUDIT_TAXONOMY_FIELDS = [
    ('chapter_id', 'integer'), ('pub_id', 'integer'), ('title', 'string'), ('publication_title', 'string'),
    ('publication_type', 'string'), ('follow_up', 'string'), ('publish_date', 'date'), ('year', 'integer'),
    ('n_findings', 'integer'), ('domain', 'string'), ('domains', 'array'), ('lenses', 'array'), ('event', 'string'),
]
REPORT_FIELDS = [
    ('pub_id', 'integer'), ('title', 'string'), ('publish_date', 'date'), ('page_url', 'string'), ('kind', 'string'),
    ('name', 'string'), ('when', 'string'), ('description', 'string'), ('event', 'string'), ('event_name', 'string'),
    ('event_when', 'string'), ('event_description', 'string'),
]
LABEL_FIELDS = [
    ('axis', 'string'), ('key', 'string'), ('label', 'string'), ('description', 'string'), ('field', 'string'),
    ('display_order', 'integer'), ('search_url', 'string'),
]


def load_audits(path):
    fields = ['chapter_id', 'pub_id', 'chapter_order', 'title', 'publication_title', 'publication_type', 'main_body',
              'audit_subject', 'summary', 'topics', 'follow_up', 'publish_date', 'n_findings', 'language',
              'analysis_status']
    rows = DF.Flow(DF.load(os.path.join(path, 'datapackage.json'), resources=['audits']),
                   DF.select_fields(fields)).results()[0][0]
    return [r for r in rows if indexed(r)]


def load_publications(path):
    fields = ['pub_id', 'title', 'publication_type', 'publish_date', 'page_url']
    return DF.Flow(DF.load(os.path.join(path, 'datapackage.json'), resources=['publications']),
                   DF.select_fields(fields)).results()[0][0]


def run(reports_path, chapters_path, limit=None):
    audits = load_audits(chapters_path)
    if limit:
        audits = audits[:limit]
    reports = special_reports(load_publications(reports_path), audits)
    rows = classify(audits, {r['pub_id']: r['event'] for r in reports})
    return rows, reports


def output_flow(rows, reports):
    return DF.Flow(
        sc.typed_resource(rows, 'audit_taxonomy', AUDIT_TAXONOMY_FIELDS, ['chapter_id']),
        sc.typed_resource(reports, 'special_reports', REPORT_FIELDS, ['pub_id']),
        sc.typed_resource(label_rows(), 'taxonomy', LABEL_FIELDS, ['axis', 'key']),
    )


def flow(*_):
    from datapackage_pipelines_budgetkey.common.dump_to_sql_atomic import dump_to_sql_atomic
    rows, reports = run(REPORTS_PATH, CHAPTERS_PATH)
    return DF.Flow(
        output_flow(rows, reports),
        DF.update_resource(None, **{'dpp:streaming': True}),
        DF.dump_to_path(OUT_PATH),
        dump_to_sql_atomic(dict(
            state_comptroller_audit_taxonomy={'resource-name': 'audit_taxonomy'},
            state_comptroller_special_reports={'resource-name': 'special_reports'},
            state_comptroller_taxonomy={'resource-name': 'taxonomy'},
        )),
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    for noisy in ('httpx', 'httpcore', 'google_genai', 'urllib3'):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    rows, reports = run(REPORTS_PATH, CHAPTERS_PATH, int(os.environ.get('LIMIT') or 0) or None)
    DF.Flow(output_flow(rows, reports),
            DF.dump_to_path(os.environ.get('OUT', 'state_comptroller_taxonomy_out'))).process()
