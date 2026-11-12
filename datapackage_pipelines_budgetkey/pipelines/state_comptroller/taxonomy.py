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

The audited bodies' names in the findings are free text, with spelling variants and old names of renamed bodies
("המשרד לאיכות הסביבה" / "המשרד להגנת הסביבה") that can show up side by side in one issue's list. Names that appear
together in a finding or an issue and look alike are judged in pairs (same body or not, and which name is current);
the same-body pairs are grouped, and `body_names` maps each name to its group's current name. The documents and the
dashboard apply it.

Outputs: `audit_taxonomy` (one row per audit, with the metadata the dashboard needs), `special_reports` (each special
publication's name, kind and event group), `body_names` (a body name -> its canonical name) and `taxonomy` (the
labels, which the search page's filter menus read).
"""
import collections
import datetime
import difflib
import hashlib
import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor

import dataflows as DF

import sc_common as sc
from datapackage_pipelines_budgetkey.common import llm

REPORTS_PATH = os.environ.get('REPORTS', '/var/datapackages/state_comptroller/reports')
CHAPTERS_PATH = os.environ.get('CHAPTERS', '/var/datapackages/state_comptroller/chapters')
ISSUES_PATH = os.environ.get('ISSUES', '/var/datapackages/state_comptroller/issues')
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
SHORT_NAME_MAX = 30
SHORT_SCHEMA = {'type': 'object', 'properties': {'items': {'type': 'array', 'items': {'type': 'object', 'properties': {
    'key': {'type': 'string'}, 'short_name': {'type': 'string'},
}, 'required': ['key', 'short_name']}}}, 'required': ['items']}
SHORT_PROMPT = '''לפניך שמות של דוחות מיוחדים של מבקר המדינה ושל האירועים שבהם עסקו. לכל אחד, תן שם קצר: השם המקובל
בציבור ובתקשורת, עד {max} תווים (למשל "שרפת הכרמל", "משט המרמרה", "פרשת המעונות", "ההתנתקות", "7 באוקטובר",
"מגפת הקורונה"). אם השם כבר קצר וברור, החזר אותו כמות שהוא.
השם הקצר נשען רק על השם והתיאור שלפניך, ניטרלי ועובדתי: אל תוסיף שמות של תיקים פליליים, חקירות או כינויים
שאינם בשם המקורי, ואל תקרא "פרשה" לדוח שהשם המלא שלו אינו מתאר פרשה. החזר פריט לכל key.

{events}'''


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


HEBREW = re.compile('[\u05d0-\u05ea]')


def sc_date(v):
    if isinstance(v, datetime.date):
        return v
    try:
        return datetime.date.fromisoformat(str(v)[:10])
    except ValueError:
        return None


def special_reports(publications, audits):
    """The special publications of the analysed years: named, classified and keyed by their event group.

    That includes the few whose chapters have no readable file (e.g. the 1994 report on the Turner affair, which has
    no file on the comptroller's site): they're named from their title alone."""
    by_pub = {}
    for a in audits:
        by_pub.setdefault(a.get('pub_id'), []).append(a)
    dates = [d for d in (sc_date(a.get('publish_date')) for a in audits) if d]
    first = min(dates) if dates else None

    def wanted(p):
        if p.get('publication_type') != 'special':
            return False
        if p['pub_id'] in by_pub:
            return True
        d = sc_date(p.get('publish_date'))
        return bool(first and d and d >= first and HEBREW.search(p.get('title') or ''))

    pubs = [p for p in publications if wanted(p)]
    with ThreadPoolExecutor(4) as ex:
        infos = list(ex.map(lambda p: name_report(p, sorted(by_pub.get(p['pub_id'], []), key=lambda a: a['chapter_id'])),
                            pubs))
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
    for r in reports:
        r['when'], r['event_when'] = normalize_when(r['when']), normalize_when(r['event_when'])
    add_short_names(reports)
    polish_descriptions(reports, by_pub)
    logging.info('Taxonomy: %d special reports in %d groups', len(reports), len({r['event'] for r in reports}))
    return reports


def add_short_names(reports):
    """Each event's colloquial name (`event_short_name`, for menus and cards), from one call over all the events.
    A name the model doesn't give, or gives too long, falls back to the full name. Two events with the same short name
    (e.g. two reports on Jerusalem years apart) get their years appended."""
    events = {}
    for r in sorted(reports, key=lambda r: (str(r['publish_date'] or ''), r['pub_id'])):
        events.setdefault(r['event'], dict(key=r['event'], name=r['event_name'], description=r['event_description'],
                                           year=str(r['publish_date'] or '')[:4]))
    shorts = {}
    if events:
        listing = json.dumps(sorted(events.values(), key=lambda e: int(e['key'])), ensure_ascii=False)
        try:
            _, out = llm.complete(SHORT_PROMPT.format(max=SHORT_NAME_MAX, events=listing), schema=SHORT_SCHEMA,
                                  model=llm.MODEL_FAST, temperature=0, usage=USAGE, thinking='low')
        except Exception:
            logging.exception('Taxonomy: shortening the event names failed')
            out = {}
        for it in (out or {}).get('items') or []:
            name = (it.get('short_name') or '').strip()
            if it.get('key') in events and name and len(name) <= SHORT_NAME_MAX + 10:
                shorts[it['key']] = name
    for key, e in events.items():
        shorts.setdefault(key, e['name'])
    counts = {}
    for name in shorts.values():
        counts[name] = counts.get(name, 0) + 1
    for key, name in shorts.items():
        if counts[name] > 1 and events[key]['year']:
            shorts[key] = '{} ({})'.format(name, events[key]['year'])
    for r in reports:
        r['event_short_name'] = shorts[r['event']]


POLISH_SCHEMA = {'type': 'object', 'properties': {'items': {'type': 'array', 'items': {'type': 'object', 'properties': {
    'key': {'type': 'string'}, 'description': {'type': 'string'},
}, 'required': ['key', 'description']}}}, 'required': ['items']}
POLISH_PROMPT = '''לפניך תיאורים של דוחות מיוחדים של מבקר המדינה, שיוצגו לקוראים בדף על דוחות המבקר. ערוך כל תיאור:
- משפט אחד או שניים, עד 35 מילים, בעברית תקנית וטבעית (שימוש עקבי ונכון במילות זיקה: "שבה", "שבהם" וכו').
- גוון את המבנה בין התיאורים: אל תפתח את כולם באותה צורה ואל תסיים את כולם ב"שבו בדק המבקר את...".
- אל תחזור על שם הדוח (name) מילה במילה; הוא מוצג מעליו.
- הישען רק על המידע שלפניך (השם, המועד, התיאור הקיים וכותרות הפרקים). אל תוסיף עובדות, תארים או הערכות.
החזר פריט לכל key.

{events}'''


def normalize_when(text):
    """One dash style for date ranges: an en dash without spaces ("יולי–אוגוסט 2014", "2020–2022")."""
    if not text:
        return text
    return re.sub(r'\s*[-–—]\s*', '–', text.strip())


def polish_descriptions(reports, chapters_by_pub):
    """Copy-edits every event's description in one call, for the dashboard's readers (variety, Hebrew usage, no
    repetition of the name). A description the model doesn't return keeps its original."""
    events = {}
    for r in sorted(reports, key=lambda r: r['pub_id']):
        e = events.setdefault(r['event'], dict(key=r['event'], name=r['event_name'], when=r['event_when'] or '',
                                               description=r['event_description'] or '', chapters=[]))
        e['chapters'] += [a.get('title') for a in chapters_by_pub.get(r['pub_id'], [])][:10 - len(e['chapters'])]
    if not events:
        return
    listing = json.dumps(sorted(events.values(), key=lambda e: int(e['key'])), ensure_ascii=False)
    try:
        _, out = llm.complete(POLISH_PROMPT.format(events=listing), schema=POLISH_SCHEMA, model=llm.MODEL_FAST,
                              temperature=0, usage=USAGE, thinking='low')
    except Exception:
        logging.exception('Taxonomy: polishing the event descriptions failed')
        out = {}
    polished = {it['key']: it['description'].strip() for it in (out or {}).get('items') or []
                if it.get('key') in events and (it.get('description') or '').strip()
                and len(it['description']) <= 400}
    for r in reports:
        r['event_description'] = polished.get(r['event'], r['event_description'])


PAIR_SCHEMA = {'type': 'object', 'properties': {'items': {'type': 'array', 'items': {'type': 'object', 'properties': {
    'id': {'type': 'integer'}, 'same': {'type': 'boolean'}, 'current': {'type': 'string', 'enum': ['a', 'b']},
}, 'required': ['id', 'same', 'current']}}}, 'required': ['items']}
PAIR_PROMPT = '''לפניך זוגות של שמות גופים שהופיעו יחד בממצא או בליקוי של מבקר המדינה. לכל זוג, קבע:
- same: האם שני השמות הם אותו גוף עצמו: הבדל כתיב, קיצור, או שם ישן ושם חדש של אותו גוף אחרי שינוי שם
  (למשל "המשרד לאיכות הסביבה" ו"המשרד להגנת הסביבה"). לא אותו גוף: יחידה וגוף האם שלה (אגף במשרד, בית חולים
  ברשת), שתי רשויות, שני בתי חולים או שני סניפים שונים, תפקיד בגוף לעומת הגוף.
- current: איזה משני השמות (a או b) הוא השם העדכני והמלא של הגוף. בזוג שאינו אותו גוף, a.
החזר פריט לכל id.

{pairs}'''
PAIR_BUCKETS = 12
# Words that every body of a kind shares ("עיריית X", "המועצה המקומית Y"), so they don't make two bodies look alike
SIGNIFICANT_STOP = {'משרד', 'המשרד', 'רשות', 'הרשות', 'אגף', 'חברה', 'החברה', 'ישראל', 'לישראל', 'בישראל', 'עיריית',
                    'מועצה', 'המועצה', 'מקומית', 'המקומית', 'אזורית', 'האזורית', 'איגוד', 'ערים', 'בית', 'חולים',
                    'מרכז', 'המרכז', 'רפואי', 'הרפואי', 'יחידת', 'היחידה', 'מחוז', 'ועדה', 'הוועדה', 'המקומית'}


def _words(name):
    """A name's significant words, without the prefixes ו and ה ("והתעשייה" = "התעשייה" = "תעשייה")."""
    words = set()
    for w in re.findall(r'[א-ת]+', name):
        if len(w) > 2 and w not in SIGNIFICANT_STOP:
            w = re.sub('^ו', '', w)
            words.add(re.sub('^ה', '', w) if len(w) > 3 else w)
    return words


def candidate_pairs(lists):
    """Pairs of different names that appear in the same list (an issue's or a finding's bodies) and look alike."""
    pairs = set()
    for names in lists:
        names = sorted({n.strip() for n in names or [] if isinstance(n, str) and n.strip()})
        for i, a in enumerate(names):
            for b in names[i + 1:]:
                ka, kb = sc.body_key(a), sc.body_key(b)
                if ka == kb:
                    pairs.add((a, b))
                    continue
                wa, wb = _words(a), _words(b)
                ratio = difflib.SequenceMatcher(None, ka, kb).ratio()
                if ratio >= 0.6 or len(wa & wb) / max(1, len(wa | wb)) >= 0.25:
                    pairs.add((a, b))
    return sorted(pairs)


def judge_pairs(pairs):
    """{(a, b): current name} for the pairs that are the same body. Pairs that differ only in spelling need no call.
    The rest go to Gemini in fixed hash buckets, so a new pair re-asks only its own bucket."""
    same = {}
    buckets = collections.defaultdict(list)
    for a, b in pairs:
        if sc.body_key(a) == sc.body_key(b):
            same[(a, b)] = None
        else:
            bucket = int(hashlib.md5((a + '|' + b).encode('utf8')).hexdigest(), 16) % PAIR_BUCKETS
            buckets[bucket].append((a, b))

    def ask(bucket_pairs):
        listing = json.dumps([dict(id=i, a=a, b=b) for i, (a, b) in enumerate(bucket_pairs)], ensure_ascii=False)
        try:
            _, out = llm.complete(PAIR_PROMPT.format(pairs=listing), schema=PAIR_SCHEMA, model=llm.MODEL_FAST,
                                  temperature=0, usage=USAGE, thinking='low')
        except Exception:
            logging.exception('Taxonomy: judging body-name pairs failed')
            return {}
        answers = {}
        for it in (out or {}).get('items') or []:
            if isinstance(it.get('id'), int) and 0 <= it['id'] < len(bucket_pairs) and it.get('same') is True:
                a, b = bucket_pairs[it['id']]
                answers[(a, b)] = b if it.get('current') == 'b' else a
        return answers
    with ThreadPoolExecutor(4) as ex:
        for answers in ex.map(ask, [buckets[k] for k in sorted(buckets)]):
            same.update(answers)
    return same


def canonical_body_names(lists, counts):
    """{name: canonical name}: the names judged to be the same body are grouped (union-find), and each group takes the
    name judged current most often, then the most frequent one."""
    same = judge_pairs(candidate_pairs(lists))
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    votes = collections.Counter()
    for (a, b), current in same.items():
        parent[find(a)] = find(b)
        if current:
            votes[current] += 1
    groups = collections.defaultdict(list)
    for name in list(parent):
        groups[find(name)].append(name)
    mapping = {}
    for names in groups.values():
        best = max(names, key=lambda n: (votes[n], counts.get(n, 0), n))
        mapping.update({n: best for n in names if n != best})
    logging.info('Taxonomy: %d body-name pairs judged the same body, %d names mapped to another',
                 len(same), len(mapping))
    return mapping


def load_body_lists(chapters_path, issues_path):
    """The bodies' names as they appear together: each finding's responsible bodies and each issue's bodies."""
    lists = [r.get('responsible_bodies') or [] for r in DF.Flow(
        DF.load(os.path.join(chapters_path, 'datapackage.json'), resources=['findings']),
        DF.select_fields(['responsible_bodies'])).results()[0][0]]
    try:
        lists += [r.get('bodies') or [] for r in DF.Flow(
            DF.load(os.path.join(issues_path, 'datapackage.json'), resources=['issues']),
            DF.select_fields(['bodies'])).results()[0][0]]
    except Exception:
        logging.warning('No issues at %s: body names are matched within findings only', issues_path)
    return lists


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
    ('event_when', 'string'), ('event_description', 'string'), ('event_short_name', 'string'),
]
BODY_FIELDS = [('name', 'string'), ('canonical', 'string'), ('mentions', 'integer')]
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


def run(reports_path, chapters_path, issues_path, limit=None):
    audits = load_audits(chapters_path)
    if limit:
        audits = audits[:limit]
    reports = special_reports(load_publications(reports_path), audits)
    rows = classify(audits, {r['pub_id']: r['event'] for r in reports})
    lists = load_body_lists(chapters_path, issues_path)
    counts = collections.Counter(b.strip() for names in lists for b in names if isinstance(b, str) and b.strip())
    mapping = canonical_body_names(lists, counts)
    bodies = [dict(name=n, canonical=mapping.get(n, n), mentions=counts[n]) for n in sorted(counts)]
    return rows, reports, bodies


def output_flow(rows, reports, bodies):
    return DF.Flow(
        sc.typed_resource(rows, 'audit_taxonomy', AUDIT_TAXONOMY_FIELDS, ['chapter_id']),
        sc.typed_resource(reports, 'special_reports', REPORT_FIELDS, ['pub_id']),
        sc.typed_resource(bodies, 'body_names', BODY_FIELDS, ['name']),
        sc.typed_resource(label_rows(), 'taxonomy', LABEL_FIELDS, ['axis', 'key']),
    )


def flow(*_):
    from datapackage_pipelines_budgetkey.common.dump_to_sql_atomic import dump_to_sql_atomic
    rows, reports, bodies = run(REPORTS_PATH, CHAPTERS_PATH, ISSUES_PATH)
    return DF.Flow(
        output_flow(rows, reports, bodies),
        DF.update_resource(None, **{'dpp:streaming': True}),
        DF.dump_to_path(OUT_PATH),
        dump_to_sql_atomic(dict(
            state_comptroller_audit_taxonomy={'resource-name': 'audit_taxonomy'},
            state_comptroller_special_reports={'resource-name': 'special_reports'},
            state_comptroller_body_names={'resource-name': 'body_names'},
            state_comptroller_taxonomy={'resource-name': 'taxonomy'},
        )),
    )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    for noisy in ('httpx', 'httpcore', 'google_genai', 'urllib3'):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    rows, reports, bodies = run(REPORTS_PATH, CHAPTERS_PATH, ISSUES_PATH, int(os.environ.get('LIMIT') or 0) or None)
    DF.Flow(output_flow(rows, reports, bodies),
            DF.dump_to_path(os.environ.get('OUT', 'state_comptroller_taxonomy_out'))).process()
