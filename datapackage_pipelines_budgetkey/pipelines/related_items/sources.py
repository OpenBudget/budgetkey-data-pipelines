"""Source adapters: turn each source's rows into the linker's documents (see linker.py for the shape)."""
import json

from datapackage_pipelines_budgetkey.pipelines.related_items import ri_text as T
from datapackage_pipelines_budgetkey.pipelines.related_items.linker import as_date


def _list(v):
    if isinstance(v, str):
        try:
            return json.loads(v) if v else []
        except ValueError:
            return []
    return v or []


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# --- State Comptroller ---------------------------------------------------------------------------------------

def comptroller_finding_text(f):
    lines = ['[{}] {}'.format(f['finding_id'], f.get('title') or '')]
    for key, label in (('description', 'ממצא'), ('recommendation', 'המלצה'), ('excerpt', 'מתוך הדוח')):
        if f.get(key):
            lines.append('{}: {}'.format(label, f[key]))
    bodies = _list(f.get('responsible_bodies'))
    if bodies:
        lines.append('גופים אחראים: ' + ', '.join(bodies))
    return '\n'.join(lines)


def comptroller_documents(audits, findings):
    """One document per Hebrew audit chapter; its findings are the parts."""
    by_chapter = {}
    for f in findings:
        by_chapter.setdefault(str(f['chapter_id']), []).append(f)
    for a in audits:
        if (a.get('language') or 'he') != 'he' or a.get('analysis_status') == 'not-hebrew':
            continue
        chapter_id = str(a['chapter_id'])
        fs = sorted(by_chapter.get(chapter_id, []), key=lambda f: _int(f.get('seq')) or 0)
        audited = _list(a.get('audited_bodies'))
        figures = _list(a.get('key_figures'))
        llm_text = '\n'.join(x for x in [
            'כותרת: ' + (a.get('title') or ''),
            'דוח: {} ({})'.format(a.get('publication_title') or '', a.get('publish_date') or ''),
            'גופים מבוקרים: ' + ', '.join(b.get('name') or '' for b in audited) if audited else '',
            'נושא הביקורת: ' + a['audit_subject'] if a.get('audit_subject') else '',
            'תקופת הביקורת: ' + a['audit_period'] if a.get('audit_period') else '',
            'תקציר: ' + a['summary'] if a.get('summary') else '',
            'נתונים מרכזיים: ' + '; '.join(json.dumps(k, ensure_ascii=False) if not isinstance(k, str) else k
                                            for k in figures) if figures else '',
            'ממצאים:' if fs else '',
        ] + [comptroller_finding_text(f) for f in fs] if x)
        yield dict(
            doc_id='state_comptroller_audits/' + chapter_id,
            kind='state_comptroller_audits',
            title=a.get('title'),
            date=as_date(a.get('publish_date')),
            text=a.get('text_md') or '',
            llm_text=llm_text,
            what='a State Comptroller audit report chapter ("{}", published {}): its summary and findings'.format(
                a.get('title'), a.get('publish_date')),
            parts=[dict(part_id=f['finding_id'], text=comptroller_finding_text(f)) for f in fs],
            parts_hint='the ids of the findings (in square brackets, e.g. "{}-3") where the item appears; empty '
                       'if only in the summary.'.format(chapter_id),
            hints=dict(
                bodies=[dict(code=b.get('code'), name=b.get('name'), relation='audited') for b in audited],
                years=(_int(a.get('audit_period_start_year')), _int(a.get('audit_period_end_year'))),
                llm=True,
                same_government_default=False,
            ),
        )


# --- Government decisions -----------------------------------------------------------------------------------

DECISION_TYPE = 'החלטות ממשלה'
# Published "decisions" of other kinds are mostly procedures, licenses and land deals of private people: scanned for
# references, but not read by the LLM.
LLM_TYPES = {DECISION_TYPE, 'החלטות ועדת שירות המדינה', 'מסמכי מדיניות', 'החלטות הנהלה'}
NO_LLM_TYPES = {'החלטות ועדת פטור'}


def decision_documents(rows):
    for r in rows:
        text = T.html_to_text(r.get('text'))
        if not text and not r.get('title'):
            continue
        policy_type = r.get('policy_type')
        date = as_date(r.get('doc_published_date')) or as_date(r.get('publish_date'))
        is_decision = policy_type == DECISION_TYPE
        what = 'an Israeli government decision (החלטת ממשלה {}, "{}", {})'.format(
            r.get('procedure_number_str') or '', r.get('title'), date) if is_decision else \
            'a publication of {} ({}, "{}", {})'.format(r.get('office') or 'an Israeli government office',
                                                       policy_type, r.get('title'), date)
        yield dict(
            doc_id='gov_decisions/' + r['id'],
            kind='gov_decisions',
            title=r.get('title'),
            date=date,
            text='{}\n{}'.format(r.get('title') or '', text),
            what=what,
            parts=[],
            hints=dict(
                llm=policy_type in LLM_TYPES and policy_type not in NO_LLM_TYPES and len(text) > 200,
                # Government decisions cite the government's own earlier decisions without a date.
                same_government_default=is_decision,
                exclude_decision=r['id'],
            ),
        )
