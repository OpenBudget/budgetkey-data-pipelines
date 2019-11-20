from datetime import datetime
import dataflows as DF


now = datetime.now()
today = now.date()


def process_row(row):
    tips = row.setdefault('actionable_tips', [])
    claim_date = row.get('claim_date')
    decision = row.get('decision')
    if claim_date:
        if now < claim_date:
            row['decision'] = 'פתוח'
        else:
            row['decision'] = decision or 'סגור'
    else:
        publication_date = row.get('start_date') or row.get('__created_at')
        if isinstance(publication_date, datetime):
            publication_date = publication_date.date()
        if not decision or decision.startswith('FILLER'):
            if publication_date:
                tips.append((
                    'לא הצלחנו לאתר במקור המידע הממשלתי את התאריך בו נסגרת ההרשמה. לבירור, מומלץ לבדוק בפרסום המקורי או ליצור קשר עם האחרא/ית',
                    None
                ))
                if (today - publication_date).days < 30:
                    row['decision'] = 'חדש'
                else:
                    row['decision'] = 'לא ידוע'
            else:
                # Ensure this will get filled later
                row['decision'] = 'FILLER' + now.isoformat()
    return row


def process(package):
    fields = package.pkg.descriptor['resources'][0]['schema']['fields']
    names = [f['name'] for f in fields]
    if 'actionable_tips' not in names:
        fields.append({
            'name': 'actionable_tips',
            'type': 'array',
            'es:itemType': 'object',
            'es:index': False
        })
    assert len(package.pkg.descriptor['resources']) == 1
    yield package.pkg
    for resource in package:
        yield (process_row(row) for row in resource)


def flow(*_):
    return DF.Flow(
        process
    )