from datetime import datetime
import dataflows as DF


now = datetime.now()
today = now.date()


def process(row):
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
                if (today - publication_date).days < 30:
                    row['decision'] = 'חדש'
                else:
                    row['decision'] = 'לא ידוע'
            else:
                # Ensure this will get filled later
                row['decision'] = 'FILLER' + now.isoformat()


def flow(*_):
    return DF.Flow(process)