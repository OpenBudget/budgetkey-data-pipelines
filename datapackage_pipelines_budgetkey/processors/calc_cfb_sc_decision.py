from datetime import datetime
import dataflows as DF
import os
import logging
from sqlalchemy import create_engine


now = datetime.now()
today = now.date()


def process_row(creation_dates, pk, row):
    tips = row.setdefault('actionable_tips', [])
    claim_date = row.get('claim_date')
    decision = row.get('decision')
    if claim_date:
        if now < claim_date:
            row['decision'] = 'פתוח'
        else:
            row['decision'] = decision or 'סגור'
    else:
        created_at = creation_dates.get((row.get(k) for k in pk))
        if not created_at:
            logging.warning('ROW %r doesn''t have a creation date', row)
        publication_date = row.get('start_date') or created_at or datetime.now()
        if isinstance(publication_date, datetime):
            publication_date = publication_date.date()
        if not decision:
            tips.append((
                '<b class="red">לא הצלחנו לאתר במקור המידע הממשלתי את התאריך בו נסגרת ההרשמה</b>' +
                '. לבירור, מומלץ לבדוק בפרסום המקורי או ליצור קשר עם האחרא/ית',
                None
            ))
            if (today - publication_date).days < 30:
                row['decision'] = 'פתוח'
            else:
                row['decision'] = 'לא ידוע'
    return row


def process(table_name):
    def func(package):
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
        pk = package.pkg.descriptor['resources'][0]['schema']['primaryKey']
        creation_dates = {}
        try:
            engine = create_engine(os.environ['DPP_DB_ENGINE'])
            sql = "select " + ''.join('"%s",' % k for k in pk) + ",__created_at from " + table_name
            creation_dates = map(dict, engine.execute(sql))
            creation_dates = dict((tuple(r[k] for k in pk), r['__created_at']) for r in creation_dates)
        except Exception:
            logging.exception('Failed to fetch creation dates')
        logging.info('GOT %d existing entries from %s', len(creation_dates), table_name)
        for resource in package:
            yield (process_row(creation_dates, pk, row) for row in resource)
    return func


def flow(parameters, *_):
    return DF.Flow(
        process(parameters['creation_date_table'])
    )
