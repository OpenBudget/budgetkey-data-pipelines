import itertools
import logging

from datapackage_pipelines_budgetkey.common.periods import convert_period

from datapackage_pipelines.wrapper import process


def process_row(row, *_):
    majors = [
        ('office', 'claim_date', 'מועד אחרון להגשה'),
        ('exemptions', 'claim_date', 'תאריך אחרון להגשת השגות'),

        ('office', 'start_date', 'תאריך פרסום המכרז'),
        ('exemptions', 'start_date', 'תחילת תקופת התקשרות'),

        ('exemptions', 'end_date', 'תום תקופת ההתקשרות'),
        ('central', 'end_date', 'תאריך סיום תוקף מכרז'),
    ]
    timeline = []
    for tender_type, field, title in majors:
        if row['tender_type'] == tender_type:
            if row[field]:
                timeline.append(dict(
                    timestamp = str(row[field]),
                    major = True,
                    title = title
                ))
    for document in row.get('documents', []):
        if document['update_time']:
            timeline.append(dict(
                timestamp = document['update_time'],
                url = document['link'],
                title = 'צירוף קובץ: {}'.format(document['description']),
                major = False
            ))
    payments = sorted((p for a in row.get('awardees', []) for p in a.get('payments', [])), key=lambda t: t[0])
    for period, payments in itertools.groupby(payments, lambda t: t[0]):
        period = convert_period(period, False)
        if period is None:
            continue
        paid = sum(t[1] for t in payments)
        if paid > 0:
            executed = float(row['contract_executed'])
            volume = float(row['contract_volume'])
            if executed == 0:
                logging.error('Paid > 0 while Executed == 0 for row %s/%s/%s', 
                              row['publication_id'], row['tender_type'], row['tender_id'])
                break
            elif volume == 0:
                logging.error('Paid > 0 while Volume == 0 for row %s/%s/%s', 
                              row['publication_id'], row['tender_type'], row['tender_id'])
                break
            else: 
                percent = 100 * paid / volume
                timeline.append(dict(
                    timestamp = str(period),
                    title = 'תשלום של ₪{:,.0f}, {:.0f}% מהסכום'.format(paid, percent),
                    major = True,
                    percent = percent
                ))
    
    percent = None
    timeline = sorted(timeline, key = lambda x: x['timestamp'], reverse=True)
    for event in reversed(timeline):
        if 'percent' not in event:
            if percent is not None:
                event['percent'] = percent
        else:
            percent = event['percent']
    row['timeline'] = timeline

    if row.get('subjects'):
        row['subject_list'] = [x.strip() for x in row['subjects'].split(';')]
    else:
        row['subject_list'] = []
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([
        {
            'name': 'timeline',
            'type': 'array',
            'es:itemType': 'object',
            'es:index': False
        },
        {
            'name': 'subject_list',
            'type': 'array',
            'es:itemType': 'string',
        }
    ])
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)