import itertools

from datapackage_pipelines.wrapper import process

def date_for_period(x):
    return x


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
        paid = sum(t[1] for t in payments)
        if paid > 0:
            percent = 100*paid/row['contract_executed']
            timeline.append(dict(
                timestamp = date_for_period(period),
                title = 'תשלום של {:,}₪, {}% מהסכום'.format(paid, percent),
                major = True,
                percent = percent
            ))
    
    timeline = sorted(timeline, key = lambda x: x['timestamp'], reverse=True)
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