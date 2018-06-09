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
        timeline.append(dict(
            timestamp = document['update_time'],
            url = document['link'],
            title = document['description'],
            major = False
        ))
    timeline = sorted(timeline, key = lambda x: x['timestamp'], reverse=True)
    row['timeline'] = timeline
    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append(
        {
            'name': 'timeline',
            'type': 'array',
            'es:itemType': 'object',
            'es:index': False
        }
    )
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)