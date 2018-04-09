from datapackage_pipelines.wrapper import process

def process_row(row, *_):
    if row['kind'] == 'association':
        row['charts'] = [ 
            {
                'title': 'מיהו הארגון?',
                'long_title': 'מיהו הארגון',
                'type': 'template',
                'template_id': 'org_status'
            },
            {
                'title': 'מקבל כספי ממשלה?',
                'long_title': 'האם הארגון מקבל כספי ממשלה?',
                'description': 'התקשרויות ותמיכות לפי משרדים, הנתונים כוללים העברות המתועדות במקורות המידע הזמינים מכל השנים.',
            },
            {
                'title': 'אילו אישורים?',
                'long_title': 'אישורים והכרה בארגון',
            },
            {
                'title': 'כמה עובדים ומתנדבים?',
                'long_title': 'כמה עובדים ומתנדבים בארגון',
            },
            {
                'title': 'מי הארגונים הדומים?',
                'long_title': 'ארגונים הפועלים בתחום הבריאות, לפי גובה המחזור הכספי השנתי',
                'description': 'xx ארגונים נוספים הפועלים בתחום לא דיווחו על על גובה המחזור הכספי השנתי'
            }
        ]

    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].append(
        {
            'name': 'charts',
            'type': 'array',
            'es:itemType': 'object',
            'es:index': False
        }
    )
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
