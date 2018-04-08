from datapackage_pipelines.wrapper import process

def process_row(row, *_):
    if row['key'].startswith('ngo-activity-report'):
        details = row['details']
        row['charts'] = [ 
            {
                'title': 'מי פעיל/ה ואיפה',
                'description': '*ארגונים שדיווחו על כמה אזורי פעילות נספרים במחוזות השונים',
                'subcharts': [
                    {
                        'title': 'ארגונים: <span class="figure">{}</span>'.format(details['report'].get('total', {}).get('total_amount', 0)),
                        'long_title': 'מספר הארגונים הפעילים בתחום הבריאות לפי מחוז',
                        'chart': {
                            'type': 'horizontal-barchart',
                            'values': [
                                dict(
                                    label=x[0],
                                    value=x[1]
                                )
                                for x in details['report'].get('total', {}).get('association_activity_region_list', [])
                            ]
                        }
                    },
                    {
                        'title': 'בעלי אישור ניהול תקין: <span class="figure">{}</span>'.format(details['report'].get('proper_management', {}).get('total_amount', 0)),
                        'long_title': 'מספר הארגונים הפעילים בתחום הבריאות לפי מחוז',
                        'chart': {
                            'type': 'horizontal-barchart',
                            'values': [
                                dict(
                                    label=x[0],
                                    value=x[1]
                                )
                                for x in details['report'].get('proper_management', {}).get('association_activity_region_list', [])
                            ]
                        }
                    },
                    {
                        'title': 'בעלי סעיף 46: <span class="figure">{}</span>'.format(details['report'].get('has_article_46', {}).get('total_amount', 0)),
                        'long_title': 'מספר הארגונים הפעילים בתחום הבריאות לפי מחוז',
                        'chart': {
                            'type': 'horizontal-barchart',
                            'values': [
                                dict(
                                    label=x[0],
                                    value=x[1]
                                )
                                for x in details['report'].get('has_article_46', {}).get('association_activity_region_list', [])
                            ]
                        }
                    },
                ]
            },
            {
                'title': 'מי מקבל/ת כספי ממשלה, וכמה?',
                'long_title': 'אילו ארגונים בתחום {} מקבלים כספי ממשלה, וכמה?'.format(details['field_of_activity_display']),
            },
            {
                'title': 'במה מושקע הכסף הממשלתי?',
                'description': 'הנתונים המוצגים כוללים את העברות הכספי המתועדות במקורות המידע שלנו בכל השנים'
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
