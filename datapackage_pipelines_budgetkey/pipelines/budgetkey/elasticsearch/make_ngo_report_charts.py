import os
from sqlalchemy import create_engine

from datapackage_pipelines.wrapper import process

engine = create_engine(os.environ['DPP_DB_ENGINE'])


SPENDING_ANALYSIS_FOR_FOA = """
SELECT payer, spending
FROM publisher_foa_analysis
WHERE field_of_activity='{foa}'
"""


def get_spending_analysis(foa):
    query = SPENDING_ANALYSIS_FOR_FOA.format(foa=foa)
    results = engine.execute(query)
    results = [dict(r) for r in results]
    for r in results:
        r['amount'] = sum(x['amount'] for x in r['spending'])
    results = sorted(results, key=lambda x: -x['amount'])
    return results


def process_row(row, *_):
    if row['key'].startswith('ngo-activity-report'):
        details = row['details']
        foa = details['field_of_activity']
        foad = details['field_of_activity_display']
        spending_analysis = get_spending_analysis(foa)
        row['charts'] = [ 
            {
                'title': 'מי פעיל/ה ואיפה',
                'description': '*ארגונים שדיווחו על כמה אזורי פעילות נספרים במחוזות השונים',
                'subcharts': [
                    {
                        'title': 'ארגונים: <span class="figure">{}</span>'.format(details['report'].get('total', {}).get('total_amount', 0)),
                        'long_title': 'מספר הארגונים הפעילים בתחום {} לפי מחוז'.format(foad),
                        'type': 'horizontal-barchart',
                        'chart': {
                            'values': [
                                dict(
                                    label=x[0],
                                    value=x[1]
                                )
                                for x in details['report'].get('total', {}).get('association_activity_region_districts', [])
                            ]
                        }
                    },
                    {
                        'title': 'בעלי אישור ניהול תקין: <span class="figure">{}</span>'.format(details['report'].get('proper_management', {}).get('total_amount', 0)),
                        'long_title': 'מספר הארגונים הפעילים בתחום {} לפי מחוז'.format(foad),
                        'type': 'horizontal-barchart',
                        'chart': {
                            'values': [
                                dict(
                                    label=x[0],
                                    value=x[1]
                                )
                                for x in details['report'].get('proper_management', {}).get('association_activity_region_districts', [])
                            ]
                        }
                    },
                    {
                        'title': 'בעלי סעיף 46: <span class="figure">{}</span>'.format(details['report'].get('has_article_46', {}).get('total_amount', 0)),
                        'long_title': 'מספר הארגונים הפעילים בתחום {} לפי מחוז'.format(foad),
                        'type': 'horizontal-barchart',
                        'chart': {
                            'values': [
                                dict(
                                    label=x[0],
                                    value=x[1]
                                )
                                for x in details['report'].get('has_article_46', {}).get('association_activity_region_districts', [])
                            ]
                        }
                    },
                ]
            },
            {
                'title': 'מי מקבל/ת כספי ממשלה, וכמה?',
                'long_title': 'אילו ארגונים בתחום {} מקבלים כספי ממשלה, וכמה?'.format(details['field_of_activity_display']),
                'subcharts': [
                    {
                        'title': 'סה״כ העברות כספי מדינה' + '<br/><span class="figure">{:,} ₪</span>'.format(details['income_total']),
                        'type': 'adamkey',
                        'chart': {
                            'values': [
                                dict(
                                    label='<a href="/i/{}">{}</a>'.format(x['doc_id'], x['name']),
                                    amount=x['amount'],
                                    amount_fmt='{:,} ₪'.format(x['amount']),
                                )
                                for x in details['income_list']
                            ]
                        }
                    },
                    {
                        'title': 'סך התקשרויות ממשלתיות מדווחות' + '<br/><span class="figure">{:,} ₪</span>'.format(details['income_total_contracts']),
                        'type': 'adamkey',
                        'chart': {
                            'values': [
                                dict(
                                    label='<a href="/i/{}">{}</a>'.format(x['doc_id'], x['name']),
                                    amount=x['amount'],
                                    amount_fmt='{:,} ₪'.format(x['amount']),
                                )
                                for x in details['income_list_contracts']
                            ]
                        }
                    },
                    {
                        'title': 'סך התמיכות הממשלתיות המדווחות' + '<br/><span class="figure">{:,} ₪</span>'.format(details['income_total_supports']),
                        'type': 'adamkey',
                        'chart': {
                            'values': [
                                dict(
                                    label='<a href="/i/{}">{}</a>'.format(x['doc_id'], x['name']),
                                    amount=x['amount'],
                                    amount_fmt='{:,} ₪'.format(x['amount']),
                                )
                                for x in details['income_list_supports']
                            ]
                        }
                    },
                ]
            },
            {
                'title': 'במה מושקע הכסף הממשלתי?',
                'description': 'הנתונים המוצגים כוללים את העברות הכספי המתועדות במקורות המידע שלנו בכל השנים',
                'type': 'spendomat',
                'chart': {
                    'data': spending_analysis
                }                
            }
        ]

    elif row['key'].startswith('ngo-district-report'):
        details = row['details']
        district = details['district']
        row['charts'] = [ 
            {
                'title': 'מספר הארגונים הפעילים במחוז {} לפי תחום'.format(district),
                'subcharts': [
                    {
                        'title': 'סה״כ ארגונים באזור: <span class="figure">{}</span>'.format(details['report'].get('total', {}).get('count', 0)),
                        'type': 'horizontal-barchart',
                        'chart': {
                            'values': [
                                dict(
                                    label=x[0],
                                    value=x[1]
                                )
                                for x in details['report'].get('total', {}).get('activities', [])
                            ]
                        }
                    },
                    {
                        'title': 'סה״כ ארגונים עם אישור ניהול תקין: <span class="figure">{}</span>'.format(details['report'].get('proper_management', {}).get('count', 0)),
                        'type': 'horizontal-barchart',
                        'chart': {
                            'values': [
                                dict(
                                    label=x[0],
                                    value=x[1]
                                )
                                for x in details['report'].get('proper_management', {}).get('activities', [])
                            ]
                        }
                    },
                    {
                        'title': 'סה״כ ארגונים עם אישור 46: <span class="figure">{}</span>'.format(details['report'].get('has_article_46', {}).get('count', 0)),
                        'type': 'horizontal-barchart',
                        'chart': {
                            'values': [
                                dict(
                                    label=x[0],
                                    value=x[1]
                                )
                                for x in details['report'].get('has_article_46', {}).get('activities', [])
                            ]
                        }
                    },

                ]
            },
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
