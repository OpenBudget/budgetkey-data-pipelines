import os
from sqlalchemy import create_engine, text

from datapackage_pipelines.wrapper import process
from datapackage_pipelines_budgetkey.common.format_number import format_number

engine = create_engine(os.environ['DPP_DB_ENGINE']).connect()


SPENDING_ANALYSIS_FOR_FOA = """
SELECT payer, spending
FROM publisher_foa_analysis
WHERE field_of_activity='{foa}'
"""

ALL_DISTRICTS = """
WITH a AS
  (SELECT jsonb_array_elements(association_activity_region_districts) AS x,
          count(1)
   FROM guidestar_processed
   GROUP BY 1
   ORDER BY 2 DESC)
SELECT x
FROM a"""

ALL_FOAS = """
select distinct(association_field_of_activity) as x from guidestar_processed order by 1
"""


def get_spending_analysis(foa):
    query = SPENDING_ANALYSIS_FOR_FOA.format(foa=foa)
    results = engine.execute(text(query))
    results = [r._asdict() for r in results]
    for r in results:
        r['amount'] = sum(x['amount'] for x in r['spending'])
    results = sorted(results, key=lambda x: -x['amount'])
    return results


def get_distinct_list(query):
    results = engine.execute(text(query))
    results = [r._asdict()['x'] for r in results]
    return results


all_districts = get_distinct_list(ALL_DISTRICTS)
all_foas = get_distinct_list(ALL_FOAS)


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
                                    label='<a href="//next.obudget.org/i/reports/ngo-district-report/{0}?theme=budgetkey">{0}</a>'.format(x[0]),
                                    value=x[1]
                                )
                                for x in details['report'].get('total', {}).get('association_activity_region_districts', [])
                            ]
                        }
                    },
                    {
                        'title': '''בעלי
                        <span class='bk-tooltip-anchor'>אישור ניהול תקין<span class='bk-tooltip'>
אישור ניהול תקין מרשם העמותות הוא תנאי לקבלת תמיכה מהמדינה.
 על מנת לקבל אישור ניהול תקין נדרש הארגון לעמוד בדרישות מחמירות יותר מאשר הדרישות הקיימות בחוק העמותות.                        
                        </span></span>: 
                         <span class="figure">{}</span>'''.format(details['report'].get('proper_management', {}).get('total_amount', 0)),
                        'long_title': 'מספר הארגונים הפעילים בתחום {} לפי מחוז'.format(foad),
                        'type': 'horizontal-barchart',
                        'chart': {
                            'values': [
                                dict(
                                    label='<a href="//next.obudget.org/i/reports/ngo-district-report/{0}?theme=budgetkey">{0}</a>'.format(x[0]),
                                    value=x[1]
                                )
                                for x in details['report'].get('proper_management', {}).get('association_activity_region_districts', [])
                            ]
                        }
                    },
                    {
                        'title': '''בעלי
                        <span class='bk-tooltip-anchor'>סעיף 46<span class='bk-tooltip'>
סעיף 46 לפקודת מס ההכנסה מגדיר כי תרומה לארגונים
 המוכרים כארגונים לתועלת הציבור, מעניקה זיכוי בסך 35% מסכום התרומה.
                        </span></span>: 
                         
                         <span class="figure">{}</span>'''.format(details['report'].get('has_article_46', {}).get('total_amount', 0)),
                        'long_title': 'מספר הארגונים הפעילים בתחום {} לפי מחוז'.format(foad),
                        'type': 'horizontal-barchart',
                        'chart': {
                            'values': [
                                dict(
                                    label='<a href="//next.obudget.org/i/reports/ngo-district-report/{0}?theme=budgetkey">{0}</a>'.format(x[0]),
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
                'description': 'כספי ממשלה שהועברו לארגונים הפעילים בתחום בשלוש השנים האחרונות',
                'subcharts': [
                    {
                        'title': 'סה״כ כספי ממשלה' + '<br/><span class="figure">{}</span>'.format(format_number(details['income_total'])),
                        'type': 'adamkey',
                        'chart': {
                            'values': [
                                dict(
                                    label='<a href="/i/{}?theme=budgetkey">{}</a>'.format(x['doc_id'], x['name']),
                                    amount=x['amount'],
                                    amount_fmt=format_number(x['amount']),
                                )
                                for x in details['income_list']
                            ]
                        }
                    },
                    {
                        'title': 'סך התקשרויות ממשלתיות מדווחות' + '<br/><span class="figure">{}</span>'.format(format_number(details['income_total_contracts'])),
                        'type': 'adamkey',
                        'chart': {
                            'values': [
                                dict(
                                    label='<a href="/i/{}?theme=budgetkey">{}</a>'.format(x['doc_id'], x['name']),
                                    amount=x['amount'],
                                    amount_fmt=format_number(x['amount']),
                                )
                                for x in details['income_list_contracts']
                            ]
                        }
                    },
                    {
                        'title': 'סך התמיכות הממשלתיות המדווחות' + '<br/><span class="figure">{}</span>'.format(format_number(details['income_total_supports'])),
                        'type': 'adamkey',
                        'chart': {
                            'values': [
                                dict(
                                    label='<a href="/i/{}?theme=budgetkey">{}</a>'.format(x['doc_id'], x['name']),
                                    amount=x['amount'],
                                    amount_fmt=format_number(x['amount']),
                                )
                                for x in details['income_list_supports']
                            ]
                        }
                    },
                ]
            },
            {
                'title': 'במה מושקע הכסף הממשלתי?',
                'description': '''
                <span class='bk-tooltip-anchor'>
                הנתונים המוצגים כוללים את העברות הכספים המתועדות במקורות המידע שלנו בשלוש השנים האחרונות
                <span class='bk-tooltip'>
העברות הכספים הממשלתיות מבוססות על דו"חות ההתקשרויות הרבעוניים של גופי הממשלה.
 למידע מעודכן בנוגע לגופים שעמדו והפרו את חובת הפרסום, ראו אתר היחידה לחופש המידע במשרד המשפטים.
                </span>
                </span>
                ''',
                'type': 'spendomat',
                'chart': {
                    'data': spending_analysis
                }                
            }
        ]
        row['others'] = [x for x in all_foas if x != foa]

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
                                    label='<a href="//next.obudget.org/i/reports/ngo-activity-report/{0}?theme=budgetkey">{0}</a>'.format(x[0]),
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
                                    label='<a href="//next.obudget.org/i/reports/ngo-activity-report/{0}?theme=budgetkey">{0}</a>'.format(x[0]),
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
                                    label='<a href="//next.obudget.org/i/reports/ngo-activity-report/{0}?theme=budgetkey">{0}</a>'.format(x[0]),
                                    value=x[1]
                                )
                                for x in details['report'].get('has_article_46', {}).get('activities', [])
                            ]
                        }
                    },

                ]
            },
        ]
        row['others'] = [x for x in all_districts if x != district]

    return row


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'].extend([
        {
            'name': 'charts',
            'type': 'array',
            'es:itemType': 'object',
            'es:index': False
        },
        {
            'name': 'others',
            'type': 'array',
            'es:index': False,
            'es:itemType': 'string',
        }
    ])
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
