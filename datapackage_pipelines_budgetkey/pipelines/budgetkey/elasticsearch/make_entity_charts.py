import os
from sqlalchemy import create_engine

from datapackage_pipelines.wrapper import process
from datapackage_pipelines_budgetkey.common.format_number import format_number

engine = create_engine(os.environ['DPP_DB_ENGINE'])

UNREPORTED_TURNOVER_ASSOCIATIONS_QUERY = """
SELECT count(1) as count
FROM guidestar_processed
WHERE association_field_of_activity='{foa}'
  AND (association_yearly_turnover = 0
       OR association_yearly_turnover IS NULL)
"""

REPORTED_TURNOVER_ASSOCIATIONS_QUERY = """
SELECT id,
       association_title AS name,
       association_yearly_turnover AS amount
FROM guidestar_processed
WHERE association_field_of_activity='{foa}'
  AND association_yearly_turnover > 0
ORDER BY 3 DESC
"""

SPENDING_ANALYSIS_FOR_ID = """
SELECT payer, spending
FROM publisher_entity_analysis
WHERE entity_id='{id}'
"""

unreported_turnover_associations_ = {}
def unreported_turnover_associations(foa):
    if unreported_turnover_associations_.get(foa) is None:
        query = UNREPORTED_TURNOVER_ASSOCIATIONS_QUERY.format(foa=foa)
        results = engine.execute(query)
        unreported_turnover_associations_[foa] = int(dict(list(results)[0])['count'])

    return unreported_turnover_associations_[foa]


reported_turnover_associations_ = {}
def reported_turnover_associations(foa):
    if reported_turnover_associations_.get(foa) is None:
        query = REPORTED_TURNOVER_ASSOCIATIONS_QUERY.format(foa=foa)
        results = engine.execute(query)
        reported_turnover_associations_[foa] = [dict(r) for r in results]

    return reported_turnover_associations_[foa]


def get_spending_analysis(id):
    query = SPENDING_ANALYSIS_FOR_ID.format(id=id)
    results = engine.execute(query)
    results = [dict(r) for r in results]
    for r in results:
        r['amount'] = sum(x['amount'] for x in r['spending'])
    results = sorted(results, key=lambda x: -x['amount'])
    return results
    

def process_row(row, *_):
    id = row['id']
    kind = row['kind']
    charts = []
    if kind in ('association'):
        charts.append({
                'title': 'מיהו הארגון?',
                'long_title': 'מיהו הארגון',
                'type': 'vertical',
                'chart': {
                    'parts': [
                        {
                            'type': 'template',
                            'template_id': 'org_status'                            
                        },
                    ]
                }
        })

    if kind == 'association':
        foa = row['details']['field_of_activity']
        foad = row['details']['field_of_activity_display']
        num_of_employees = row['details']['num_of_employees']
        num_of_volunteers = row['details']['num_of_volunteers']
        last_report_year = row['details']['last_report_year']
        num_unreported = unreported_turnover_associations(foa)
        reported_list = reported_turnover_associations(foa)
        selected_index = [i for i, x in enumerate(reported_list) if x['id'] == row['id']]
        if len(selected_index) > 0:
            selected_index = selected_index[0]
        else:
            selected_index = None
        salaries = row['details']['top_salaries']
        if salaries:
            top_salary = salaries[0]['salary']
        else:
            top_salary = None
        median_turnover_in_field_of_activity = row['details'].get('median_turnover_in_field_of_activity')
        median_top_salary = row['details'].get('median_top_salary')
        yearly_turnover = row['details'].get('yearly_turnover')
        if None not in (median_turnover_in_field_of_activity, yearly_turnover, last_report_year):
            charts[-1]['chart']['parts'].append(
                {
                    'type': 'comparatron',
                    'title': 'המחזור הכספי המדווח לארגון בשנת {}: {}'.format(last_report_year, format_number(yearly_turnover)),
                    'chart': {
                        'main': {
                            'amount': yearly_turnover,
                            'amount_fmt': format_number(yearly_turnover),
                            'label': str(last_report_year),
                            'color': '#FFAE90'
                        },
                        'compare': {
                            'amount': median_turnover_in_field_of_activity,
                            'amount_fmt': format_number(median_turnover_in_field_of_activity),
                            'label': '''
                            <span class='bk-tooltip-anchor'>חציון בתחום {foad}<span class='bk-tooltip'>
                            מבוסס על נתוני כל הארגונים הפעילים בתחום {foad} אשר דיווחו על גובה המחזור הכספי באחת, או יותר, משלוש השנים האחרונות.
                            </span></span>'''.format(foad=foad)
                        },
                    }
                }
            )
    spending_analysis = get_spending_analysis(row['id'])
    if spending_analysis:
        title = 'מהן ההכנסות מהממשלה?' if kind =='company' else 'מקבל כספי ממשלה?'
        charts.append({
                'title': title,
                'long_title': 'האם הארגון מקבל כספי ממשלה?',
                'description': '''
                <span class='bk-tooltip-anchor'>
                התקשרויות ותמיכות<span class='bk-tooltip'>
                התקשרות היא הסכם בין המשרד הממשלתי לגורם אחר,
                    במסגרתו מתחייב הגורם האחר לספק למשרד הממשלתי שירותים או מוצרים בתמורה לתשלום.
                    <br/>
                תמיכה ממשלתית היא סיוע, בכסף ובשווה כסף, שממשלת ישראל מעבירה לגורמים חוץ ממשלתיים 
                במטרה לסייע במימון פעילות אשר היא מעוניינת לעודד את קיומה ואשר משרתת את אוכלוסייתה.
                </span></span>
                    לפי משרדים, הנתונים כוללים העברות המתועדות במקורות המידע הזמינים מכל השנים.''',
                'type': 'spendomat',
                'chart': {
                    'data': spending_analysis,
                    'theme': 'theme-1' if kind=='association' else 'theme-2'
                }
        })

    if row['kind'] == 'association':
        charts.append({
                'title': 'אילו אישורים?',
                'long_title': 'אישורים והכרה בארגון',
                'type': 'template',
                'template_id': 'org_credentials'
        })
        parts = []
        if None not in (num_of_employees, 
                        num_of_volunteers,
                        last_report_year):
            parts.append({
                            'type': 'pointatron',
                            'title': 'מספר העובדים והמתנדבים בארגון בשנת {}'.format(last_report_year),
                            'chart': {
                                'values': [
                                    {
                                        'title': 'מספר עובדים',
                                        'amount': num_of_employees,
                                        'color': '#6F46E0'
                                    },
                                    {
                                        'title': 'מספר מתנדבים',
                                        'amount': num_of_volunteers,
                                        'color': '#FE8255'
                                    },
                                ]
                            }
                        })
        if None not in (last_report_year, top_salary, median_top_salary, foad):
            parts.append({
                            'type': 'comparatron',
                            'title': 'שכר השנתי הגבוה בארגון בשנת {}'.format(last_report_year),
                            'description': '*הנתונים מבוססים ומחושבים על בסיס המידע הזמין ומוצג ב<a href="http://www.guidestar.org.il/he/organization/{}" target="_blank">אתר גיידסטאר</a>'.format(id),
                            'chart': {
                                'main': {
                                    'amount': top_salary,
                                    'amount_fmt': format_number(top_salary),
                                    'label': 'מקבל השכר הגבוה בארגון',
                                    'color': '#FFAE90'
                                },
                                'compare': {
                                    'amount': median_top_salary,
                                    'amount_fmt': format_number(median_top_salary),
                                    'label': '''
                                    <span class='bk-tooltip-anchor'>חציון בתחום {foad}<span class='bk-tooltip'>
                                    מבוסס על נתוני כל הארגונים הפעילים בתחום {foad} אשר דיווחו על גובה השכר באחת, או יותר, משלוש השנים האחרונות.
                                    </span></span>'''.format(foad=foad)
                                },
                            }
                        })
        if len(parts) > 0:
            charts.append({
                'title': 'כמה עובדים ומתנדבים?',
                'long_title': 'כמה עובדים ומתנדבים בארגון',
                'type': 'vertical',
                'chart': {
                    'parts': parts
                }
            })
        if foad is not None:
            charts.append({
                    'title': 'ארגונים נוספים בתחום',
                    'long_title': 'ארגונים הפועלים בתחום {}, לפי גובה המחזור הכספי השנתי'.format(foad),
                    'description': '{} ארגונים נוספים הפועלים בתחום לא דיווחו על על גובה המחזור הכספי השנתי'
                                        .format(num_unreported),
                    'type': 'adamkey',
                    'chart': {
                        'values': [dict(
                            label='<a href="/i/org/association/{}?theme=budgetkey">{}</a>'.format(x['id'], x['name']),
                            amount=x['amount'],
                            amount_fmt=format_number(x['amount']),
                        )
                        for x in reported_list],
                        'selected': selected_index
                    }
            })
    row['charts'] = charts

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
