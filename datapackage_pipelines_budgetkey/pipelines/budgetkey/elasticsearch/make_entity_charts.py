import os
from sqlalchemy import create_engine
import requests

from datapackage_pipelines.wrapper import process
from datapackage_pipelines_budgetkey.common.format_number import format_number
from datapackage_pipelines_budgetkey.common.datacity_api import hit_datacity_api

import logging

engine = create_engine(os.environ['DPP_DB_ENGINE'])
conn = engine.connect()

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
        results = conn.execute(query)
        unreported_turnover_associations_[foa] = int(dict(list(results)[0])['count'])

    return unreported_turnover_associations_[foa]


reported_turnover_associations_ = {}
def reported_turnover_associations(foa):
    if reported_turnover_associations_.get(foa) is None:
        query = REPORTED_TURNOVER_ASSOCIATIONS_QUERY.format(foa=foa)
        results = conn.execute(query)
        reported_turnover_associations_[foa] = [dict(r) for r in results]

    return reported_turnover_associations_[foa]


def get_spending_analysis(id):
    query = SPENDING_ANALYSIS_FOR_ID.format(id=id)
    results = conn.execute(query)
    results = [dict(r) for r in results]
    for r in results:
        r['amount'] = sum(x['amount'] for x in r['spending'])
    results = sorted(results, key=lambda x: -x['amount'])
    return results
    

def get_association_charts(row, spending_analysis_chart):
    charts = [{
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
    }]

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
    salaries = None #row['details']['top_salaries']
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

    if spending_analysis_chart:
        charts.append(spending_analysis_chart)

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
                        'description': '*הנתונים מבוססים ומחושבים על בסיס המידע הזמין ומוצג ב<a href="http://www.guidestar.org.il/organization/{}" target="_blank">אתר גיידסטאר</a>'.format(id),
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
    return charts


MUNICIPALITY_DETAILS_CONFIG = [
    ('symbol', 'סמל הרשות', 'כללי - סמל הרשות'),
    ('district', 'מחוז', 'כללי - מחוז (משתנה נומינלי)'),
    ('status', 'מעמד מוניציפלי', 'כללי - מעמד מוניציפלי'),
    ('council_member_count', 'מספר חברי מועצה', 'כללי - מספר חברי מועצה'),
    ('status_year', 'שנת קבלת מעמד מוניציפלי', 'כללי - שנת קבלת מעמד מוניציפלי'),
    ('socioeconomic_cluster', 'מדד חברתי-כלכלי - אשכול', 'מדד חברתי-כלכלי - אשכול (מ-1 עד 10, 1 הנמוך ביותר)'),
    ('socioeconomic_rating', 'מדד חברתי-כלכלי - דירוג', 'מדד חברתי-כלכלי - דירוג (מ-1 עד 252, 1 הנמוך ביותר)'),
    ('residents', 'מספר תושבים', 'סה"כ אוכלוסייה'),
    ('pct_jews_other', 'יהודים ואחרים', 'דמוגרפיה - יהודים ואחרים (אחוז)'),
    ('pct_arabs', 'ערבים', 'דמוגרפיה - ערבים (אחוז)'),
    ('pct_65_plus', 'מעל גיל 65', 'דמוגרפיה - בני 65 ומעלה (אחוז באוכלוסייה)'),
    ('pct_young', 'מתחת לגיל 17', 'דמוגרפיה - בני 0-17 (אחוז באוכלוסייה)'),
    ('pct_immigrants', 'עולי 1990+', 'דמוגרפיה - עולי 1990+ (אחוז)'),
    ('compactness_cluster', 'מדד קומפקטיות', 'מדד קומפקטיות - אשכול (מ-1 עד 10, 1 הקומפקטי ביותר)'),
    ('peripheriality_cluster', 'מדד פריפריאליות', 'מדד פריפריאליות - אשכול (1 הפריפריאלי ביותר)'),
    ('num_settlements', 'מספר יישובים במועצה', 'יישובים במועצות אזוריות - סה"כ יישובים במועצה'),
    ('building_committee_name', 'שם ועדת תכנון ובנייה', 'כללי - שם ועדת תכנון ובנייה'),
]

SYMBOLS = dict([
    ('שדות דן', '40'),
])

def get_municipality_names(row):
    municipality_name = row['details'].get('name_municipality')
    symbol = row['details'].get('symbol_municipality_2015')
    if symbol is None:
        symbol = SYMBOLS.get(municipality_name)
    assert symbol is not None, 'No symbol for municipality {}'.format(municipality_name)

    all_names_query = '''
    select name, year from lamas_muni where value='{}' and header='{}' order by year
    '''.format(symbol, 'כללי - סמל הרשות')
    all_names_query = hit_datacity_api(all_names_query)
    names = []
    for name in all_names_query:
        if name['name'] not in names:
            names.append(name['name'])
    if len(all_names_query) < 20:
        logging.info('Only {} years for {}'.format(len(all_names_query), names))
    if len(names) > 1:
        logging.info('Multiple names for {}: {}'.format(municipality_name, names))
    assert len(names) > 0, 'Couldn\'t resolve municipality {}'.format(municipality_name)
    return symbol, names, all_names_query[-1]['year']


def format_str_list(lst):
    lst = [x.replace("'", r"''") for x in lst]
    lst = ', '.join("'{}'".format(x) for x in lst)
    return lst


def get_municipality_details(row):
    symbol, names, latest_year = get_municipality_names(row)
    formatted_names = format_str_list(names)

    query = '''
    select * from (select header, year, value, 
           row_number() over (partition by header order by year desc) as row_number
           from lamas_muni where name in ({})) as a where row_number=1
    '''.format(formatted_names)
    values = hit_datacity_api(query)
    values = dict((v['header'], dict(year=v['year'], value=v['value'])) for v in values)
    details = dict(
        name=dict(name='שם הרשות', year=latest_year, value=names[-1]),
        all_names=dict(name='כל השמות', year=latest_year, value=names),
        other_names=dict(name='שמות נוספים', year=latest_year, value=names[:-1]),
    )
    for key, name, header in MUNICIPALITY_DETAILS_CONFIG:
        if header in values and values[header]['value'] is not None:
            details[key] = dict(
                name=name,
                value=values[header]['value'],
                year=values[header]['year'],
            )
    if len(details) - 3 != len(MUNICIPALITY_DETAILS_CONFIG):
        logging.info('Missing data for {}: {!r}'.format(names, set(c[0] for c in MUNICIPALITY_DETAILS_CONFIG) - set(details.keys())))
    return symbol, names, dict(extended=details)

def get_municipality_comparison(muni_names, title, header, **kwargs):
    header = header.replace("'", r"''")
    all_munis_query = '''
           select * from (select name, year, value, 
           row_number() over (partition by name order by year desc) as row_number
           from lamas_muni where header='{}') as a where row_number=1 order by value::numeric desc
    '''.format(header)
    all_munis_values = hit_datacity_api(all_munis_query)
    max_year = max(v['year'] for v in all_munis_values)
    all_munis_values = [v for v in all_munis_values if v['year'] == max_year]
    all_names = [x['name'] for x in all_munis_values]
    idx = None
    for x in muni_names:
        if x in all_names:
            idx = all_names.index(x)
            break
    if idx is None:
        print('Couldn\'t find {} in {}'.format(muni_names, all_names))
        return None
    return dict(
        title=title,
        type='adamkey',
        chart=dict(
            values=[
                dict(
                    label=x['name'],
                    amount=float(x['value']),
                    amount_fmt='{:.1f}%'.format(float(x['value']))
                )
                for x in all_munis_values
                if x['value']
            ],
            selected=idx
        ),
        **kwargs
    )


def get_municipality_graph(formatted_names, title, units, series, mode='lines', **kwargs):
    chart_options = dict()
    if mode == 'lines':
        chart_options=dict(mode='lines+markers')
    if mode == 'stacked-100':
        chart_options=dict(stackgroup='one', groupnorm='percent')
    if mode == 'stacked':
        chart_options=dict(stackgroup='one')
    charts=[]
    for name, *headers in series:
        if len(headers) == 0:
            headers = [name]
        headers = format_str_list(headers)
        query = '''
            select year, value::numeric as value from lamas_muni where name in ({}) and header in ({}) group by 1, 2 order by 1 asc
        '''.format(formatted_names, headers)
        rows = hit_datacity_api(query)
        for r in rows:
            r['value'] = float(r['value']) if r['value'] else None
        x = [r['year'] for r in rows if r['value'] is not None]
        if len(x) == 0:
            continue
        if len(x) != len(set(x)):
            logging.warning('Duplicate years for {}: {!r}'.format(name, x))
            continue
        y = [r['value'] for r in rows if r['value'] is not None]        
        charts.append((name, x, dict(zip(x, y))))
    if len(charts) == 0:
        return None
    all_years = sorted(set(sum((x for _, x, _ in charts), [])))
    charts = [
        (name, all_years, [d.get(x) for x in all_years])
        for name, _, d in charts
    ]
    return dict(
        type='plotly',
        title=title,
        layout=dict(
            xaxis=dict(title='שנה', type='category'),
            yaxis=dict(title=units, separatethousands=True, rangemode='tozero'),
        ),
        chart=[
            dict(
                name=series_name,
                x=x,
                y=y,
                **chart_options
            )
            for series_name, x, y in charts
        ],
        **kwargs
    )


def prune(l):
    return [x for x in l if x is not None]


def get_municipality_charts(names, spending_analysis_chart):
    fn = format_str_list(names)
    charts = [
        dict(
            title='דמוגרפיה',
            long_title='תושבי הרשות',
            subcharts=prune([
                get_municipality_graph(fn, 'מספר התושבים ברשות', 'תושבים', [
                    ('סה"כ אוכלוסיה', 'סה"כ אוכלוסייה'),
                ]),
                get_municipality_graph(fn, 'תמהיל גילאים', 'אחוז', [
                    ('בני 0-4', 'דמוגרפיה - בני 0-4 (אחוז באוכלוסייה)'),
                    ('בני 5-9', 'דמוגרפיה - בני 5-9 (אחוז באוכלוסייה)'),
                    ('בני 10-14', 'דמוגרפיה - בני 10-14 (אחוז באוכלוסייה)'),
                    ('בני 15-19', 'דמוגרפיה - בני 15-19 (אחוז באוכלוסייה)'),
                    ('בני 20-29', 'דמוגרפיה - בני 20-29 (אחוז באוכלוסייה)'),
                    ('בני 30-44', 'דמוגרפיה - בני 30-44 (אחוז באוכלוסייה)'),
                    ('בני 45-59', 'דמוגרפיה - בני 45-59 (אחוז באוכלוסייה)'),
                    ('בני 60-64', 'דמוגרפיה - בני 60-64 (אחוז באוכלוסייה)'),
                    ('בני 65 ומעלה', 'דמוגרפיה - בני 65 ומעלה (אחוז באוכלוסייה)'),
                ], mode='stacked-100'),
                get_municipality_comparison(
                    names, 'השוואת אחוז עולים חדשים', 'דמוגרפיה - עולי 1990+ (אחוז)', 
                    long_title='השוואת אחוז העולים החדשים באוכלוסיה בין הרשויות השונות',
                    description='עולים חדשים מאז שנת 1990',
                ),
                get_municipality_comparison(
                    names, 'השוואת אחוז מבוגרים', 'דמוגרפיה - בני 65 ומעלה (אחוז באוכלוסייה)', 
                    long_title='השוואת אחוז המבוגרים מעל לגיל 65 באוכלוסיה בין הרשויות השונות',
                ),
                get_municipality_comparison(
                    names, 'השוואת אחוז צעירים', 'דמוגרפיה - בני 0-17 (אחוז באוכלוסייה)', 
                    long_title='השוואת אחוז הצעירים מתחת לגיל 17 באוכלוסיה בין הרשויות השונות',
                ),
                get_municipality_graph(fn, 'עולים חדשים', 'אחוז', [
                    ('אחוז עולי 1990+ מכלל האוכלוסיה', 'דמוגרפיה - עולי 1990+ (אחוז)'),
                ], description='עולים חדשים מאז שנת 1990'),
            ])
        ),
        dict(
            title='גיאוגרפיה',
            long_title='מאפייני הרשות במרחב',
            subcharts=prune([
                get_municipality_graph(fn, 'שטח השיפוט', 'קמ״ר', [
                    ('שטח הרשות', 'סך הכל שטח (קמ"ר)'),
                    ('שטח השיפוט', 'שימושי קרקע - סך הכל שטח שיפוט (שטח בקמ"ר)'),
                ]),
                get_municipality_graph(fn, 'צפיפות אוכלוסיה', 'נפשות לקמ״ר', [
                    ('צפיפות אוכלוסיה', 'דמוגרפיה - צפיפות אוכלוסייה (נפשות לקמ"ר)'),
                    # ('צפיפות אוכלוסיה בשטח בנוי', 'צפיפות אוכלוסייה לשטח בנוי (נפשות לקמ"ר)'),
                    ('צפיפות בשטח בנוי למגורים', 'צפיפות אוכלוסייה לשטח בנוי למגורים (נפשות לקמ"ר)'),
                ]),
                get_municipality_graph(fn, 'דירות למגורים', 'מספר דירות למגורים', [
                    # ('לפי חיובי ארנונה', 'מספר דירות למגורים לפי חיובי ארנונה'),
                    ('מספר דירות', 'בנייה ודיור - מספר דירות למגורים לפי מרשם מבנים ודירות (סה"כ)'),
                ]),
                get_municipality_graph(fn, 'בנייה למגורים', 'מספר דירות', [
                    ('סיום בנייה', 'בנייה ודיור - גמר בנייה: מספר דירות (סה"כ)'),
                    ('התחלת בנייה', 'בנייה ודיור - התחלת בנייה - מספר דירות (ק"מ)'),
                ]),
            ])
        ),
        dict(
            title='כספים',
            long_title='הכנסות והוצאות הרשות',
            subcharts=[
                spending_analysis_chart,
                get_municipality_graph(fn, 'הוצאות הרשות', 'אלפי ש״ח', [
                    ('תקציב רגיל', 'הוצאות בתקציב הרגיל - סה"כ הוצאות בתקציב רגיל (אלפי ש"ח)'),
                    ('תקציב בלתי רגיל', 'הוצאות בתקציב בלתי רגיל - סה"כ הוצאות בתקציב בלתי רגיל (אלפי ש"ח)'),
                ]),
                get_municipality_graph(fn, 'סוגי הוצאות', 'אחוז', [
                    ('תקציב רגיל', 'הוצאות בתקציב הרגיל - סה"כ הוצאות בתקציב רגיל (אלפי ש"ח)'),
                    ('תקציב בלתי רגיל', 'הוצאות בתקציב בלתי רגיל - סה"כ הוצאות בתקציב בלתי רגיל (אלפי ש"ח)'),
                ], mode='stacked-100'),
                get_municipality_graph(fn, 'סוגי הכנסות', 'אחוז', [
                    ('הכנסות עצמיות', 'הכנסות בתקציב הרגיל - הכנסות עצמיות של הרשות (אלפי ש"ח)'),
                    ('הכנסות מהממשלה', 'הכנסות בתקציב הרגיל - הכנסות מהממשלה (אלפי ש"ח)'),
                ], mode='stacked-100'),

            ]
        ),
        dict(
            title='חינוך',
            long_title='מה מצב מערכת החינוך ברשות המקומית?',
            subcharts=[
                get_municipality_graph(fn, 'מספר בתי ספר לפי סוג', 'בתי ספר', [
                    ('בתי״ס יסודיים וחינוך מיוחד', 'חינוך והשכלה - בתי ספר יסודיים (סה"כ)'),
                    ('חטיבות ביניים', 'חינוך והשכלה - חטיבות ביניים (סה"כ)'),
                    ('בתי״ס תיכוניים', 'חינוך והשכלה - בתי ספר תיכוניים (סה"כ)'),
                ]),
                get_municipality_graph(fn, 'מספר כיתות לפי סוג', 'כיתות', [
                    ('בתי״ס יסודיים וחינוך מיוחד', 'כיתות בבתי ספר יסודיים (כולל חינוך מיוחד)'),
                    ('חטיבות ביניים', 'כיתות בחטיבות ביניים'),
                    ('בתי״ס תיכוניים', 'כיתות בבתי ספר תיכוניים'),
                ]),
                get_municipality_graph(fn, 'מספר תלמידים לפי סוג', 'תלמידים', [
                    ('גנים של משרד החינוך', 'חינוך - ילדים בגנים של משרד החינוך - סה"כ'),
                    ('בתי״ס יסודיים וחינוך מיוחד', 'חינוך - תלמידים בבתי ספר יסודיים (כולל חינוך מיוחד)'),
                    ('חטיבות ביניים', 'חינוך - תלמידים בחטיבות ביניים'),
                    ('בתי״ס תיכוניים', 'חינוך - תלמידים בבתי ספר תיכוניים'),
                ]),
                get_municipality_graph(fn, 'זכאים לבגרות', 'אחוז זכאים', [
                    ('חינוך והשכלה - זכאים לתעודת בגרות מבין תלמידי כיתות יב (אחוז)',),
                ]),
                get_municipality_graph(fn, 'תקציב החינוך', 'אלפי ₪', [
                    ('סה״כ הוצאה לחינוך', 'הוצאות בתקציב הרגיל - חינוך (אלפי ש"ח)',),
                    ('השתתפות ממשלתית בתקציב החינוך', 'הכנסות בתקציב הרגיל - הכנסות של הרשות ממשרד הרווחה (אלפי ש"ח)'), 
                ]),
            ]
        ),
        dict(
            title='רווחה',
            long_title=' מאפייני רווחה ברשות המקומית',
            subcharts=prune([
                get_municipality_graph(fn, 'מדד-חברתי כלכלי', 'דירוג הרשות, 1 הנמוך ביותר', [
                    ('מדד חברתי-כלכלי - דירוג', 'מדד חברתי-כלכלי - דירוג (מ-1 עד 252, 1 הנמוך ביותר)'),
                ]),
                get_municipality_comparison(
                    names, 'מדד חברתי-כלכלי - השוואה', 'מדד חברתי-כלכלי - דירוג (מ-1 עד 252, 1 הנמוך ביותר)', 
                    long_title='השוואת הרשויות השונות לפי ציון המדד החברתי כלכלי',
                ),
                get_municipality_graph(fn, 'מקבלי קיצבאות לפי סוג', 'מקבלי קיצבאות', [
                    # ('ילדים מקבלי קיצבאות', 'מספר הילדים מקבלי קצבאות בגין ילדים - סה"כ'),
                    ('זקנה ושאירים', 'שכר ורווחה - מקבלי קצבאות זקנה ושאירים (סה"כ)'),
                    ('הבטחת הכנסה', 'שכר ורווחה - מקבלי הבטחת הכנסה (נפשות)'),
                    ('דמי אבטלה', 'שכר ורווחה - מקבלי דמי אבטלה (ממוצע חודשי)'),
                    ('גמלאות סיעוד', 'שכר ורווחה - מקבלי גמלאות סיעוד (סה"כ)'),
                    ('גמלאות נכות מעבודה ותלויים', 'שכר ורווחה - מקבלי גמלאות נכות מעבודה ותלויים (סה"כ)'),
                    ('גמלאות נכות כללית', 'שכר ורווחה - מקבלי גמלאות נכות כללית (סה"כ)'),
                    ('גמלאות ניידות', 'שכר ורווחה - מקבלי גמלאות ניידות (סה"כ)'),
                ]),
                get_municipality_graph(fn, 'שכר חודשי ממוצע (לשכירים)', '₪ לחודש', [
                    ('סה״כ', 'שכר ורווחה - שכר חודשי ממוצע של שכירים (ש"ח)'),
                    ('גברים', 'שכר ורווחה - שכר חודשי ממוצע של גברים שכירים (ש"ח)'),
                    ('נשים', 'שכר ורווחה - שכר חודשי ממוצע של נשים שכירות (ש"ח)'),
                ]),
                get_municipality_graph(fn, 'תקציב הרווחה', 'אלפי ₪', [
                    ('סה״כ הוצאה לרווחה', 'הוצאות בתקציב הרגיל - רווחה (אלפי ש"ח)',),
                    ('השתתפות ממשלתית בתקציב לרווחה', 'הכנסות בתקציב הרגיל - הכנסות של הרשות ממשרד הרווחה (אלפי ש"ח)'), 
                ]),
            ])
        ),
    ]
    return charts

def get_muni_budgets(symbol):
    query = f'''
        SELECT title, code, year, allocated, revised, executed
        from muni_budgets where muni_code='{symbol}' and func_2_code is null;
    '''
    results = conn.execute(query)
    results = [dict(r) for r in results]
    return dict(
        (i['code'], i) for i in results
    )



def select_muni_budgets(details, budgets):
    items = []
    for code, name, *extra in [
        ('1', 'ארנונה ואגרות', 'income-pie'),
        ('2', 'שירותים עירוניים', 'income-pie'),
        ('3', 'הכנסות ממשלתיות', 'income-pie'),
        ('4', 'מפעלים (מים, ביוב...)', 'income-pie'),
        ('5', 'הכנסות אחרות', 'income-pie'),
        ('81','חינוך', 'selected', 'fa-graduation-cap'),
        ('82','תרבות', 'selected', 'fa-university'),
        ('83','בריאות', 'selected', 'fa-medkit'),
        ('84','רווחה', 'selected', 'fa-wheelchair'),
        ('85','שירותי דת', 'selected', 'fa-book'),
        ('86','קליטת עליה', 'selected', 'fa-plane'),
        ('87','איכות הסביבה', 'selected', 'fa-globe'),
        ('71','תברואה', 'selected', 'fa-trash'),
        ('72','שמירה ובטחון', 'selected', 'fa-shield'),
        ('75','חגיגות ואירועים', 'selected', 'fa-birthday-cake'),
        ('6','הנהלה כללית'),
        ('7','שירותים עירוניים'),
        ('8','שירותים ממלכתיים'),
        ('9','מפעלים והוצאות אחרות'),
        ('EXPENDITURE', ''),
        ('REVENUE', ''),
    ]:
        use = extra[0] if extra else None
        icon = extra[1] if use == 'selected' else None
        if code in budgets:
            bi = budgets[code]
            v = None
            if bi['executed']:
                v = dict(
                    value=bi['executed'],
                    value_kind='ביצוע',
                )
            elif bi['revised']:
                v = dict(
                    value=bi['revised'],
                    value_kind='עריכה',
                )
            elif bi['allocated']:
                v = dict(
                    value=bi['allocated'],
                    value_kind='הקצאה',
                )
            if v is not None:
                bi.update(v)
                bi.update(dict(
                    name=name, icon=icon, use=use
                ))
                items.append(bi)
    details['select_budgets'] = items


def process_row(row, *_):
    id = row['id']
    kind = row['kind']
    charts = []

    spending_analysis = get_spending_analysis(id)
    spending_analysis_chart = None
    if spending_analysis:
        title = 'מהן ההכנסות מהממשלה?' if kind =='company' else 'מקבל כספי ממשלה?'
        spending_analysis_chart = {
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
        }


    if kind == 'association':
        charts = get_association_charts(row, spending_analysis_chart)

    elif kind == 'municipality':
        try:
            symbol, names, details = get_municipality_details(row)
            row['details'].update(details)
            muni_budgets = get_muni_budgets(symbol)
            select_muni_budgets(row['details'], muni_budgets)
            charts = get_municipality_charts(names, spending_analysis_chart)
        except Exception as e:
            logging.info('Error getting municipality details for {}: {}'.format(row['name'], e))
    else:
        if spending_analysis_chart:
            charts = [spending_analysis_chart]
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
