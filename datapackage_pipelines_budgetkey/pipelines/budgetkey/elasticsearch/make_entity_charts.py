import os
from sqlalchemy import create_engine
import requests

from datapackage_pipelines.wrapper import process
from datapackage_pipelines_budgetkey.common.format_number import format_number

import logging

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

def hit_datacity_api(query):
    url = 'https://api.datacity.org.il/api/query'
    params = dict(
        query=query
    )
    resp = requests.get(url, params=params).json()
    if resp.get('error'):
        assert False, resp['error']
    if len(resp['rows']) == 0:
        logging.info('EMPTY QUERY %s', query)
    return resp['rows']


MUNICIPALITY_DETAILS_CONFIG = [
    ('symbol', 'סמל הרשות', 'סמל הרשות'),
    ('district', 'מחוז', 'מחוז'),
    ('status', 'מעמד מוניציפלי', 'מעמד מוניציפלי'),
    ('council_member_count', 'מספר חברי מועצה', 'מספר חברי מועצה'),
    ('status_year', 'שנת קבלת מעמד מוניציפלי', 'שנת קבלת מעמד מוניציפלי'),
    ('socioeconomic_cluster', 'מדד חברתי-כלכלי - אשכול', 'מדד חברתי-כלכלי - אשכול (1 הנמוך ביותר)'),
    ('socioeconomic_rating', 'מדד חברתי-כלכלי - דירוג', 'מדד חברתי-כלכלי - דירוג (1 הנמוך ביותר)'),
    ('residents', 'מספר תושבים', 'סה"כ אוכלוסייה (אלפים)'),
    ('pct_jews_other', 'יהודים ואחרים', 'יהודים ואחרים (אחוז)'),
    ('pct_arabs', 'ערבים', 'ערבים (אחוז)'),
    ('pct_65_plus', 'מעל גיל 65', 'אחוז בני 65 ומעלה באוכלוסייה'),
    ('pct_young', 'מתחת לגיל 17', 'אחוז בני 0-17 באוכלוסייה'),
    ('pct_immigrants', 'עולי 1990+', 'עולי 1990+ (אחוז)'),
    ('compactness_cluster', 'מדד קומפקטיות', 'מדד קומפקטיות - אשכול (1 הקומפקטי ביותר)'),
    ('peripheriality_cluster', 'מדד פריפריאליות', 'מדד פריפריאליות - אשכול (1 הפריפריאלי ביותר)'),
    ('num_settlements', 'מספר יישובים במועצה', 'סה"כ יישובים במועצה'),
    ('building_committee_name', 'שם ועדת תכנון ובנייה', 'שם ועדת תכנון ובנייה'),
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
    '''.format(symbol, 'סמל הרשות')
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
    return names, all_names_query[-1]['year']


def format_str_list(lst):
    lst = [x.replace("'", r"''") for x in lst]
    lst = ', '.join("'{}'".format(x) for x in lst)
    return lst


def get_municipality_details(row):
    names, latest_year = get_municipality_names(row)
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
    return formatted_names, dict(extended=details)


def get_municipality_graph(formatted_names, title, units, series, mode='lines'):
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
        charts.append((name, x, y))
    if len(charts) == 0:
        return None
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
        ]
    )


def prune(l):
    return [x for x in l if x is not None]


def get_municipality_charts(fn, spending_analysis_chart):
    charts = [
        dict(
            title='דמוגרפיה',
            long_title='מספר התושבים',
            subcharts=[
                get_municipality_graph(fn, 'מספר התושבים ברשות', 'תושבים', [
                    ('סה"כ אוכלוסיה', 'סה"כ אוכלוסייה'),
                ]),
                get_municipality_graph(fn, 'תמהיל גילאים', 'אחוז', [
                    ('בני 0-4', 'אחוז בני 0-4 באוכלוסייה'),
                    ('בני 5-9', 'אחוז בני 5-9 באוכלוסייה'),
                    ('בני 10-14', 'אחוז בני 10-14 באוכלוסייה'),
                    ('בני 15-19', 'אחוז בני 15-19 באוכלוסייה'),
                    ('בני 20-29', 'אחוז בני 20-29 באוכלוסייה'),
                    ('בני 30-44', 'אחוז בני 30-44 באוכלוסייה'),
                    ('בני 45-59', 'אחוז בני 45-59 באוכלוסייה'),
                    ('בני 60-64', 'אחוז בני 60-64 באוכלוסייה'),
                    ('בני 65 ומעלה', 'אחוז בני 65 ומעלה באוכלוסייה'),
                ], mode='stacked-100'),
                get_municipality_graph(fn, 'עולים חדשים', 'אחוז', [
                    ('אחוז עולי 1990+ מכלל האוכלוסיה', 'עולי 1990+ (אחוז)'),
                ]),
            ]
        ),
        dict(
            title='גיאוגרפיה',
            long_title='מאפייני הרשות במרחב',
            subcharts=prune([
                get_municipality_graph(fn, 'שטח השיפוט', 'קמ״ר', [
                    ('שטח השיפוט', 'סך הכל שטח (קמ"ר)'),
                ]),
                get_municipality_graph(fn, 'צפיפות אוכלוסיה', 'נפשות לקמ״ר', [
                    ('צפיפות בשטח בנוי', 'צפיפות אוכלוסייה (נפשות לקמ"ר)'),
                    ('צפיפות בשטח בנוי למגורים', 'צפיפות אוכלוסייה לשטח בנוי למגורים (נפשות לקמ"ר)'),
                ]),
                get_municipality_graph(fn, 'דירות למגורים', 'מספר דירות למגורים', [
                    ('לפי חיובי ארנונה', 'מספר דירות למגורים לפי חיובי ארנונה'),
                    ('לפי מרשם מבנים ודירות', 'מספר דירות למגורים לפי מרשם מבנים ודירות'),
                ]),
            ])
        ),
        dict(
            title='כספים',
            long_title='הכנסות והוצאות הרשות',
            subcharts=[
                spending_analysis_chart,
                get_municipality_graph(fn, 'הוצאות הרשות', 'אלפי ש״ח', [
                    ('תקציב רגיל', 'סה"כ הוצאות של הרשות בתקציב רגיל (אלפי ש"ח)'),
                    ('תקציב בלתי רגיל', 'סה"כ הוצאות של הרשות בתקציב בלתי רגיל (אלפי ש"ח)'),
                ]),
                get_municipality_graph(fn, 'סוגי הוצאות', 'אחוז', [
                    ('תקציב רגיל', 'סה"כ הוצאות של הרשות בתקציב רגיל (אלפי ש"ח)'),
                    ('תקציב בלתי רגיל', 'סה"כ הוצאות של הרשות בתקציב בלתי רגיל (אלפי ש"ח)'),
                ], mode='stacked-100'),
                get_municipality_graph(fn, 'סוגי הכנסות', 'אחוז', [
                    ('הכנסות עצמיות', 'הכנסות של הרשות - הכנסות עצמיות של הרשות (אלפי ש"ח)'),
                    ('הכנסות מהממשלה', 'הכנסות של הרשות - הכנסות מהממשלה (אלפי ש"ח)'),
                ], mode='stacked-100'),

            ]
        ),
        dict(
            title='חינוך',
            long_title='מה מצב מערכת החינוך ברשות המקומית?',
            subcharts=[
                get_municipality_graph(fn, 'מספר בתי ספר לפי סוג', 'בתי ספר', [
                    ('בתי״ס יסודיים וחינוך מיוחד', 'בתי ספר יסודיים (כולל חינוך מיוחד)'),
                    ('חטיבות ביניים',),
                    ('בתי״ס תיכוניים', 'בתי ספר תיכוניים'),
                ]),
                get_municipality_graph(fn, 'מספר כיתות לפי סוג', 'כיתות', [
                    ('בתי״ס יסודיים וחינוך מיוחד', 'כיתות בבתי ספר יסודיים (כולל חינוך מיוחד)'),
                    ('חטיבות ביניים', 'כיתות בחטיבות ביניים'),
                    ('בתי״ס תיכוניים', 'כיתות בבתי ספר תיכוניים'),
                ]),
                get_municipality_graph(fn, 'מספר תלמידים לפי סוג', 'תלמידים', [
                    ('גנים של משרד החינוך', 'ילדים בגנים של משרד החינוך - סה"כ'),
                    ('בתי״ס יסודיים וחינוך מיוחד', 'תלמידים בבתי ספר יסודיים (כולל חינוך מיוחד)'),
                    ('חטיבות ביניים', 'תלמידים בחטיבות ביניים'),
                    ('בתי״ס תיכוניים', 'תלמידים בבתי ספר תיכוניים'),
                ]),
                get_municipality_graph(fn, 'זכאים לבגרות', 'אחוז זכאים', [
                    ('אחוז זכאים לתעודת בגרות מבין תלמידי כיתות יב',),
                ]),
                get_municipality_graph(fn, 'תקציב החינוך', 'אלפי ₪', [
                    ('סה״כ הוצאה לחינוך', 'הוצאות של הרשות לחינוך (אלפי ש"ח)',),
                    ('השתתפות ממשלתית בתקציב החינוך', 'הכנסות של הרשות - הכנסות של הרשות ממשרד החינוך (אלפי ש"ח)'), 
                ]),
            ]
        ),
        dict(
            title='רווחה',
            long_title=' מאפייני רווחה ברשות המקומית',
            subcharts=[
                get_municipality_graph(fn, 'מדד-חברתי כלכלי', 'דירוג הרשות, 1 הנמוך ביותר', [
                    ('מדד חברתי-כלכלי - דירוג', 'מדד חברתי-כלכלי - דירוג (1 הנמוך ביותר)'),
                ]),
                get_municipality_graph(fn, 'מקבלי קיצבאות לפי סוג', 'מקבלי קיצבאות', [
                    ('ילדים מקבלי קיצבאות', 'מספר הילדים מקבלי קצבאות בגין ילדים - סה"כ'),
                    ('זקנה ושאירים', 'מקבלי קצבאות זקנה ושאירים - סה"כ'),
                    ('הבטחת הכנסה', 'מקבלי הבטחת הכנסה (נפשות)'),
                    ('דמי אבטלה', 'מקבלי דמי אבטלה - סה"כ'),
                    ('גמלאות סיעוד', 'מקבלי גמלאות סיעוד'),
                    ('גמלאות נכות מעבודה ותלויים', 'מקבלי גמלאות נכות מעבודה ותלויים'),
                    ('גמלאות נכות כללית', 'מקבלי גמלאות נכות כללית'),
                    ('גמלאות ניידות', 'מקבלי גמלאות ניידות'),
                ]),
                get_municipality_graph(fn, 'שכר חודשי ממוצע (לשכירים)', '₪ לחודש', [
                    ('סה״כ', 'שכר חודשי ממוצע של שכירים (ש"ח)'),
                    ('גברים', 'שכר חודשי ממוצע של גברים שכירים (ש"ח)'),
                    ('נשים', 'שכר חודשי ממוצע של נשים שכירות (ש"ח)'),
                ]),
                get_municipality_graph(fn, 'תקציב הרווחה', 'אלפי ₪', [
                    ('סה״כ הוצאה לרווחה', 'הוצאות של הרשות לרווחה (אלפי ש"ח)',),
                    ('השתתפות ממשלתית בתקציב לרווחה', 'הכנסות של הרשות - הכנסות של הרשות ממשרד הרווחה (אלפי ש"ח)'), 
                ]),
            ]
        ),
    ]
    return charts


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
            formatted_names, details = get_municipality_details(row)
            row['details'].update(details)
            charts = get_municipality_charts(formatted_names, spending_analysis_chart)
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
