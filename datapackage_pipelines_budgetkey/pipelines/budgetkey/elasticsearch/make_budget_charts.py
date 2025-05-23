import os

import datapackage
from copy import deepcopy
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew
from decimal import Decimal

from sqlalchemy import create_engine, text

parameters, dp, res_iter = ingest()


gdp = dict(datapackage.Package('./data/gdp/datapackage.json').resources[0].iter())
inflation = dict(datapackage.Package('./data/inflation/datapackage.json').resources[0].iter())
population = dict(datapackage.Package('./data/population/datapackage.json').resources[0].iter())
infl_pop = dict((k,v*inflation[k]) for k, v in population.items() if k in inflation)
totals = dict(
    (int(row['year']), row['net_revised'])
    for row in
    datapackage.Package('/var/datapackages/budget/national/processed/totals/datapackage.json').resources[0].iter(keyed=True)
)

ECON_TRANSLATIONS = {'capital_expenditure': 'הוצאות הון', 'debt_repayment_principal': 'החזר חוב - קרן',
                     'debt_repayment_interest': 'החזר חוב - ריבית',
                     'income_bank_of_israel': 'הכנסות המדינה - בנק ישראל', 'income': 'הכנסות המדינה - הכנסות',
                     'income_loans': 'הכנסות המדינה - מילוות', 'income_grants': 'הכנסות המדינה - מענקים',
                     'dedicated_income': 'הכנסות מיועדות', 'transfers': 'העברות ותמיכות',
                     'internal_transfers': 'העברות פנים תקציביות', 'investment': 'השקעה',
                     'interim_accounts': 'חשבונות מעבר', 'credit': 'מתן אשראי', 'procurement': 'קניות ורכש',
                     'reserve': 'רזרבות', 'salaries': 'שכר'}

LEFT_OFFSET = 100
RIGHT_OFFSET = -100
HEIGHT = 100


def mushonkey_chart(title, groups):
    num_items = max(
        sum(
            len(group['flows'])
            for group in groups
            if group['leftSide'] == side
        )
        for side in [True, False]
    )
    height = LEFT_OFFSET - RIGHT_OFFSET + HEIGHT + num_items*30 + 100
    return {
        'type': 'mushonkey',
        'width': '100%',
        'height': '%spx' % height,
        'centerWidth': 200,
        'centerHeight': 75,
        'centerTitle': title,
        'directionLeft': True,
        'groups': groups
    }


def mushonkey_group(offset, width, left, klass, flows):
    return {
        'offset': offset,
        'class': klass,
        'width': width,
        'leftSide': left,
        'slope': 1.2,
        'roundness': 50,
        'labelTextSize': 14,
        'flows': flows
    }


def mushonkey_flow(size, label, id):
    return {
        'size': size,
        'label': label,
        'context': id
    }

def budget_id(code, row):
    if code is not None:
        return 'budget/%s/%s' % (code, row['year'])
    else:
        return None


def budget_sankey(row, kids):
    groups = []
    if row.get('hierarchy'):
        parent_code = row['hierarchy'][-1][0]
        if parent_code != '00':
            parent = mushonkey_flow(
                row['net_revised'],
                row['hierarchy'][-1][1] + ' ›',
                budget_id(parent_code, row)
            )
            groups.append(mushonkey_group(-100, 1.0, False, 'budget-parent', [parent]))
    expenses = []
    revenues = []
    for child in sorted(kids, key=lambda x: abs(x['amount'])):
        amount = child['amount']
        if amount != 0:
            if amount < 0:
                revenues.append(mushonkey_flow(
                    -amount,
                    child['label'],
                    budget_id(child.get('extra'), row)
                ))
            elif amount > 0:
                expenses.append(mushonkey_flow(
                    amount,
                    child['label'],
                    budget_id(child.get('extra'), row)
                ))
    if expenses:
        groups.append(mushonkey_group(100, 1.0, True, 'budget-expense', expenses))
    if revenues:
        groups.append(mushonkey_group(100, 0.8, False, 'budget-revenues', revenues))
    return mushonkey_chart(row['title'], groups)


def category_sankey(row, prefix, translations={}):
    kids = [
        {
            'label': translations.get(k[len(prefix):], k[len(prefix):]),
            'amount': v,
        }
        for k, v in row.items()
        if k.startswith(prefix) and v is not None
    ]
    row_ = deepcopy(row)
    del row_['hierarchy']
    return budget_sankey(row_, kids)


CAT1_QUERY="""SELECT substring(code
FROM 1
FOR 4) AS extra,
'‹ ' || (((hierarchy->>1)::json)->>1) as label,
sum(net_revised) AS amount
FROM raw_budget
WHERE func_cls_title_1->>0='{}'
AND length(code)=8
AND YEAR={}
GROUP BY 1, 2"""

CAT2_QUERY="""SELECT substring(code
FROM 1
FOR 4) AS extra,
'‹ ' || (((hierarchy->>1)::json)->>1) as label,
sum(net_revised) AS amount
FROM raw_budget
WHERE func_cls_title_1->>0='{}'
AND func_cls_title_2->>0='{}'
AND length(code)=8
AND YEAR={}
GROUP BY 1, 2"""

engine = create_engine(os.environ['DPP_DB_ENGINE']).connect()


def query_based_charts(row):
    if row['code'].startswith('C'):
        if len(row['func_cls_title_2']) == 1:
            query = CAT2_QUERY.format(
                row['func_cls_title_1'][0],
                row['func_cls_title_2'][0],
                row['year']
            )
        else:
            query = CAT1_QUERY.format(
                row['func_cls_title_1'][0],
                row['year']
            )
        result = engine.execute(text(query))
        result = list(r._asdict() for r in result)
        chart = budget_sankey(row, result)
        yield 'חלוקה למשרדים', 'אילו משרדים ממשלתיים מטפלים בנושא זה?', 'פירוט הסעיפים התקציביים הראשיים הקשורים לנושא זה ', chart


def history_chart(row, normalisation=None, normalisation_unit='אחוזים'):
    traces = []
    history = row.get('history', [])
    history = dict(
        (int(k), v)
        for k, v in history.items()
    )
    if history is not None and len(history) > 0:
        history[row['year']] = row
        years = sorted(history.keys())
        if normalisation is not None:
            years = [year for year in years if year in normalisation]
            normalisation = [normalisation[year] for year in years]
        if len(years) < 2:
            return None, None
        unit = '?'
        for measure, name in (
                ('net_allocated', 'תקציב מקורי'),
                ('net_revised', 'אחרי שינויים'),
                ('net_executed', 'ביצוע בפועל')
        ):
            if normalisation is not None:
                values = [Decimal(history[year].get(measure))/Decimal(factor)*100
                          if history[year].get(measure) is not None
                          else None
                          for year, factor
                          in zip(years, normalisation)]
                unit = normalisation_unit
            else:
                values = [history[year].get(measure) for year in years]
                unit = 'תקציב ב-₪'

            trace = {
                'x': [int(y) for y in years],
                'y': values,
                'mode': 'lines+markers',
                'name': name
            }
            traces.append(trace)
        layout = {
            'xaxis': {
                'title': 'שנה',
                'type': 'category'
            },
            'yaxis': {
                'title': unit,
                'rangemode': 'tozero',
                'separatethousands': True,
            }
        }
        return traces, layout
    return None, None


def admin_hierarchy_chart(row):
    if row.get('children'):
        # Admin Hierarchy chart
        kids = [
            {
                'label': '‹ (…{}) {}'.format(child['code'][-2:], child['title']),
                'extra': child['code'],
                'amount': child['net_revised'],
            }
            for child in row.get('children')
        ]
        return budget_sankey(row, kids)
    return None


def process_resource(res_):
    for row in res_:
        row['charts'] = []
        chart = admin_hierarchy_chart(row)
        if chart is not None:
            row['charts'].append(
                {
                    'title': 'מבנה התקציב',
                    'long_title': 'מהם תתי הסעיפים של סעיף זה?',
                    'description': None,
                    'type': 'mushonkey',
                    'chart': chart,
                }
            )
        for title, long_title, description, chart in query_based_charts(row):
            row['charts'].append(
                {
                    'title': title,
                    'long_title': long_title,
                    'description': description,
                    'type': 'mushonkey',
                    'chart': chart,
                }
            )
        change_charts = []
        if not row['code'].startswith('0000'):
            chart, layout = history_chart(row)
            if chart is not None:
                change_charts.append(
                    {
                        'title': 'נומינלית',
                        'long_title': 'ערכי התקציב לאורך השנים (כפי שהופיעו בספרי התקציב)',
                        'type': 'plotly',
                        'chart': chart,
                        'layout': layout
                    }
                )
            if row['code'].startswith('C') or len(row['code']) <= 4:
                chart, layout = history_chart(row, normalisation=totals)
                if chart is not None:
                    change_charts.append(
                        {
                            'title': 'באחוזים מכלל התקציב',
                            'long_title': 'ערכי התקציב לאורך השנים כאחוז מכלל התקציב',
                            'type': 'plotly',
                            'chart': chart,
                            'layout': layout
                        }
                    )
                chart, layout = history_chart(row, normalisation=gdp)
                if chart is not None:
                    change_charts.append(
                        {
                            'title': 'לעומת התוצר המקומי הגולמי',
                            'long_title': 'ערכי התקציב לאורך השנים כאחוז מהתוצר הלאומי הגולמי',
                            'type': 'plotly',
                            'chart': chart,
                            'layout': layout
                        }
                    )
                chart, layout = history_chart(row, normalisation=inflation, normalisation_unit='מחירי 2016')
                if chart is not None:
                    change_charts.append(
                        {
                            'title': 'בערכים ריאליים',
                            'long_title': 'ערכי התקציב לאורך השנים, מותאמים לאינפלציה (במחירי 2016)',
                            'type': 'plotly',
                            'chart': chart,
                            'layout': layout
                        }
                    )
                chart, layout = history_chart(row, normalisation=infl_pop, normalisation_unit='תקציב לתושב, מחירי 2016')
                if chart is not None:
                    change_charts.append(
                        {
                            'title': 'תקציב ריאלי לתושב',
                            'long_title': 'ערכי התקציב לאורך השנים, מנורמלים לפי גודל האוכלוסיה ומותאמים לאינפלציה (במחירי 2016)',
                            'type': 'plotly',
                            'chart': chart,
                            'layout': layout
                        }
                    )
            if change_charts:
                row['charts'].append({
                    'title': 'שינויי תקציב',
                    'long_title': 'איך התקציב בסעיף זה השתנה לאורך השנים?',
                    'subcharts': change_charts
                })
        else:
            chart, layout = history_chart(row)
            if chart is not None:
                change_charts.append(
                    {
                        'title': 'נומינלית',
                        'long_title': 'ערכי ההכנסה לאורך השנים (כפי שהופיעו בספרי התקציב)',
                        'type': 'plotly',
                        'chart': chart,
                        'layout': layout
                    }
                )
            chart, layout = history_chart(row, normalisation=totals)
            if chart is not None:
                change_charts.append(
                    {
                        'title': 'באחוזים מכלל התקציב',
                        'long_title': 'ערכי ההכנסה לאורך השנים כאחוז מכלל התקציב',
                        'type': 'plotly',
                        'chart': chart,
                        'layout': layout
                    }
                )
            chart, layout = history_chart(row, normalisation=gdp)
            if chart is not None:
                change_charts.append(
                    {
                        'title': 'לעומת התוצר המקומי הגולמי',
                        'long_title': 'ערכי ההכנסה לאורך השנים כאחוז מהתוצר הלאומי הגולמי',
                        'type': 'plotly',
                        'chart': chart,
                        'layout': layout
                    }
                )
            if change_charts:
                row['charts'].append({
                    'title': 'שינויי הכנסה',
                    'long_title': 'איך ההכנסה בסעיף זה השתנה לאורך השנים?',
                    'subcharts': change_charts
                })
        chart = category_sankey(row, 'total_econ_cls_', ECON_TRANSLATIONS)
        if chart is not None:
            row['charts'].append(
                {
                    'title': 'דרכי שימוש',
                    'long_title': 'באילו דרכים ושיטות מוציאים את הכסף בסעיף זה?',
                    'type': 'mushonkey',
                    'chart': chart
                }
            )
        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_resource(first)
    yield from res_iter_


dp['resources'][0]['schema']['fields'].append(
    {
        'name': 'charts',
        'type': 'array',
        'es:itemType': 'object',
        'es:index': False
    }
)

spew(dp, process_resources(res_iter))
