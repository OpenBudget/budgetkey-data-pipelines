import datapackage
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew
from decimal import Decimal

parameters, dp, res_iter = ingest()


gdp = dict(datapackage.Package('./data/gdp/datapackage.json').resources[0].iter())
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


def sankey_chart(nodes, links):
    for i, node in enumerate(nodes):
        node['id'] = i
    return [{
        "type": "sankey",
        "domain": {
            "x": [0,1],
            "y": [0,1]
        },
        "orientation": "h",
        "valueformat": ".0f",
        "valuesuffix": "₪",
        "arrangement": "fixed",
        "hoverinfo": "none",
        "customdata": [node['extra'] for node in nodes],
        "node": {
            "pad": 20,
            "thickness": 60,
            "line": {
                "color": "FF5A5F",
                "width": 0.5
            },
            "label": [node['label'] for node in nodes],
            "color": [node.get('color', '#FF5A5F') for node in nodes]
        },
        "link": {
            "source": [link['source']['id'] for link in links],
            "target": [link['target']['id'] for link in links],
            "value": [link['value'] for link in links],
            "label": [link.get('label', '') for link in links],
        }
    }], {}


def budget_sankey(row, kids):
    center_node = {
        'label': row['title'],
        'extra': row['code']
    }
    links = []
    nodes = [center_node]
    if row.get('hierarchy'):
        parent_node = {
            'label': row['hierarchy'][-1][1],
            'extra': row['hierarchy'][-1][0]
        }
        nodes.append(parent_node)
        links.append({
            'source': center_node,
            'target': parent_node,
            'value': row['net_revised'],
        })
    for child in sorted(kids, key=lambda x: abs(x['amount'])):
        amount = child['amount']
        if amount != 0:
            node = {
                'label': child['label'],
                'extra': child.get('extra')
            }
            nodes.append(node)
            if amount < 0:
                links.append({
                    'source': center_node,
                    'target': node,
                    'value': -amount,
                })
            elif amount > 0:
                links.append({
                    'source': node,
                    'target': center_node,
                    'value': amount,
                })
    return sankey_chart(nodes, links)


def category_sankey(row, prefix, translations={}):
    kids = [
        {
            'label': translations.get(k[len(prefix):], k[len(prefix):]),
            'amount': v,
        }
        for k, v in row.items()
        if k.startswith(prefix) and v is not None
    ]
    return budget_sankey(row, kids)


def query_based_charts(row):
    if False:
        yield None


def history_chart(row, normalisation=None):
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
                unit = 'אחוזים'
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
                'label': child['title'],
                'extra': child['code'],
                'amount': child['net_revised'],
            }
            for child in row.get('children')
        ]
        return budget_sankey(row, kids)
    return None, None


def process_resource(res_):
    for row in res_:
        row['charts'] = []
        chart, layout = admin_hierarchy_chart(row)
        if chart is not None:
            row['charts'].append(
                {
                    'title': 'לאן הולך הכסף?',
                    'chart': chart,
                    'layout': layout
                }
            )
        chart, layout = history_chart(row)
        if chart is not None:
            row['charts'].append(
                {
                    'title': 'איך השתנה התקציב?',
                    'chart': chart,
                    'layout': layout
                }
            )
        if row['code'].startswith('C'):
            chart, layout = history_chart(row, normalisation=gdp)
            if chart is not None:
                row['charts'].append(
                    {
                        'title': 'איך השתנה התקציב (לעומת כלל התקציב)?',
                        'chart': chart,
                        'layout': layout
                    }
                )
            chart, layout = history_chart(row, normalisation=totals)
            if chart is not None:
                row['charts'].append(
                    {
                        'title': 'איך השתנה התקציב (לעומת התוצר המקומי הגולמי)?',
                        'chart': chart,
                        'layout': layout
                    }
                )

        chart, layout = category_sankey(row, 'total_econ_cls_', ECON_TRANSLATIONS)
        if chart is not None:
            row['charts'].append(
                {
                    'title': 'איך משתמשים בתקציב?',
                    'chart': chart,
                    'layout': layout
                }
            )
        for title, chart, layout in query_based_charts(row):
            row['charts'].append(
                {
                    'title': title,
                    'chart': chart,
                    'layout': layout
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
