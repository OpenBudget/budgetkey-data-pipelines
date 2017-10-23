from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()


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
    }]


def query_based_charts(row):
    if False:
        yield None


def history_chart(row):
    traces = []
    history = row.get('history', [])
    if history is not None and len(history) > 0:
        years = sorted(history.keys())
        for measure, name in (
                ('net_allocated', 'מקורי'),
                ('net_revised', 'מקורי'),
                ('net_executed', 'מקורי')
        ):
            trace = {
                'x': [int(y) for y in years] + [row['year']],
                'y': [history[year].get(measure) for year in years] + [row.get(measure)],
                'mode': 'lines',
                'name': name
            }
            traces.append(trace)
    return traces


def admin_hierarchy_chart(row):
    if row.get('children'):
        # Admin Hierarchy chart
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
                'value': row['net_allocated'],
            })
        for child in sorted(row.get('children'), key=lambda x: abs(x['net_allocated'])):
            node = {
                'label': child['title'],
                'extra': child['code']
            }
            nodes.append(node)
            if child['net_allocated'] < 0:
                links.append({
                    'source': center_node,
                    'target': node,
                    'value': -child['net_allocated'],
                })
            elif child['net_allocated'] > 0:
                links.append({
                    'source': node,
                    'target': center_node,
                    'value': child['net_allocated'],
                })
        return sankey_chart(nodes, links)


def process_resource(res_):
    for row in res_:
        row['charts'] = []
        chart = admin_hierarchy_chart(row)
        if chart is not None:
            row['charts'].append(
                {
                    'title': 'לאן הולך הכסף?',
                    'chart': chart,
                }
            )
        chart = history_chart(row)
        if chart is not None:
            row['charts'].append(
                {
                    'title': 'איך השתנה התקציב?',
                    'chart': chart,
                }
            )
        for title, chart in query_based_charts(row):
            row['charts'].append(
                {
                    'title': title,
                    'chart': chart,
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
