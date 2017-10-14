from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()


def sankey_chart(nodes, links):
    for i, node in enumerate(nodes):
        node['id'] = i
    return {
        "type": "sankey",
        "domain": {
            "x": [0,1],
            "y": [0,1]
        },
        "orientation": "h",
        "valueformat": ".0f",
        "valuesuffix": "₪",
        "node": {
            "pad": 15,
            "thickness": 15,
            "line": {
                "color": "black",
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
    }


def process_resource(res_):
    for row in res_:
        row['charts'] = []
        if row.get('children'):
            # Admin Hierarchy chart
            center_node = {
                'label': row['title']
            }
            links = []
            nodes = [center_node]
            if row.get('hierarchy'):
                parent_node = {
                    'label': row['hierarchy'][-1][1]
                }
                nodes.append(parent_node)
                links.append({
                    'source': center_node,
                    'target': parent_node,
                    'value': row['net_allocated'],
                })
            for child in sorted(row.get('children'), key=lambda x: abs(x['net_allocated'])):
                node = {
                    'label': child['title']
                }
                nodes.append(node)
                if child['net_allocated'] < 0:
                    links.append({
                        'source': center_node,
                        'target': node,
                        'value': -child['net_allocated'],
                    })
                else:
                    links.append({
                        'source': node,
                        'target': center_node,
                        'value': child['net_allocated'],
                    })
            row['charts'].append(
                {
                    'chart': sankey_chart(nodes, links),
                    'title': 'לאן הולך הכסף?'
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
