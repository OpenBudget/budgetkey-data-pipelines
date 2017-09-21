from itertools import chain

import requests
import logging
from pyquery import PyQuery as pq

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()
resource_name = parameters['resource-name']
session = requests.session()


def query_uris_from_node(node_uri):
    single = session.get(node_uri, verify=False).text
    files = pq(single).find('span.file a')
    for a in files:
        href = pq(a).attr('href')
        if 'xls' not in href.lower():
            continue
        yield href


def query_uris_from_nodes(rows):
    for row in rows:
        for uri in query_uris_from_node(row['node-url']):
            row['report-url'] = uri
            yield row


def handle_resources(resources):
    for resource in resources:
        if resource.spec['name'] == resource_name:
            yield query_uris_from_nodes(resource)
        else:
            yield resource

resource = next(filter(
    lambda r: r['name'] == resource_name,
    datapackage['resources']
))
resource['schema']['fields'].append({
    'name': 'report-url',
    'type': 'string',
    'format': 'uri'
})
spew(datapackage, handle_resources(res_iter))