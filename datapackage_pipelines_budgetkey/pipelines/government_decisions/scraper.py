# coding=utf-8
import requests
from itertools import chain
from datapackage_pipelines.utilities.resources import PROP_STREAMING

from pyquery import PyQuery as pq

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()

SEARCH_PAGE_RESULTS_URL = "https://www.gov.il/he/api/PolicyApi/Index?PmoMinistersComittee=&skip={skip}&limit=100000"


def get_decision_list():
    session = requests.Session()
    response = session.get(SEARCH_PAGE_RESULTS_URL.format(skip=0)).json()
    results = response['results']
    count = 0
    while True:
        for result in results:
            content_pq = pq(result['Content']) if result['Content'] else None
            yield {
                'text': content_pq.text() if content_pq else '',
                'document': content_pq.find('a').attr['href'] if '<a href=' in result['Content'] else '',
                'doc_published_date': result['DocPublishedDate'],
                'doc_update_date': result['DocUpdateDate'],
                'id': result['ItemUniqueId'],
                'office': result['OfficeDesc'][0] if result['OfficeDesc'] else '',
                'government': result['PmoGovernmentDesc'][0] if result['PmoGovernmentDesc'] else '',
                'policy_type': result['PolicyTypeDesc'][0] if result['PolicyTypeDesc'] else '',
                'procedure_number': result['ProcedureNumberNumeric'],
                'publish_date': result['PublishDate'],
                'publish_date_prod': result['PublishProd'],
                'title': result['Title'],
                'unit': result['UnitsDesc'][0] if result['UnitsDesc'] else '',
                'update_date': result['UpdateDate'],
                'url_id': result['UrlName'],
            }
            count += 1
        response = session.get(SEARCH_PAGE_RESULTS_URL.format(skip=count)).json()
        results = response['results']
        if not results:
            return


resource = parameters['resource']
resource[PROP_STREAMING] = True
schema = {
    'fields': [
        {'name': 'text', 'type': 'string'},
        {'name': 'document', 'type': 'string'},
        {'name': 'doc_published_date', 'type': 'datetime', 'format': '%Y-%m-%dT%H:%M:%SZ'},
        {'name': 'doc_update_date', 'type': 'datetime', 'format': '%Y-%m-%dT%H:%M:%SZ'},
        {'name': 'id', 'type': 'string'},
        {'name': 'office', 'type': 'string'},
        {'name': 'government', 'type': 'string'},
        {'name': 'policy_type', 'type': 'string'},
        {'name': 'procedure_number', 'type': 'integer'},
        {'name': 'publish_date', 'type': 'datetime', 'format': '%Y-%m-%dT%H:%M:%SZ'},
        {'name': 'publish_date_prod', 'type': 'datetime', 'format': '%Y-%m-%dT%H:%M:%SZ'},
        {'name': 'title', 'type': 'string'},
        {'name': 'unit', 'type': 'string'},
        {'name': 'update_date', 'type': 'datetime', 'format': '%Y-%m-%dT%H:%M:%SZ'},
        {'name': 'url_id', 'type': 'string'},
    ]
}
resource['schema'] = schema
datapackage['resources'].append(resource)

spew(datapackage, chain(res_iter, [get_decision_list()]))
