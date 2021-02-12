# coding=utf-8
import requests
from itertools import chain
from datapackage_pipelines_budgetkey.common.object_storage import object_storage
from datapackage_pipelines_budgetkey.common.sanitize_html import sanitize_html
from datapackage_pipelines.utilities.resources import PROP_STREAMING

from pyquery import PyQuery as pq

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()

SEARCH_PAGE_RESULTS_URL = "https://www.gov.il/he/api/PolicyApi/Index?PmoMinistersComittee=&skip={skip}&limit=1000"

SITE_URL = 'https://www.gov.il'
TIMEOUT = 60


def get_links(content, session):
    links = []
    if '<a' in content:
        for link in pq(content)('a'):
            if 'href' not in link.attrib:
                continue
            href = link.attrib['href']
            if href.startswith('/'):
                href = SITE_URL + href
            if not href.startswith('http'):
                continue
            if href in links:
                continue
            filename = href.rpartition('/')[2]
            if filename == '' or filename.endswith('.html') or filename.endswith('.aspx'):
                continue

            s3_object_name = 'government_decisions/' + filename
            if not object_storage.exists(s3_object_name):
                try:
                    conn = session.get(href, timeout=TIMEOUT)
                    if not conn.status_code == requests.codes.ok:
                        continue
                    href = object_storage.write(s3_object_name, data=conn.content, public_bucket=True, create_bucket=True)
                except:
                    continue
            else:
                href = object_storage.urlfor(s3_object_name)
            links.append(dict(href=href, title=pq(link).text()))
    return links


def get_decision_list():
    session = requests.Session()
    session.headers['User-Agent'] = 'datagov-external-client'
    response = session.get(SEARCH_PAGE_RESULTS_URL.format(skip=0), timeout=TIMEOUT).json()
    results = response['results']
    count = 0
    while True:
        for result in results:
            content_pq = pq(result['Content']) if result['Content'] else None
            links = get_links(result['Content'], session)
            content_pq = sanitize_html(content_pq)

            yield {
                'text': content_pq,
                'linked_docs': links,
                'doc_published_date': result['DocPublishedDate'],
                'doc_update_date': result['DocUpdateDate'],
                'id': result['ItemUniqueId'],
                'office': result['ConnectedOffices'][0]["Title"] if result.get('ConnectedOffices') else '',
                'government': result['PmoGovernmentDesc'][0] if result.get('PmoGovernmentDesc') else (result.get('PmoGovernment')[0] if result.get('PmoGovernment') else None),
                'policy_type': result['PolicyTypeDesc'][0] if result.get('PolicyTypeDesc') else '',
                'procedure_number': result['ProcedureNumberNumeric'],
                'procedure_number_str': result['ProcedureNumber'],
                'publish_date': result['PublishDate'],
                'publish_date_prod': result['PublishProd'],
                'title': result['Title'],
                'unit': result['UnitsDesc'][0] if result.get('UnitsDesc') else (result.get('Units')[0] if result.get('Units') else None),
                'update_date': result['UpdateDate'],
                'url_id': result['UrlName'],
                'score': 1
            }
            count += 1
        response = session.get(SEARCH_PAGE_RESULTS_URL.format(skip=count), timeout=TIMEOUT).json()
        results = response['results']
        if not results or len(results) == 0:
            return


resource = parameters['resource']
resource[PROP_STREAMING] = True
schema = {
    'fields': [
        {'name': 'text', 'type': 'string', 'es:hebrew': True},
        {
            "name": "linked_docs",
            "type": "array",
            "es:itemType": "object",
            "es:index": False
        },
        {'name': 'doc_published_date', 'type': 'datetime', 'format': '%Y-%m-%dT%H:%M:%SZ'},
        {'name': 'doc_update_date', 'type': 'datetime', 'format': '%Y-%m-%dT%H:%M:%SZ'},
        {'name': 'id', 'type': 'string', 'es:index': False},
        {'name': 'office', 'type': 'string'},
        {'name': 'government', 'type': 'string', 'es:keyword': True},
        {'name': 'policy_type', 'type': 'string', 'es:keyword': True},
        {'name': 'procedure_number', 'type': 'integer'},
        {'name': 'procedure_number_str', 'type': 'string', 'es:keyword': True},
        {'name': 'publish_date', 'type': 'datetime', 'format': '%Y-%m-%dT%H:%M:%SZ'},
        {'name': 'publish_date_prod', 'type': 'datetime', 'format': '%Y-%m-%dT%H:%M:%SZ'},
        {'name': 'title', 'type': 'string', 'es:title': True},
        {'name': 'unit', 'type': 'string'},
        {'name': 'update_date', 'type': 'datetime', 'format': '%Y-%m-%dT%H:%M:%SZ'},
        {'name': 'url_id', 'type': 'string', 'es:index': False},
        {'name': 'score', 'type': 'number', 'es:score-column': True}
    ]
}
resource['schema'] = schema
datapackage['resources'].append(resource)


spew(datapackage, chain(res_iter, [get_decision_list()]))
