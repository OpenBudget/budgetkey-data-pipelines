import logging
import requests

# ALL_PACKAGES_URL = 'https://data.gov.il/api/3/action/package_search?rows=10000'
SEARCH_RESOURCE_URL = 'https://data.gov.il/api/action/resource_search'


# def get_all_packages():
#     return requests.get(ALL_PACKAGES_URL).json()['result']['results']


def search_resource(name):
    results = requests.get(SEARCH_RESOURCE_URL, params=dict(query='name:'+name)).json()['result']['results']
    results = [
        r for r in results
        if r['name'] == name
    ]
    assert len(results) == 1, 'Failed to find result for name %s: %r' % (name, results)
    return results[0]


def get_resource(dataset_name, resource_name):
    return search_resource(resource_name)
