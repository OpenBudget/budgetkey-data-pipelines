import os
import logging
import requests
import json

# ALL_PACKAGES_URL = 'https://data.gov.il/api/3/action/package_search?rows=10000'
SEARCH_RESOURCE_URL = 'https://data.gov.il/api/action/resource_search'
BASE_PATH = os.path.dirname(__file__)

# def get_all_packages():
#     return requests.get(ALL_PACKAGES_URL).json()['result']['results']


def search_dataset(dataset_name):
    try:
        results = requests.get(SEARCH_RESOURCE_URL, params=dict(query='name:'+dataset_name)).json()
    except Exception:
        results = json.load(open(os.path.join(BASE_PATH, 'datagovil.json')))
    results = results['result']['results']
    results = [
        r for r in results
        if r['name'] == dataset_name
    ]
    assert len(results) == 1, 'Failed to find dataset for name %s: %r' % (dataset_name, results)
    return results[0]


def get_resource(dataset_name, resource_name):
    dataset = search_dataset(dataset_name)
    for resource in dataset['resources']:
        if resource['name'] == resource_name:
            return resource
    assert False, 'Failed to find resource for name %s' % (resource_name,)

