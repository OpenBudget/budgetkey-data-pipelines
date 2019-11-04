import os
import logging
import requests
import json

# ALL_PACKAGES_URL = 'https://data.gov.il/api/3/action/package_search?rows=10000'
PACKAGE_GET_URL = 'https://data.gov.il/api/action/package_show?id='
BASE_PATH = os.path.dirname(__file__)

# def get_all_packages():
#     return requests.get(ALL_PACKAGES_URL).json()['result']['results']


def search_dataset(gcd, dataset_name):
    results = gcd.json(PACKAGE_GET_URL + dataset_name)
    return results['result']


def get_resource(gcd, dataset_name, resource_name):
    dataset = search_dataset(gcd, dataset_name)
    for resource in dataset['resources']:
        if resource['name'] == resource_name:
            return gcd.download(resource['url'].replace('//e.', '//'))
    assert False, 'Failed to find resource for name %s' % (resource_name,)

