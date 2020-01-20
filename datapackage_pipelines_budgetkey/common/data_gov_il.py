import os
import logging
import requests
import json
from pyquery import PyQuery as pq

# ALL_PACKAGES_URL = 'https://data.gov.il/api/3/action/package_search?rows=10000'
PACKAGE_GET_URL = 'https://data.gov.il/api/action/package_show?id='
PACKAGE_PAGE_URL = 'https://data.gov.il/dataset/'
BASE_PATH = os.path.dirname(__file__)

# def get_all_packages():
#     return requests.get(ALL_PACKAGES_URL).json()['result']['results']


def search_dataset(gcd, dataset_name):
    try:
        results = requests.get(PACKAGE_GET_URL + dataset_name, headers={'User-Agent':'datagov-internal-client'}).json()
        logging.info('GOT REQUESTS JSON %s', results)
        results = results['result']
    except:
        results = gcd.json(PACKAGE_GET_URL + dataset_name)
        logging.info('GOT GCD JSON %s', results)
        results = results['result']
    return results


def get_dataset_html(gcd, dataset_name):
    try:
        page = requests.get(PACKAGE_PAGE_URL + dataset_name, headers={'User-Agent':'datagov-internal-client'}).text
    except:
        gcd.driver.get(PACKAGE_PAGE_URL + dataset_name)
        page = gcd.driver.page_source
    page = pq(page)
    resources = page.find('#dataset-resources .resource-item')
    results = []
    for resource in resources:
        resource = pq(resource)
        results.append(dict(
            name=pq(resource.find('a.heading')).attr('title'),
            url='https://data.gov.il' + pq(resource.find('.dropdown-menu li:last-child a')).attr('href')
        ))

    return dict(resources=results)


def get_resource(gcd, dataset_name, resource_name):
    try:
        dataset = search_dataset(gcd, dataset_name)
    except:
        dataset = get_dataset_html(gc, dataset_name)
    for resource in dataset['resources']:
        if resource['name'] == resource_name:
            url = resource['url'].replace('//e.', '//')
            try:
                return url, gcd.download(url)
            except AssertionError:
                return url, None
    assert False, 'Failed to find resource for name %s' % (resource_name,)

