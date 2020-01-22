import os
import logging
import requests
import json
import time
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

def get_page(gcd, url, test):
    try:
        for i in range(3):
            response = requests.get(url, headers={'User-Agent':'datagov-internal-client'})
            if response.status_code == 200:
                page = response.text
                break
            time.sleep(5)
        assert test(page)
    except:
        gcd.driver.get(url)
        page = gcd.driver.page_source
        assert test(page)
    return pq(page)


def get_dataset_html(gcd, dataset_name):
    page = get_page(gcd, PACKAGE_PAGE_URL + dataset_name, lambda page: 'resource-item' in page)
    resources = page.find('#dataset-resources .resource-item')
    results = []
    for resource in resources:
        resource = pq(resource)
        resource_page = get_page(gcd, 'http://data.gov.il' + pq(resource.find('a.heading')).attr('href'), lambda page: 'module-content' in page)
        results.append(dict(
            name=pq(resource.find('a.heading')).attr('title'),
            url='https://data.gov.il' + pq(resource_page.find('.module .module-content:first-child p.muted.ellipsis a.btn-primary')).attr('href'),
            # format=pq(resource.find('span.format-label')).text().lower(),
            # any_file=False
        ))

    return dict(resources=results)


def get_resource(gcd, dataset_name, resource_name):
    try:
        dataset = search_dataset(gcd, dataset_name)
    except:
        dataset = get_dataset_html(gcd, dataset_name)
    for resource in dataset['resources']:
        if resource['name'] == resource_name:
            url = resource['url'].replace('//e.', '//')
            try:
                if resource.get('any_file'):
                    return url, gcd.download(url, any_file=True, format='.' + resource['format'])
                else:
                    return url, gcd.download(url)
            except AssertionError:
                return url, None
    assert False, 'Failed to find resource for name %s' % (resource_name,)

