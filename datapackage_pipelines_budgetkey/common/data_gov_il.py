import os
import logging
import requests
import json
import time
import shutil
import tempfile
from pyquery import PyQuery as pq

# ALL_PACKAGES_URL = 'https://data.gov.il/api/3/action/package_search?rows=10000'
PACKAGE_GET_URL = 'https://data.gov.il/api/action/package_show?id='
PACKAGE_PAGE_URL = 'https://data.gov.il/dataset/'
BASE_PATH = os.path.dirname(__file__)
HEADERS = {
    'User-Agent': 'datagov-external-client'
}
# def get_all_packages():
#     return requests.get(ALL_PACKAGES_URL, headers=HEADERS).json()['result']['results']


def search_dataset(gcd, dataset_name):
    try:
        results = requests.get(PACKAGE_GET_URL + dataset_name, headers=HEADERS).json()
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
            response = requests.get(url, headers=HEADERS)
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
        resource_page = get_page(gcd, 'https://data.gov.il' + pq(resource.find('a.heading')).attr('href'), lambda page: 'module-content' in page)
        file_url = pq(resource_page.find('a.resource-url-analytics')).attr('href')
        if not file_url.startswith('https://'):
            file_url = 'https://data.gov.il' + file_url
        results.append(dict(
            name=pq(resource.find('a.heading')).attr('title'),
            url=file_url,
            # format=pq(resource.find('span.format-label')).text().lower(),
            # any_file=False
        ))

    return dict(resources=results)

def get_resource_by_id(dataset_id, resource_id, extension='csv'):
    url = f'https://data.gov.il/dataset/{dataset_id}/resource/{resource_id}/download/{resource_id}.{extension}'
    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix=os.path.basename(url)) as downloaded:
        resp = requests.get(url, stream=True, headers=HEADERS)
        amount = 0
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                downloaded.write(chunk)
                amount += len(chunk)
        downloaded.close()
        data = open(downloaded.name, 'rb').read()
        assert data[:5] != b'<html', 'GOT HTML RESPONSE FOR URL %s: %r' % (url, data)
        # logging.info('%s/%s -> %s %d bytes (%r...%r)',
        #              dataset_name, resource_name, downloaded.name,
        #              amount, data[:256], data[-256:])
        return url, downloaded.name

def get_resource(gcd, dataset_name, resource_name):
    try:
        dataset = search_dataset(gcd, dataset_name)
    except:
        dataset = get_dataset_html(gcd, dataset_name)
    for resource in dataset['resources']:
        if resource['name'].strip() == resource_name:
            url = resource['url'].replace('//e.', '//')
            try:
                try:
                    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix=os.path.basename(url)) as downloaded:
                        resp = requests.get(url, stream=True, headers=HEADERS)
                        amount = 0
                        for chunk in resp.iter_content(chunk_size=8192):
                            if chunk:
                                downloaded.write(chunk)
                                amount += len(chunk)
                        downloaded.close()
                        data = open(downloaded.name, 'rb').read()
                        assert data[:5] != b'<html'
                        # logging.info('%s/%s -> %s %d bytes (%r...%r)',
                        #              dataset_name, resource_name, downloaded.name,
                        #              amount, data[:256], data[-256:])
                        return url, downloaded.name
                except Exception:
                    if callable(gcd):
                        gcd = gcd()
                    if resource.get('any_file'):
                        return url, gcd.download(url, any_file=True, format='.' + resource['format'])
                    else:
                        return url, gcd.download(url)
            except AssertionError:
                return url, None
    assert False, 'Failed to find resource for name %s: possibilities are %r' % (resource_name, [r['name'] for r in dataset['resources']])
