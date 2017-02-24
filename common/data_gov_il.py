import requests

ALL_PACKAGES_URL = 'https://data.gov.il/api/3/action/package_search?rows=10000'


def get_all_packages():
    return requests.get(ALL_PACKAGES_URL).json()['result']['results']


def get_resource(dataset_name, resource_name):
    dataset = next(filter(lambda ds: ds['name'] == dataset_name, get_all_packages()))
    resource = next(filter(lambda res: res['name'] == resource_name, dataset['resources']))
    if 'e.data.gov.il' in resource['url']:
        resource['url'] = resource['url'].replace('e.data.gov.il', 'data.gov.il')
    return resource
