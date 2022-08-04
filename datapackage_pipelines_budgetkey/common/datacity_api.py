import requests
import logging


def hit_datacity_api(query):
    url = 'https://api.datacity.org.il/api/query'
    params = dict(
        query=query
    )
    for i in range(3):
        try:
            resp = requests.get(url, params=params, timeout=30).json()
        except requests.exceptions.RequestException:
            continue
        break
    if resp.get('error'):
        assert False, resp['error']
    if len(resp['rows']) == 0:
        logging.info('EMPTY QUERY %s', query)
    return resp['rows']
