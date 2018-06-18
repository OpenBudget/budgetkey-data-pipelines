import logging
import requests
from datapackage_pipelines.wrapper import process 
from requests import HTTPError

url_prefix="https://www.mr.gov.il"


def _get_url_response_text(url, timeout=180):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) ' +
                        'AppleWebKit/537.36 (KHTML, like Gecko) ' +
                        'Chrome/54.0.2840.87 Safari/537.36'
    }
    response = requests.get(url, timeout=timeout, headers=headers)
    response.raise_for_status()
    return response.text


def process_row(row, row_index,
                spec, resource_index,
                parameters, stats):
    if not row['__is_stale']:
        return None
    stats.setdefault('handled-urls', 0)
    if stats['handled-urls'] < 1500:
        try:
            stats['handled-urls'] += 1
            url = row['url']
            if not url.startswith("http"):
                url = '{}{}'.format(url_prefix, url)
            row.update(dict(
                url=url,
                data=_get_url_response_text(url),
            ))
            return row
        except HTTPError:
            stats.setdefault('failed-urls', 0)
            stats['failed-urls'] += 1
            logging.exception('Failed to load %s', url)


def modify_datapackage(dp, *_):
    dp['resources'][0]['name'] = 'tender-urls-downloaded-data'
    dp['resources'][0]['schema']['fields'].append(dict(
        name='data',
        title='page html data',
        type='string'
    ))
    return dp


if __name__ == '__main__':
    process(modify_datapackage=modify_datapackage,
            process_row=process_row)
