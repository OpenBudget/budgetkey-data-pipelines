from itertools import chain

import requests
import logging
from pyquery import PyQuery as pq

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()

URL = "https://foi.gov.il/he/search/site?page={0}&f[0]=im_field_mmdtypes%3A368"


def get_all_reports():
    page = 0
    session = requests.session()
    done = False
    while not done:
        results = session.get(URL.format(page), verify=False).text
        results = pq(results)
        results = results.find('.search-result')
        for result in results:
            result = pq(result).find('li')
            rec = {
                'node-url': pq(result[0].find('a')).attr('href'),
                'report-title': pq(result[0].find('a')).text(),
                'report-publisher': pq(result[1]).text(),
                'report-date': pq(result[2]).text()
            }
            logging.info(rec)
            yield rec
        if len(results) == 0:
            done = True
        page += 1


resource = parameters['target-resource']
resource['schema'] = {
    'fields': [
        {
            'name': 'report-date',
            'type': 'date',
            'format': 'fmt:%d.%m.%Y'
        },
        {
            'name': 'report-title',
            'type': 'string'
        },
        {
            'name': 'report-publisher',
            'type': 'string'
        },
        {
            'name': 'node-url',
            'type': 'string',
            'format': 'uri'
        }
    ]
}
datapackage['resources'].append(resource)

logging.info(datapackage)
spew(datapackage, chain(res_iter, [get_all_reports()]))