import logging

import requests
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from urllib.parse import urljoin
from nominations_page import NominationsPage
from http_client import HttpClient

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, _ = ingest()

INIT_RELATIVE_URL = '/Ext/Comp/NominationList/CdaNominationList_Iframe/1,15014,L-0-1,00.html?parms=5543'
BASE_URL = 'http://www.calcalist.co.il'
CALCALIST_INIT_URL = urljoin(BASE_URL, INIT_RELATIVE_URL)


def scrape():
    logging.info('Scraping calcalist')

    url = CALCALIST_INIT_URL
    logging.info('Initial URL: %s' % url)
    i = 0
    while url is not None:
        logging.info('iteration #%d with url: %s' % (i, url))
        page_source = HttpClient.download_page_source(url)
        nominations_page = NominationsPage(url, BASE_URL, page_source)

        for nomination in nominations_page.nominations():
            nomination['source'] = 'calcalist'
            yield nomination

        url = nominations_page.next_url()
        logging.info('next URL: %s' % url)
        i += 1


datapackage['resources'].append({
            'path': 'data/nominations-list.csv',
            'name': 'nominations-list',
            PROP_STREAMING: True,
            'schema': {
                'fields': [
                    {'name': 'date', 'type': 'string'},
                    {'name': 'full_name', 'type': 'string'},
                    {'name': 'company', 'type': 'string'},
                    {'name': 'description', 'type': 'string'},
                    {'name': 'proof_url', 'type': 'string'},
                    {'name': 'source', 'type': 'string'}
                ]
            }
})

spew(datapackage, [scrape()])


