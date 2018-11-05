import logging
import requests
import re
from urllib.parse import urljoin

from time import sleep
from datetime import datetime

from pyquery import PyQuery as pq

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING

site = "https://mof.gov.il"
INITIAL_URL = "/GCA/Directors/Pages/DirectorsManningReport.aspx"

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

parameters, datapackage, _ = ingest()
session = requests.Session()


def mark_first(iter):
    is_first = True
    for res in iter:
        res['is_latest'] = is_first
        is_first = False
        yield res


def scrape():
    current_page = 0

    next_url = INITIAL_URL


    while next_url is not None:
        page = pq(session.get(urljoin(site,next_url)).text)
        sleep(3)


        # Iterate over the available documents and get their individual URLs
        for elem in page.find('.dlf a'):
            if 'דירקטור' in pq(elem).attr('title'):
                date_str = re.search('\d+\.\d+\.\d+', pq(elem).attr('title')).group(0)
                try:
                    date = datetime.strptime(date_str, '%d.%m.%Y')
                except ValueError:
                    date = datetime.strptime(date_str, '%d.%m.%y')

                yield {
                    'date': date,
                    'url': urljoin(site,pq(elem).attr('href'))
                }

        next_url = page.find("div.aNextLabelStyle a.resultsPagingText").attr('href')
        current_page += 1

datapackage['resources'].append({
    'path': 'data/reports.csv',
    'name': 'reports',
    PROP_STREAMING: True,
    'schema': {
        'fields': [
            {'name': 'url', 'type': 'string'},
            {'name': 'date', 'type': 'date'},
            {'name': 'is_latest', 'type': 'boolean'}
        ]
    }
})




spew(datapackage, [mark_first(scrape())])


