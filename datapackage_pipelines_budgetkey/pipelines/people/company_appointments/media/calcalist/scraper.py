import logging
import time
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from urllib.parse import urljoin
from nominations_page import NominationsPage

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, _ = ingest()

INIT_RELATIVE_URL = '/Ext/Comp/NominationList/CdaNominationList_Iframe/1,15014,L-0-1,00.html?parms=5543'
BASE_URL = 'https://www.calcalist.co.il'

#TODO: Use Last seen as External
def scrape():
    logging.info('Scraping calcalist')
    url = INIT_RELATIVE_URL
    i = 0
    while url is not None:
        nominations_page = NominationsPage(urljoin(BASE_URL,url))
        time.sleep(1)
        for nomination in nominations_page.nominations():
            nomination['source'] = 'calcalist'
            yield nomination

        url = nominations_page.next_url()
        i += 1

def fix_dates(scraper):
    last_seen_date = None
    for nomination in scraper:
        if nomination['date']:
            last_seen_date = nomination['date']
        else:
            nomination['date'] = last_seen_date

        yield nomination

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

spew(datapackage, [fix_dates(scrape())])


