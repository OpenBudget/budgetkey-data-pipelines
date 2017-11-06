import logging

from selenium import webdriver
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import urljoin
from pyquery import PyQuery as pq
from init_nominations_page import InitNominationsPage
from nominations_page import NominationsPage

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, _ = ingest()

CALCALIST_BASE_URL = 'http://www.calcalist.co.il'


def scrape():
    logging.info('Scraping calcalist')

    driver = webdriver.PhantomJS()
    driver.set_window_size(1200, 800)

    logging.info('Selenium driver initialized')

    full_init_url = urljoin(CALCALIST_BASE_URL, '/local/home/0,7340,L-3789,00.html')
    logging.info('Initial URL: %s' % full_init_url)
    driver.get(full_init_url)
    init_page = InitNominationsPage(CALCALIST_BASE_URL, driver.page_source)
    url = init_page.nominations_url()
    logging.info('IFrame URL: %s' % url)

    i = 0
    while url is not None:
        # while url is not None and i < 100:
        logging.info('iteration #%d with url: %s' % (i, url))
        driver.get(url)
        nominations_page = NominationsPage(url, CALCALIST_BASE_URL, driver.page_source)

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


