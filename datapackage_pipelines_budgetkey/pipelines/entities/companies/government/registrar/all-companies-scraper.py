import itertools
import logging
from time import sleep

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from selenium import webdriver
from selenium.webdriver.remote.remote_connection import LOGGER
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

from pyquery import PyQuery as pq

from datapackage_pipelines.wrapper import ingest, spew


LOGGER.setLevel(logging.WARNING)

parameters, datapackage, res_iter = ingest()

headers = ['name', 'href', 'ministry_responsible', 'last_activity_report_href', 'last_financial_report_href']

hostname = 'http://mof.gov.il'


def get_href(href):
    if href is None:
        return href
    else:
        return hostname + href


def get_element_from_row(row):
    element = {}
    row_data = pq(row).children()
    name_href_element = pq(row_data[0]).find('a')
    element['name'] = name_href_element.text().strip()
    element['href'] = get_href(name_href_element.attr['href'])

    element['ministry_responsible'] = pq(row_data[1]).find('.tableTRText').text().strip()

    last_activity_report_href_element = pq(row_data[2]).find('a')
    element['last_activity_report_href'] = get_href(last_activity_report_href_element.attr['href'])

    last_financial_report_href_element = pq(row_data[3]).find('a')
    element['last_financial_report_href'] = get_href(last_financial_report_href_element.attr['href'])

    return element


def verify_row_structure(row):
    expected_texts = ['שם חברה', 'משרד אחראי', 'דו"ח פעילות אחרון', 'דו"ח כספי אחרון']
    row_data = pq(row).children()
    for index in range(0, 3):
        text = pq(pq(row_data[index]).children()[0]).text()
        expected_text = expected_texts[index]
        assert text == expected_text, 'Cannot find tag: %r existing tag: %r. Check if page structure was changed!' % (expected_text, text)


def scrape():
    logging.info('PREPARING')
    # Prepare Driver
    # driver = webdriver.Chrome()

    driver = webdriver.Remote(
        command_executor='http://tzabar.obudget.org:8910',
        desired_capabilities=DesiredCapabilities.PHANTOMJS)

    driver.set_window_size(1200, 800)

    url = hostname + "/GCA/CompaniesInformation/Pages/default.aspx"
    logging.info('GETTING DATA %s', url)
    driver.get(hostname + "/GCA/CompaniesInformation/Pages/default.aspx")

    rows = []
    attempts = 10
    while len(rows) == 0:
        page = pq(driver.page_source)
        rows = page.find('.gcaCompamies tbody tr')
        sleep(1)
        attempts = attempts - 1
        assert attempts > 0

    rows = page.find('.gcaCompamies tbody tr')
    logging.info('GOT %d ROWS', len(rows))
    verify_row_structure(rows[0])
    for row in rows:
        yield dict(get_element_from_row(row))


datapackage['resources'].append({
    'name': 'government-companies',
    PROP_STREAMING: True,
    'path': 'data/government-companies.csv',
    'schema': {
        'fields': [
            {'name': h, 'type': 'string'} for h in headers
        ]
    }
})

spew(datapackage, itertools.chain(res_iter, [scrape()]))
