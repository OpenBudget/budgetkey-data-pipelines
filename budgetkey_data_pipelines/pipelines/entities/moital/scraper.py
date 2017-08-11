import time
import requests
import logging

from pyquery import PyQuery as pq
from datapackage_pipelines.wrapper import ingest, spew

from selenium import webdriver
# from selenium.common.exceptions import TimeoutException
# from selenium.webdriver.common import utils
# from selenium.webdriver.remote.remote_connection import RemoteConnection

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.remote.remote_connection import LOGGER
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

parameters, datapackage, res_iter = ingest()

url = parameters['url']
contractors = parameters['contractors_csv'].split(',')
extention = parameters['page_extention']

delay = parameters['delay']
headers = ['close_corporation', 'name', 'id', 'license_number', 'company_owner', 'company_ceo', 
  'address', 'city', 'expired', 'status']

def nextPageElement(page):
    return page.find('#gvContractors table[border="0"] span').parent().next()

def scrape():

    driver = webdriver.Remote(
        command_executor='http://127.0.0.1:4444/wd/hub',
        desired_capabilities=DesiredCapabilities.PHANTOMJS)

    driver.set_window_size(1200, 800)

    for contractor in contractors:
        host_ = url + contractor.strip() + extention
        logging.info('Working on the host %s', host_)

        def prepare():
            logging.info('PREPARING')
            driver.get(host_)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "gvContractors"))
            )

        prepare()

        while True:
            page = pq(driver.page_source)
            rows = page.find('#gvContractors tr[align="right"]')
            logging.info('Got %d rows', len(rows))

            for row_ in rows:
                row = ([" ".join(pq(x).text().strip().split()) for x in pq(row_).find('td')])
                datum = dict(zip(headers, row))
                yield datum

            nextElem = nextPageElement(page)
            if len(nextElem) > 0:
                logging.info('Navigate to page #%s',nextElem.find('a').text())
                next_page = driver.find_element_by_xpath("//*[@border=0]/tbody/tr/td/span/parent::td/following-sibling::td/a")
                next_page.click()
                time.sleep(delay)
            else:
                break

datapackage['resources'].append({
    'name': 'name',
    'path': 'out-path',
    'schema': {
        'fields': [
            {'name': h, 'type': 'string'} for h in headers
        ]
    }
})

spew(datapackage, [scrape()])

# for run the pipeline
# budgetkey-dpp run ./entities/moital/moital_service_providers