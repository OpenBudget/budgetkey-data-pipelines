import itertools
import logging

from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support import expected_conditions as EC
# from selenium.webdriver.support.ui import WebDriverWait
from urllib.parse import urljoin
from pyquery import PyQuery as pq

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()


def scrape():
    logging.info('Scraping calcalist')

    # driver = webdriver.Remote(
    #     command_executor='http://localhost',
    #     desired_capabilities=DesiredCapabilities.PHANTOMJS
    # )
    driver = webdriver.PhantomJS()
    driver.set_window_size(1200, 800)

    logging.info('Selenium driver initialized')

    # WebDriverWait(driver, 10).until(
    #     EC.presence_of_element_located((By.CLASS_NAME, 'MainArea'))
    # )
    #
    # logging.info('Finished waiting for driver')

    base_url = 'http://www.calcalist.co.il'
    full_url = '%s/local/home/0,7340,L-3789,00.html' % base_url
    logging.info('Querying: %s' % full_url)
    driver.get(full_url)
    logging.info('Finished querying first URL')
    # CdaNominationList
    # CdaNominationList
    nominations_iframe = driver.find_element_by_id('CdaNominationList')
    src = nominations_iframe.get_attribute('src')
    url = urljoin(base_url, src)
    logging.info('IFrame URL: %s' % url)
    driver.get(url)

    page = pq(driver.page_source)
    area = page('.MainArea')
    for nomination in area:
        logging.info('nomination: %s' % nomination)

    logging.info('Finished printing nominations')
    return []


spew(datapackage, itertools.chain(res_iter, [scrape()]))
