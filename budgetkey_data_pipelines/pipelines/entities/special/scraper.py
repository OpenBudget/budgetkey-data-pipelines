from datapackage_pipelines.wrapper import ingest, spew

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import time
import itertools

import logging
from selenium.webdriver.remote.remote_connection import LOGGER
LOGGER.setLevel(logging.WARNING)

parameters, datapackage, res_iter = ingest()

slugs = {u"  \u05ea\u05d0\u05d2\u05d9\u05d3\u05d9 \u05d4\u05d0\u05d6\u05d5\u05e8  (\u05d9\u05d5''\u05e9 )": 'west_bank_corporation',
         u'\u05d0\u05d9\u05d2\u05d5\u05d3\u05d9 \u05e2\u05e8\u05d9\u05dd': 'conurbation',
         u'\u05d0\u05d9\u05d2\u05d5\u05d3\u05d9\u05dd \u05de\u05e7\u05e6\u05d5\u05e2\u05d9\u05d9\u05dd': 'professional_association',
         u'\u05d2\u05d5\u05e4\u05d9\u05dd \u05e2"\u05e4 \u05d3\u05d9\u05df': 'law_mandated_organization',
         u'\u05d4\u05e7\u05d3\u05e9 \u05d1\u05d9\u05ea \u05d3\u05d9\u05df \u05d3\u05ea\u05d9': 'religious_court_sacred_property',
         u'\u05d5\u05d5\u05e2\u05d3\u05d9\u05dd \u05de\u05e7\u05d5\u05de\u05d9\u05d9\u05dd \u05d1\u05d9\u05e9\u05d5\u05d1\u05d9\u05dd': 'local_community_committee',
         u'\u05d5\u05e2\u05d3\u05d5\u05ea \u05de\u05e7\u05d5\u05de\u05d9\u05d5\u05ea \u05dc\u05ea\u05db\u05e0\u05d5\u05df': 'local_planning_committee',
         u'\u05d5\u05e2\u05d3\u05d9 \u05d1\u05ea\u05d9\u05dd': 'house_committee',
         u'\u05d7\u05d1\u05e8\u05d5\u05ea \u05d7\u05d5\u05e5 \u05dc\u05d0 \u05e8\u05e9\u05d5\u05de\u05d5\u05ea': 'foreign_company',
         u'\u05de\u05e9\u05e8\u05d3\u05d9 \u05de\u05de\u05e9\u05dc\u05d4': 'government_office',
         u'\u05e0\u05e6\u05d9\u05d2\u05d5\u05d9\u05d5\u05ea \u05d6\u05e8\u05d5\u05ea': 'foreign_representative',
         u'\u05e7\u05d5\u05e4\u05d5\u05ea \u05d2\u05de\u05dc': 'provident_fund',
         u'\u05e8\u05d5\u05d1\u05e2\u05d9\u05dd \u05e2\u05d9\u05e8\u05d5\u05e0\u05d9\u05d9\u05dd': 'municipal_precinct',
         u'\u05e8\u05e9\u05d5\u05d9\u05d5\u05ea \u05de\u05e7\u05d5\u05de\u05d9\u05d5\u05ea': 'municipality',
         u'\u05e8\u05e9\u05d5\u05d9\u05d5\u05ea \u05e0\u05d9\u05e7\u05d5\u05d6': 'drainage_authority',
         u'\u05e8\u05e9\u05d9\u05de\u05d5\u05ea \u05dc\u05e8\u05e9\u05d5\u05d9\u05d5\u05ea \u05d4\u05de\u05e7\u05d5\u05de\u05d9\u05d5\u05ea': 'municipal_parties',
         u'\u05e9\u05d9\u05e8\u05d5\u05ea\u05d9 \u05d1\u05e8\u05d9\u05d0\u05d5\u05ea': 'health_service',
         u'\u05e9\u05d9\u05e8\u05d5\u05ea\u05d9 \u05d3\u05ea': 'religion_service'}

headers = ['kind', 'name', 'id', 'street', 'house_number', 'city', 'zipcode']

def scrape():

    # Prepare Driver
    driver = webdriver.PhantomJS(service_args=['--ignore-ssl-errors=true'])
    driver.set_window_size(1200, 800)

    def prepare():
        driver.get("http://www.misim.gov.il/mm_lelorasham/firstPage.aspx")
        bakasha = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "RadioBakasha1"))
        )
        bakasha.click()

    prepare()

    # Prepare options
    select = driver.find_element_by_id("DropdownlistSugYeshut")
    options = {}
    for option in select.find_elements_by_tag_name('option')[1:]:
        options[option.get_attribute('value')] = option.text

    for selection in options.keys():
        logging.info('OPTION %s (%s)', selection, options[selection])
        prepare()
        driver.find_element_by_css_selector('option[value="%s"]' % selection).click()
        driver.find_element_by_id('btnHipus').click()
        while True:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#dgReshima tr.row1"))
            )
            rows = driver.find_elements_by_css_selector('#dgReshima tr.row1, #dgReshima tr.row2')
            for row in rows:
                if len(set(row.get_attribute('class').split()).intersection({'row1', 'row2'})) > 0:
                    row = ([slugs[options[selection]]] +
                           [x.text for x in row.find_elements_by_tag_name('td')])
                    datum = dict(zip(headers, row))
                    yield datum

            try:
                next_button = driver.find_element_by_id('btnHaba')
                next_button.click()
                time.sleep(1)
            except Exception:
                break


datapackage['resources'].append({
    'name': 'special-entities',
    'path': 'data/special-entities.csv',
    'schema': {
        'fields': [
            {'name': h, 'type': 'string'} for h in headers
        ]
    }
})

spew(datapackage, itertools.chain(res_iter, [scrape()]))
