import logging
import json
import time

from datetime import date, datetime
from dateutil.relativedelta import relativedelta

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from urllib.parse import urlunparse, urlencode

from selenium import webdriver
from selenium.webdriver.remote.remote_connection import LOGGER
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from pyquery import PyQuery as pq


from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, _ = ingest()


def _build_maya_url(date_from, date_to, page_num):
    scheme = "https"
    netloc = "maya.tase.co.il"
    path = "/reports/company"
    query = urlencode({"q":json.dumps({
        "DateFrom": date_from.isoformat(),
        "DateTo": date_to.isoformat(),
        "Page": page_num,
        "events": [600],
        "subevents": [605,603,601,602,621,604,606,615,613,611,612,622,614,616]})

    }, True)
    return urlunparse((scheme, netloc, path, "", query, ""))


def _split_period(date_from, date_to):
    current_date = date_from
    next = current_date + relativedelta(years=1)
    while next < date_to:
        yield(current_date, next)
        current_date = next
        next = current_date + relativedelta(years=1)
    yield(current_date, date_to)


def _get_total_page_count(page):
    res = 1
    for elem in page('.pagination a'):
        try:
            res = max(res, int(pq(elem).text()))
        except (TypeError, ValueError) as e:
            pass
    return res


def _get_maya_page(driver, date_from, date_to, current_page, attempts=0):
    try:
        driver.get(_build_maya_url(date_from, date_to, current_page))
        time.sleep(3)
        WebDriverWait(driver, 5).until(
            EC.visibility_of_element_located((By.CLASS_NAME, "totalReports"))
        )
    except TimeoutException as e:
        # Had Some problems with page being empty refresh always fixed so added several attempts here
        if attempts>3:
            raise e
        return _get_maya_page(driver, date_from, date_to, current_page, attempts+1)
    return pq(driver.page_source, parser='html')


def _scrape_date_range(date_from, date_to):
    # Prepare Driver
    #driver = webdriver.Chrome()

    driver = webdriver.Remote(
        command_executor='http://tzabar.obudget.org:8910',
        desired_capabilities=DesiredCapabilities.PHANTOMJS)
    driver.set_window_size(1200, 800)

    current_page = 1
    total_pages = 1

    while current_page <= total_pages:

        page = _get_maya_page(driver, date_from, date_to, current_page)


        # Figure out how many pages are available in total
        total_pages = _get_total_page_count(page)

        # Iterate over the available documents and get their individual URLs
        events = page('maya-reports .feedItem')
        for event in events:

            date_str = pq(event)('.feedItemSideOptions .feedItemDate').text()
            #date_str will look like this 17/05/2018 12:07
            date = datetime.strptime(date_str, "%d/%m/%Y %H:%M")

            href = pq(event)('.feedItemMessage a.messageContent').attr('href')
            # href is a string of the type reports/details/1162540 we are interested only in the id of the doc
            doc_id = int(href[href.rindex('/')+1:])

            segment_start = (doc_id//1000) * 1000
            segment_end = segment_start + 1000
            yield {
                'date': date,
                'source':'maya.tase.co.il',
                's3_object_name': 'maya.tase.co.il/{}/{}.htm'.format(date.strftime('%Y_%m'),doc_id),
                'url': 'https://mayafiles.tase.co.il/RHtm/{}-{}/H{}.htm'.format(segment_start+1, segment_end, doc_id)}

        current_page = current_page +1


def scrape():
    logging.info('Scraping Maya')

    date_from = date(2011, 1, 1)
    if 'from' in parameters:
        date_from = datetime.strptime(parameters.get('from'), "%Y-%m-%d").date()

    date_to = date.today()
    if 'to' in parameters:
        date_to = datetime.strptime(parameters.get('to'), "%Y-%m-%d").date()

    for year_start, year_end in _split_period(date_from, date_to):
        yield from _scrape_date_range(year_start, year_end)


datapackage['resources'].append({
    'path': 'data/notification-list.csv',
    'name': parameters.get('name'),
    PROP_STREAMING: True,
    'schema': {
        'fields': [
            {'name': 'url', 'type': 'string'},
            {'name': 's3_object_name', 'type': 'string'},
            {'name': 'source', 'type':'string'},
            {'name': 'date', 'type': 'date'}
        ]
    }
})

spew(datapackage, [scrape()])


