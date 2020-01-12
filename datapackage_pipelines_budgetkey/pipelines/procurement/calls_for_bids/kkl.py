from datapackage_pipelines_budgetkey.common.sanitize_html import sanitize_html
from pyquery import PyQuery as pq
import re
from datapackage_pipelines_budgetkey.common.google_chrome import google_chrome_driver, finalize_teardown
import dataflows as DF
import time
import logging



logging.getLogger().setLevel(logging.INFO)


BASE_URL = 'http://www2.jdc.org.il'
BASE_URL = 'https://www.kkl.org.il'
MAIN_PAGES = ['/tenders/bids/', '/tenders/bids/2019-archive/', '/tenders/bids/2018-archive/', '/tenders/bids/2017-archive/']


claim_date_re = re.compile('מועד[^\n]+הגש[^\n]+הצע.*\n?.*[^0-9]' + '([0-9]{1,2}[./][0-9]{1,2}[./][0-9]{2,4})', re.MULTILINE)
start_date_re = re.compile('תאריך פרסום באינטרנט:' + '[^0-9\n]*' + '([0-9]{1,2}[./][0-9]{1,2}[./][0-9]{2,4})', re.MULTILINE)
claim_date_re_parts = re.compile('[0-9]+')


def all_links(gcd):
    for main_page in MAIN_PAGES:
        gcd.driver.get(BASE_URL + main_page)
        time.sleep(1)
        page = gcd.driver.page_source
        main = pq(page)
        items = main.find('.page-content-item')
        for item in items:
            item = pq(item)
            item_text = item.text()
            try:
                start_date = start_date_re.findall(item_text)[0]
            except:
                logging.warning('FAILED TO FIND START DATE %s\n%s', main_page, item_text)
                continue
            link = pq(item.find('td.full-details-link a'))
            link = link.attr('href')
            if link:
                yield start_date, link


def scraper(gcd):
    for start_date, link in all_links(gcd):
        url = BASE_URL + link
        gcd.driver.get(url)
        time.sleep(1)
        page = pq(gcd.driver.page_source)
        title = pq(page.find('.page-content-container h1')).text().strip()
        content = pq(page.find('.page-content-item'))
        description = sanitize_html(content)
        content = content.text().strip()
        if not content or not title:
            logging.warning('FAILED TO FIND TITLE OR CONTENT FOR %s', url)
            continue
        try:
            claim_date = claim_date_re.findall(content)[0]
        except:
            logging.warning('FAILED TO FIND CLAIM DATE for %s', url)
            claim_date = None

        if claim_date:
            claim_date = [int(p) for p in claim_date_re_parts.findall(claim_date)]
            if claim_date[2] < 1000:
                claim_date[2] += 2000
            claim_date = '/'.join(str(p) for p in claim_date)
        start_date = '/'.join(x for x in start_date.split('.'))

        yield dict(
            page_url=url,
            page_title=title,
            start_date=start_date,
            claim_date=claim_date,
            description=description,

            tender_type='call_for_bids',
            publication_id=0,
            tender_type_he='קול קורא',
            tender_id='kkl' + link.replace('/', '-'),
            publisher='הקרן הקיימת לישראל',
        )


gcd = None


def flow(*_):
    global gcd
    gcd = google_chrome_driver(wait=False)
    return DF.Flow(
        scraper(gcd),
        DF.update_resource(-1, **{'dpp:streaming': True}),
        finalize_teardown(gcd),
    )


if __name__ == '__main__':
    try:
        DF.Flow(
            flow(), DF.printer()
        ).process()
    finally:
        if gcd:
            gcd.teardown()
