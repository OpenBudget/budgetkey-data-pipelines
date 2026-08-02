import logging
from pathlib import Path
import time
import datetime

import requests
from pyquery import PyQuery as pq

from datapackage_pipelines_budgetkey.common.google_chrome import google_chrome_driver

# The gov.il "current year budget changes" page links to the data/explanation
# files with a weekly update date baked into each filename (e.g.
# BudgetChanges_approv_data_16072026.csv), so the URLs change over time and have
# to be rediscovered from the page on every run instead of being hardcoded.
# Same content-page API used by procurement/calls_for_bids/class_action.py, with
# the requests->browser fallback from procurement/spending/collect_report_uris.py
# (the content API is behind Cloudflare, so plain requests are often blocked).
URL = 'https://www.gov.il/he/pages/budget-changes-current-year'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:153.0) '
                  'Gecko/20100101 Firefox/153.0'
}

# Substrings identifying each file among the page's links. 'approv' = approved
# changes, 'vaada' = pending changes (on the finance committee's table).
FILE_KEYS = ['approv_data', 'approv_explain', 'vaada_data', 'vaada_explain']

week_number = datetime.datetime.now().isocalendar()[1]
year = datetime.datetime.now().year
CACHE_FILE_NAME = Path(f'cache_{year}_{week_number}.html')

def get_from_cache():
    if CACHE_FILE_NAME.exists():
        logging.info('Using cached page %s', CACHE_FILE_NAME)
        page = CACHE_FILE_NAME.read_text(encoding='utf-8')
        return page
    return None

def save_to_cache(page):
    CACHE_FILE_NAME.write_text(page, encoding='utf-8')
    logging.info('Saved page to cache %s', CACHE_FILE_NAME)

def test_page(page):
    assert 'שינויים בתקציב לשנה השוטפת המונחים על שולחן ועדת הכספים של הכנסת' in page, \
        'Page content does not contain expected text ' + page
    save_to_cache(page)

def get_page():
    page = get_from_cache()
    if page:
        return page
    try:
        resp = requests.get(URL, headers=HEADERS, timeout=120)
        assert resp.status_code == 200, resp.status_code
        resp_text = resp.text
        test_page(resp_text)
        return resp_text
    except Exception as e:
        logging.warning('Failed to fetch %s via requests (%s), trying google chrome', URL, e)
        gcd = google_chrome_driver(initial=URL, wait=False)
        try:
            gcd.driver.get(URL)
            time.sleep(20)
            # Live DOM rather than page_source: the page is rendered client-side,
            # so the original document has none of the links we're after.
            page = gcd.driver.execute_script('return document.body.outerHTML;')
            test_page(page)
            return page
        finally:
            gcd.teardown()


def get_current_year_urls():
    """Discover the current-year file URLs. Returns {file_key: url}."""
    page = get_page()
    links = pq(page).find('a')
    urls = [pq(a).attr('href') for a in links]
    urls = [u for u in urls if u]
    result = {}
    for key in FILE_KEYS:
        for u in urls:
            if key in u:
                print('Found %s URL: %s' % (key, u))
                result[key] = u
                break
    missing = [k for k in FILE_KEYS if k not in result]
    assert not missing, \
        'Could not find current-year files %r on page; found links: %r' % (missing, urls)
    logging.info('Discovered current-year budget-change URLs: %r', result)
    return result


if __name__ == '__main__':
    print(get_current_year_urls())
