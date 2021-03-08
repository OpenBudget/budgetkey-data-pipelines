from pyquery import PyQuery as pq
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import dataflows as DF
from datapackage_pipelines_budgetkey.common.google_chrome import google_chrome_driver
from datapackage_pipelines_budgetkey.common.publication_id import calculate_publication_id

BASE = 'https://www.gov.il'
URL = BASE + '/he/Departments/publications/reports/class_action_law'


def scrape():
    gcd = google_chrome_driver()
    driver = gcd.driver

    driver.get(URL)
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.ID, "ReportContent"))
    )
    page = pq(driver.page_source)
    rows = page.find('#ReportContent table.table tr')
    for row in rows:
        _row = [
            pq(td)
            for td in pq(row).find('td')
        ] 
        if len(_row) == 0:
            continue
        ret = dict(zip(
            ('tender_id', 'page_title', 'claim_date', 'description'),
            [td.text() for td in _row]
        ))
        ret['claim_date'] = ret['claim_date'].split(' ')[0] if ret['claim_date'] else None
        link = pq(_row[0].find('a')).attr('href')
        documents = []
        if link:
            if not link.startswith('http'):
                link = BASE + link
            documents=[dict(link=link, description='מסמכי הקול הקורא')]
        ret.update(dict(
            tender_type='call_for_bids',
            page_url=URL,
            publication_id=0,
            tender_type_he='קול קורא',
            publisher='ועדת העזבונות',
            start_date=None,
            documents=documents,
            contact='מוחמד זחלקה',
            contact_email='keren27@justice.gov.il'
        ))
        yield ret
    gcd.teardown()


def flow(*_,):
    return DF.Flow(
        scrape(),
        DF.update_resource(-1, **{
            'name': 'class_action',
            'dpp:streaming': True
        }),
        DF.set_type('claim_date', type='datetime', format='any', resources=-1),
        DF.set_type('claim_date', type='datetime', format='%d/%m/%Y', resources=-1, on_error=DF.schema_validator.clear),
        calculate_publication_id(8),
    )


if __name__ == '__main__':
    DF.Flow(
        flow(), DF.printer()
    ).process()
