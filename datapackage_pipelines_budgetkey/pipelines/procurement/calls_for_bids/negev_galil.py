import json
import requests
from pyquery import PyQuery as pq
import dataflows as DF
from datapackage_pipelines_budgetkey.common.publication_id import calculate_publication_id
from datapackage_pipelines_budgetkey.common.sanitize_html import sanitize_html
from datapackage_pipelines.utilities.resources import PROP_STREAMING


def scraper():
    URL = 'https://negev-galil.gov.il/kolotkorim/hagashat/'
    page = pq(requests.get(URL).text)
    tenders = page.find('tr.tenders-table')
    for tender_ in tenders:
        tender = pq(tender_)
        decision = pq(tender.find('.tenders-icon img')[0]).attr('alt').split()[1]
        page_url = pq(tender.find('a')[0]).attr('href')
        page_title = pq(tender.find('a')[0]).text()
        start_date = pq(tender.find('[id="date-open"]')[0]).text()
        claim_date = pq(tender.find('[id="date-close"]')[0]).text()
        yield dict(
            page_url='https://negev-galil.gov.il' + page_url,
            page_title=page_title,
            publisher='משרד פיתוח הפריפריה, הנגב והגליל',
            tender_type='call_for_bids',
            tender_type_he='קול קורא',
            publication_id=None,
            decision=decision,
            start_date=start_date,
            claim_date=claim_date,
            description='',
            documents=[]
        )


def flow(*_):
    return DF.Flow(
        scraper(),
        DF.filter_rows(lambda row: row['page_title'] and row['page_title'].startswith('קול קורא'), resources=-1),
        DF.set_type('start_date', type='date', format='%d/%m/%Y', resources=-1),
        DF.set_type('claim_date', type='datetime', format='%d/%m/%Y', resources=-1),
        calculate_publication_id(9),
        DF.validate(),
        DF.update_resource(
            -1, name='negev_galil',
            **{
                PROP_STREAMING: True
            }
        ),
    )


if __name__ == '__main__':
    DF.Flow(
        flow(), DF.printer()
    ).process()