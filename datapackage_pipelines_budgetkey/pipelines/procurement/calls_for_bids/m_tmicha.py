import requests
from pyquery import PyQuery as pq
from dataflows import Flow, printer, update_resource, dump_to_path
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines_budgetkey.common.publication_id import calculate_publication_id


def m_tmicha_scraper():

    url = 'https://www.health.gov.il/Subjects/Finance/Mtmicha/Pages/default.aspx'
    s = requests.Session()
    page = pq(s.get(url).text)

    for row in page.find('.ms-rtestate-field a'):
        if row.attrib['href'][-3:] == 'pdf':
            link = row.attrib['href']
            title = row.text
            yield dict(
                link=link,
                page_title=title,
                publication_id=None,
                documents='',
                tender_type='call_for_bids',
                tender_id=None
            )


def flow(*args):
    return Flow(
        m_tmicha_scraper(),
        calculate_publication_id(3),
        update_resource('res_1', **{
            # Set a proper name for the resource
            'name': 'm_tmicha',
            'path': 'm_tmicha.csv',
            'dpp:streaming': True,
        })
    )
