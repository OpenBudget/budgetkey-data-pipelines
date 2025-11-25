import json
import requests
from pyquery import PyQuery as pq
import dataflows as DF
from datapackage_pipelines_budgetkey.common.publication_id import calculate_publication_id
from datapackage_pipelines_budgetkey.common.sanitize_html import sanitize_html
from datapackage_pipelines.utilities.resources import PROP_STREAMING


def scraper():
    URL = 'https://www.gov.il/CollectorsWebApi/api/DataCollector/GetResults?CollectorType=reports&CollectorType=rfp&CollectorType=drushim&CollectorType=publicsharing&limit=10&officeId=1f628453-da79-4549-8c12-1073a718c9b0&Type=7159e036-77d5-44f9-a1bf-4500e6125bf1&Status=c8ed2e3c-e5a9-4101-93f5-bae798c4ac1d&culture=he'
    results = json.loads(requests.get(URL).text)['results']
    for result in results:
        yield dict(
            page_url = 'https://gov.il' + result['url'],
            page_title = result['title'],
            publisher = 'משרד פיתוח הפריפריה, הנגב והגליל',
            tender_id = result['tags']['promotedMetaData']['מספר'][0]['title'],
            tender_type='call_for_bids',
            tender_type_he = 'קול קורא',
            publication_id = None,
            start_date = result['tags']['metaData']['תאריך פרסום'][0]['title'],
            claim_date = result['tags']['promotedMetaData']['תאריך אחרון להגשה'][0]['title'],
            description = '',
            documents = [],
            subject_list_keywords = [],
            ordering_units = [],
            required_documents = [],
        )


def flow(*_):
    return DF.Flow(
        scraper(),
        DF.set_type('start_date', type='date', format='%d.%m.%Y', resources=-1),
        DF.set_type('claim_date', type='datetime', format='%d.%m.%Y', resources=-1),
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
