import requests
import json

import dataflows as DF
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines_budgetkey.common.publication_id import calculate_publication_id


def scraper():
    URL = 'https://www.gov.il/CollectorsWebApi/api/DataCollector/GetResults?CollectorType=reports&CollectorType=rfp&CollectorType=drushim&CollectorType=publicsharing&officeId=104cb0f4-d65a-4692-b590-94af928c19c0&limit=30&Type=7159e036-77d5-44f9-a1bf-4500e6125bf1&Status=c8ed2e3c-e5a9-4101-93f5-bae798c4ac1d&culture=he'
    results = json.loads(requests.get(URL).text)['results']
    for result in results:
        if 'תאריך אחרון להגשה' in result['tags']['promotedMetaData']:
            claim_date = result['tags']['promotedMetaData']['תאריך אחרון להגשה'][0]['title']
        else:
            claim_date = None
        yield dict(
            page_url = 'https://gov.il' + result['url'],
            page_title = result['title'],
            publisher = 'משרד הבריאות',
            tender_id = 0, # result['tags']['promotedMetaData']['מספר'][0]['title'],
            tender_type='call_for_bids',
            tender_type_he = 'קול קורא',
            publication_id = None,
            start_date = result['tags']['metaData']['תאריך פרסום'][0]['title'],
            claim_date = claim_date,
            description = result['description'],
            documents = [],
        )


def flow(*args):
    return DF.Flow(
        scraper(),
        DF.set_type('start_date', type='date', format='%d.%m.%Y', resources=-1),
        # some call_for_bids are missing a claim_date, they are preliminary
        DF.set_type('claim_date', type='date', format='%d.%m.%Y', resources=-1, on_error=DF.schema_validator.clear),
        calculate_publication_id(9),
        DF.validate(),
        DF.update_resource(
            -1, name='support_criteria_from_ministry_of_health',
            **{
                PROP_STREAMING: True
            }
        )
    )

if __name__ == '__main__':
    DF.Flow(
        flow(), DF.printer()
    ).process()
