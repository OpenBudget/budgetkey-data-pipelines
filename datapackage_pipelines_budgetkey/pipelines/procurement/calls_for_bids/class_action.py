import json
import requests

from pyquery import PyQuery as pq
import dataflows as DF
from datapackage_pipelines_budgetkey.common.publication_id import calculate_publication_id

BASE = "https://www.gov.il/"
URL = BASE + "ContentPageWebApi/api/content-pages/class_action_law?culture=he"

def scrape():
    page = json.loads(requests.get(URL).text)
    table = pq(page['contentMain']['htmlContents'][0]['sectionData'])
    for row in table.find('table')[0].find('tbody'):
        _row = [
            pq(td)
            for td in pq(row).find('td, th')
        ] 
        if len(_row) == 0:
            continue
        ret = dict(zip(
            ('tender_id', 'page_title', 'claim_date', 'description'),
            [td.text() for td in _row]
        ))
        if not ret['tender_id']:
            # Some rows are missing data, just skip them
            continue
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
            contact_email='keren27@justice.gov.il',
            subject_list_keywords=[],
            ordering_units = [],
            required_documents = [],
        ))
        yield ret


def flow(*_,):
    return DF.Flow(
        scrape(),
        DF.update_resource(-1, **{
            'name': 'class_action',
            'dpp:streaming': True
        }),
        DF.set_type('claim_date', type='datetime', format='any', resources=-1, on_error=DF.schema_validator.clear),
        DF.set_type('claim_date', type='datetime', format='%d/%m/%Y', resources=-1, on_error=DF.schema_validator.clear),
        calculate_publication_id(8),
    )


if __name__ == '__main__':
    DF.Flow(
        flow(), DF.printer()
    ).process()
