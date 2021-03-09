import requests
from pyquery import PyQuery as pq
from lxml.etree import ElementBase

from dataflows import Flow, printer, set_type, set_primary_key, delete_fields, update_resource
from datapackage_pipelines.utilities.resources import PROP_STREAMING

from datapackage_pipelines_budgetkey.common.publication_id import calculate_publication_id
from datapackage_pipelines_budgetkey.common.sanitize_html import sanitize_html


URL = 'https://jobiz.gov.il/ajax/results/הודעה ציבורית' + '/{}?ie=0&typeie=הודעה+ציבורית&search=Array'
SRC_URL = 'https://jobiz.gov.il/public_messages'


def fetch_results():
    index = 0
    while True:
        content = requests.get(URL.format(index)).json()
        content = content['content']
        if len(content) == 0:
            break
        else:
            print('GOT {} chars'.format(len(content)))
        content = pq(content)
        boxes = content.find('.modal')
        if len(boxes) == 0:
            break
        else:
            print('GOT {} boxes'.format(len(boxes)))
        for box in boxes:
            box = pq(box)
            description = pq(box.find('.moreInfoInner'))
            description = sanitize_html(description)
            print('GOT {}'.format(pq(box.find('#modal-title')).text()))
            yield dict(
                publication_id=0,
                tender_id='0',
                tender_type=None,
                tender_type_he=pq(box.find('.generalInfo-jobs li:nth-child(1) span')).text(),
                decision=None,
                page_title=pq(box.find('#modal-title')).text(),
                page_url=SRC_URL,
                publisher=pq(box.find('.publisher_link')).text(),
                start_date=pq(box.find('.generalInfo-jobs li:nth-child(2) span')).text(),
                claim_date=None,
                description=description,
            )
            index += 1

KIND_MAPPING = {
    'קולות קוראים': 'call_for_bids',
    'תמיכות': 'call_for_bids',
}
KIND_HE_MAPPING = {
    'קולות קוראים': 'קול קורא',
    'תמיכות': 'קול קורא',
}

def process_kind(row):
    row['tender_type'] = KIND_MAPPING.get(row['tender_type_he'], row['tender_type_he'])
    row['tender_type_he'] = KIND_HE_MAPPING.get(row['tender_type_he'], row['tender_type_he'])


def flow(*_):
    return Flow(
        fetch_results(),
        set_type('start_date', type='date', format='%d.%m.%Y'),
        set_type('claim_date', type='datetime', format='any'),
        set_type('tender_id', type='string'),
        set_type('tender_type', type='string'),
        process_kind,
        calculate_publication_id(2),
        set_primary_key(['publication_id']),
        update_resource(
            -1, name='jobiz',
            **{
                PROP_STREAMING: True
            }
        ),
    )


if __name__ == '__main__':
    Flow(
        flow(),
        printer(),
    ).process()
