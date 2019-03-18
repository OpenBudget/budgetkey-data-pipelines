import requests
from pyquery import PyQuery as pq

from dataflows import Flow, printer, set_type, set_primary_key, delete_fields, update_resource
from datapackage_pipelines.utilities.resources import PROP_STREAMING

from datapackage_pipelines_budgetkey.common.publication_id import calculate_publication_id


URL = 'https://jobiz.gov.il/ajax/results/הודעה ציבורית' + '/{}?ie=0&typeie=הודעה+ציבורית&search=Array'


def fetch_results():
    index = 0
    while True:
        content = requests.get(URL.format(index)).json()
        content = content['content']
        content = pq(content)
        boxes = content.find('.modal')
        if len(boxes) == 0:
            break
        for box in boxes:
            box = pq(box)
            yield dict(
                publication_id=None,
                tender_id=None,
                tender_type=None,
                page_title=pq(box.find('#modal-title')).text(),
                publisher=pq(box.find('.publisher_link')).text(),
                kind=pq(box.find('.generalInfo-jobs li:nth-child(1) span')).text(),
                start_date=pq(box.find('.generalInfo-jobs li:nth-child(2) span')).text(),
                description=pq(box.find('.moreInfoInner')).text(),
            )
            index += 1


KIND_MAPPING = {
    'קולות קוראים': 'call_fot_bids',
    'תמיכות': 'support_criteria',
}


def process_kind(row):
    row['tender_type'] = KIND_MAPPING.get(row['kind'], row['kind'])


def flow(*_):
    return Flow(
        fetch_results(),
        set_type('start_date', type='date', format='%d.%m.%Y'),
        process_kind,
        delete_fields(['kind']),
        calculate_publication_id(2),
        set_primary_key(['publisher', 'page_title', 'start_date', 'kind']),
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
