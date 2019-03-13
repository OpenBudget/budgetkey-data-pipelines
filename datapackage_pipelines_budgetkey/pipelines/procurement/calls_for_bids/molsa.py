from hashlib import md5
from dataflows import Flow, printer, update_resource
from datapackage_pipelines.utilities.resources import PROP_STREAMING
import requests
from pyquery import PyQuery as pq

s = requests.session()

BASE = {
    'publisher': 'משרד העבודה, הרווחה והשירותים החברתיים',
    'tender_type': 'call_for_bids',
    'publication_id': None,
}

headers = [
    ('page_title', 'documents'),
    ('target_audience', ),
    ('start_date', ),
    ('claim_date', ),
    ('_', ),
    ('decision', ),
    ('_', 'page_url')
]

details_headers = [
    ('reason', '#ctl00_PlaceHolderMain_lblPubAppealSubject'),
    ('publishing_unit', '#ctl00_PlaceHolderMain_lblPubAppealPublisherFactor'),
    ('ordering_unit', '#ctl00_PlaceHolderMain_lblPubAppealOrderFactor'),
    ('ordering_units', '#ctl00_PlaceHolderMain_lblPubAppealOrderFactor li'),
    ('description', '#ctl00_PlaceHolderMain_lblPubAppealSummary'),
    ('contact', '#ctl00_PlaceHolderMain_lblPubAppealHowToAppeal'),
    ('contact_email', '#ctl00_PlaceHolderMain_lblPubAppealHowToAppeal a'),
    ('required_documents', '#ctl00_PlaceHolderMain_lblPubAppealRequiredDocuments li'),
    ('partners', '#ctl00_PlaceHolderMain_lblPubAppealMembers'),
]


def fetch_calls():
    URL = 'https://www.molsa.gov.il/Publications/Pages/PubAppealNewSearch.aspx'

    catalog = pq(s.get(URL).text)
    for row in catalog.find('.ms-listviewtable tr'):
        ret = {}
        ret.update(BASE)
        ret.update(dict(
            (k, None)
            for k, *_ in details_headers
        ))
        cells = pq(row).find('td')
        if len(cells) == len(headers):
            for header, cell in zip(headers, cells):
                cell, main, *anchor = pq(cell), *header
                ret[main] = cell.text()
                if len(anchor) > 0:
                    a = cell.find('a')
                    if len(a) > 0:
                        href = pq(a).attr('href')
                        if href.startswith('/'):
                            href = 'https://www.molsa.gov.il{}'.format(href)
                        ret[anchor[0]] = href
            yield ret


def call_details():
    def func(row):
        details = pq(s.get(row['page_url']).text)
        for key, selector, *_ in details_headers:
            if selector.endswith('li'):
                row[key] = [pq(x).text() for x in details.find(selector)]
            else:
                row[key] = details.find(selector).text()
    return func


def resolve_ordering_unit():
    def func(row):
        if row.get('ordering_unit') and not row.get('ordering_units'):
            row['ordering_units'] = [row['ordering_unit']]
            row['ordering_unit'] = None
    return func


def fix_documents():
    def func(row):
        href = row['documents']
        title = row['page_title']
        update_date = row['start_date']
        row['documents'] = [
            dict(
                description=title,
                link=href,
                update_date=update_date
            )
        ]
    return func


def calculate_publication_id():
    def func(row):
        title_hash = int.from_bytes(
            md5(
                (row['publisher'] + row['page_title']).encode('utf8')
            ).digest()[:4],
            'big'
        )
        mod = 1000000000
        title_hash = mod + (title_hash % mod)
        row['publication_id'] = title_hash
    return func


def flow(*args):
    return Flow(
        fetch_calls(),
        call_details(),
        resolve_ordering_unit(),
        fix_documents(),
        calculate_publication_id(),
        update_resource(
            -1, name='molsa',
            **{
                PROP_STREAMING: True
            }
        ),
        printer()
    )


# if __name__ == '__main__':
#     flow().process()
