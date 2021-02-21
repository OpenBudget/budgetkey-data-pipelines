from pyquery import PyQuery as pq
import requests
import dataflows as DF
from datapackage_pipelines_budgetkey.common.publication_id import calculate_publication_id


URL = 'https://www.btl.gov.il/Funds/kolotkorim/Pages/default.aspx'
HEADERS = {
    'User-agent': 'Mozillaacintosh; Intel Mac OS X 10.14; rv:71.0) Gecko/20100101 Firefox/71.0'
}
BASE = dict(
    tender_id='0',
    tender_type='call_for_bids',
    publication_id=0,
    tender_type_he='קול קורא',
    publisher='הביטוח הלאומי',
    start_date=None,
    claim_date=None,
    subject_list_keywords=[],
    page_title='',
    page_url='',
)


def scrape():
    yield BASE
    page = requests.get(URL, headers=HEADERS).text
    page = pq(page)
    items = page.find('li.open')
    for item in items:
        ret = dict(
            (k, v) for k, v in BASE.items()
        )
        item = pq(item)
        links = item.find('a')
        documents = []
        for link in links:
            link = pq(link)
            href = link.attr('href')
            if not (href.startswith('http') or href.startswith('/')):
                print(link)
                href = link.attr('onclick')
                href = href.split('(')[1][1:].split(')')[0][:-1]
                assert href.startswith('http') or href.startswith('/')
            if href.startswith('/'):
                href = 'https://www.btl.gov.il' + href,
            documents.append(dict(
                link='https://www.btl.gov.il' + link.attr('href'),
                description=link.text(),
            ))
        ret['documents'] = documents
        title = documents[0]
        ret.update(dict(
            page_title=title['description'],
            page_url=URL
        ))
        details = item.find('span')
        for detail in details:
            detail = pq(detail)
            kind = pq(detail.children('label')).text().strip()
            if not kind:
                continue
            value = detail.text().replace(kind, '')
            kind = {
                'אוכלוסיית יעד:': 'target_audience',
                'מועד אחרון להגשה:': 'claim_date',
                'עדכון מתאריך:': 'start_date',
                'תחום:': 'reason',
                'מועד אחרון להגשת שאלות:': ''
            }.get(kind)
            if not kind:
                continue
            ret[kind] = value
        for x in ('reason', 'target_audience'):
            if x in ret:
                ret['subject_list_keywords'].append(ret.pop(x))
        if ret['claim_date'].split(' ') == 1:
            ret['claim_date'] += ' 00:00'
        yield ret


def flow(*_):
    return DF.Flow(
        scrape(),
        DF.update_resource(-1, **{'dpp:streaming': True, 'name': 'btl'}),
        DF.set_type('claim_date', type='datetime', format='%d/%m/%Y %H:%M', resources=-1),
        DF.set_type('start_date', type='date', format='%d/%m/%Y', resources=-1),
        calculate_publication_id(7),
        DF.filter_rows(lambda r: r['publication_id'])
    )

if __name__ == '__main__':
    DF.Flow(
        flow(),
        DF.printer()
    ).process()