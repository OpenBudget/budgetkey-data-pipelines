from datapackage_pipelines_budgetkey.common.sanitize_html import sanitize_html
from pyquery import PyQuery as pq
import re
import requests
import dataflows as DF

claim_date_re = re.compile('[0-9]{1,2}[./][0-9]{1,2}[./][0-9]{2,4}', re.MULTILINE)
claim_date_re_parts = re.compile('[0-9]+')


BASE_URL = 'http://www2.jdc.org.il'
MAIN_PAGE = '/he/jobs/85'

def all_links():
    main = pq(requests.get(BASE_URL + MAIN_PAGE).text)
    links = main.find('.item-list .item-title a')
    for link in links:
        link = pq(link)
        link = link.attr('href')
        if link:
            yield link


def scraper():
    for link in all_links():
        url = BASE_URL + link
        main = pq(requests.get(url).text)
        title = pq(main.find('h1.main-title')[0]).text()
        text = pq(main.find('article.node'))
        start_date = text.find('div.field-type-datetime')
        start_date = pq(start_date).text()
        start_date = '/'.join(x for x in start_date.split('/'))
        description = sanitize_html(pq(text.find('.field-type-text-with-summary')))
        try:
            claim_date = pq(text.find('strong')[-1]).text()
            claim_date = claim_date_re.findall(claim_date)[0]
        except IndexError:
            try:
                claim_date = claim_date_re.findall(description)[-1]
            except:
                print('SKIPPING', url)
                continue
        claim_date = [int(p) for p in claim_date_re_parts.findall(claim_date)]
        if claim_date[2] < 1000:
            claim_date[2] += 2000
        claim_date = '/'.join(str(p) for p in claim_date)
        yield dict(
            page_url=url,
            page_title=title,
            start_date=start_date,
            claim_date=claim_date,
            description=description,

            tender_type='call_for_bids',
            publication_id=0,
            tender_type_he='קול קורא',
            tender_id='joint' + link.replace('/', '-'),
            publisher='הג׳וינט',
        )


def flow(*_):
    return DF.Flow(
        scraper(),
        DF.update_resource(-1, name='joint', path='joint.csv', **{'dpp:streaming': True})
    )

if __name__ == '__main__':
    DF.Flow(
        flow(), DF.printer()
    ).process()