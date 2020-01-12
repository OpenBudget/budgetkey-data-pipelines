import json
import requests
from pyquery import PyQuery as pq
import dataflows as DF
from datapackage_pipelines_budgetkey.common.publication_id import calculate_publication_id
from datapackage_pipelines_budgetkey.common.sanitize_html import sanitize_html
from datapackage_pipelines.utilities.resources import PROP_STREAMING


def scraper():
    PN = 1
    while True:
        url = f'http://negev-galil.gov.il/MediaCenter/Tenders/Pages/default.aspx?WPID=WPQ7&PN={PN}'
        print(url)
        page = pq(requests.get(url).text)
        links = page.find('.TenderItem .Title a')
        if len(links) == 0:
            break
        for link in links:
            link = pq(link)
            yield dict(
                page_url=link.attr('href'),
                page_title=link.attr('title') or link.text(),
                parsed={},
                publisher='משרד פיתוח הפריפריה, הנגב והגליל',
                tender_type='call_for_bids',
                tender_type_he='קול קורא',
                publication_id=None,
                tender_id='0',
            )
        PN += 1


def page_parser():
    def func(row):
        if row.get('parsed') is None:
            return
        url = row['page_url']
        print(url)
        page_text = requests.get(url).text
        page = pq(page_text)
        rules = [
            ('decision', '#MMDTendersStatusField span'),
            ('start_date', '.PublishingDate'),
            ('claim_date', '.LastDateForSubmission'),
            ('description', '#ctl00_PlaceHolderMain_ctl13_ctl00__ControlWrapper_RichHtmlField'),
        ]
        output = {}
        for rule, selector in rules:
            elements = page.find(selector)
            if len(elements):
                output[rule] = sanitize_html(pq(elements[0])).strip()
            else:
                output[rule] = None

        files = [l for l in page_text.split('\n') if 'var GovXDLFWrapperObj = ' in l]
        if len(files) > 0:
            files = files[0].split('var GovXDLFWrapperObj = ')[1].split(';//]]>')[0]
            files = json.loads(files)['Items']
            files = [
                dict(
                    link=f['fileURL'],
                    description=f['FileTitle'],
                )
                for f in files
            ]
        output['documents'] = files
        row['parsed'] = output
    return func


def flow(*_):
    return DF.Flow(
        scraper(),
        DF.filter_rows(lambda row: row['page_title'] and row['page_title'].startswith('קול קורא'), resources=-1),
        page_parser(),
        DF.add_field('decision', 'string',
                     default=lambda row: row['parsed']['decision'], resources=-1),
        DF.add_field('start_date', 'date', format='%d/%m/%Y',
                     default=lambda row: row['parsed']['start_date'], resources=-1),
        DF.add_field('claim_date', 'datetime', format='%d/%m/%Y',
                     default=lambda row: row['parsed']['claim_date'], resources=-1),
        DF.add_field('documents', 'array',
                     default=lambda row: row['parsed']['documents'], resources=-1),
        DF.delete_fields(['parsed'], resources=-1),
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