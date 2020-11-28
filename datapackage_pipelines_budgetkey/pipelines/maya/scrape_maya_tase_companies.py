from dataflows import Flow, printer, update_resource, add_field
import logging

from time import sleep
import requests
from pyquery import PyQuery as pq


from datapackage_pipelines.utilities.resources import PROP_STREAMING
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def scrape_event_target(href):
    sidx = href.find("'")
    eidx = href.find("'", sidx+1)
    return href[sidx+1:eidx]


def scrape_maya_tase_companies():
    req = requests.get('https://info.tase.co.il/heb/marketdata/tase_companies/Pages/tase_companies.aspx')
    sleep(3)
    doc = pq(req.text)
    yield from collect_companies_from_document(doc)

    keys_to_remove = ['MSOSPWebPartManager_StartWebPartEditingName', 'sa', '__REQUESTDIGEST', 'MSOSPWebPartManager_OldDisplayModeName', 'MSOSPWebPartManager_ExitingDesignMode', 'MSOSPWebPartManager_DisplayModeName']

    query = {i.attr('name'): [i.attr('value')] for i in doc(":input").items()
             if i.attr('name')
             and i.attr('value')
             and i.attr('name') not in keys_to_remove
             }
    targets = [scrape_event_target(link.attr('href'))  for link in doc('.pagerText a').items()]

    for target in targets:
        query['__EVENTTARGET'] = [target]
        req = requests.post('https://info.tase.co.il/heb/marketdata/tase_companies/Pages/tase_companies.aspx',
                   data=query)
        sleep(3)
        doc = pq(req.text)
        yield from collect_companies_from_document(doc)



def collect_companies_from_document(doc):
    for item in doc(".linkInGrid").items():
        yield {
            "CompanyName": item.text(),
            "CompanyTaseId": item.attr('href').split('/')[-2]
        }

def filter_invalid(rows):
    for row in rows:
        if row['CompanyTaseId'] == 'hitechfund':
            continue
        yield row

def flow(*_):
    return Flow(
        add_field('CompanyName', 'string'),
        add_field('CompanyTaseId', 'string'),
        scrape_maya_tase_companies(),
        filter_invalid,
        update_resource(
            -1, name='scrape_maya_tase_companies', path="data/scrape_maya_tase_companies.csv",
            **{
                PROP_STREAMING: True,
            }
        ),
    )


if __name__ == '__main__':
    Flow(
        flow(),
        printer(),
    ).process()
