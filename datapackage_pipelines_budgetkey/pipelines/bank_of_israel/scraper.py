# coding=utf-8
import requests
from itertools import chain

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from pyquery import PyQuery as pq

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()

SEARCH_PAGE_RESULTS_URL = "http://www.boi.org.il/he/DataAndStatistics/Pages/SeriesSearchBySubject.aspx?Level=4&sId=16"
REPORT_URL = "http://www.boi.org.il/he/DataAndStatistics/Pages/SeriesData.aspx?SeriesCode={serie_code}&DateStart=01/01/1900&DateEnd=01/01/2100&Level=4&Sid=16"

def get_serie_codes():
    session = requests.Session()
    response = session.get(SEARCH_PAGE_RESULTS_URL)
    data = response.content
    resp = pq(data.decode(response.encoding))
    return [serie_code_title.getparent().text_content().split(" ")[-1]
            for serie_code_title in resp.find(u"table tr b:contains('קוד הסדרה:')")]


def get_report(serie_code):
    session = requests.Session()
    response = session.get(REPORT_URL.format(serie_code=serie_code))
    data = response.content
    resp = pq(data.decode(response.encoding))
    rows = resp.find("table tr")
    metadata = {}
    last_metadata_row_index = -1
    for index, tr in enumerate(rows):
        metadata[tr.find("th").text[:-1]] = tr.find("td").text
        if 'lastRow' in tr.attrib.get('class', '').split(" "):
            last_metadata_row_index = index
            break
    values = []
    for tr in rows[last_metadata_row_index + 1:]:
        values.append({"date": tr.getchildren()[0].text, "value": tr.getchildren()[1].text.strip()})
    return {'metadata': metadata, 'values': values}


def get_all_reports():
    serie_codes = get_serie_codes()
    for serie_code in serie_codes:
        report = get_report(serie_code)
        if report['metadata'][u'תדירות'] == u'חודשית (M)':
            date_format = '%m/%Y'
        elif report['metadata'][u'תדירות'] == u'רבעונית (Q)':
            date_format = '%d/%m/%Y'
        else:
            # TODO: log the frequency so it can be fixed
            continue
        resource = {
            PROP_STREAMING: True,
            'schema': {
                'fields': [

                    {
                        'name': 'description',
                        'type': 'string'
                    },
                    {
                        'name': 'units',
                        'type': 'string'
                    },
                    {
                        'name': 'season_adjusted',
                        'type': 'string'
                    },
                    {
                        'name': 'date',
                        'type': 'date',
                        'format': date_format
                    },
                    {
                        'name': 'value',
                        'type': 'string'
                    },
                ]
            }
        }
        datapackage['resources'].append(resource)
        for value in report['values']:
            rec = {
                "description": report['metadata'][u'תיאור'],
                "units": report['metadata'][u'יחידות'],
                "season_adjusted": report['metadata'][u'ניכוי עונתיות'],
                "date": value['date'],
                "value": value['value']
            }
            yield rec


# resource = parameters['target-resource']
# resource[PROP_STREAMING] = True
# resource['schema'] = {
#     'fields': [
#
#         {
#             'name': 'description',
#             'type': 'string'
#         },
#         {
#             'name': 'units',
#             'type': 'string'
#         },
#         {
#             'name': 'season_adjusted',
#             'type': 'string'
#         },
#         {
#             'name': 'date',
#             # 'type': 'string',
#             'type': 'date',
#             'format': '%d-%m-%Y'
#         },
#         {
#             'name': 'value',
#             'type': 'string'
#         },
#     ]
# }
# datapackage['resources'].append(resource)

spew(datapackage, chain(res_iter, [get_all_reports()]))
