# coding=utf-8
import hashlib

import requests
from itertools import chain

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from pyquery import PyQuery as pq

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()

sids = range(32)
SEARCH_PAGE_RESULTS_URL = "http://www.boi.org.il/he/DataAndStatistics/_layouts/boi/handlers/WebPartHandler.aspx?wp" \
                          "=SeriesSearchBySubject&lang=he-IL&PaggingSize=100&Level=4&sId={sid} "
REPORT_URL = "http://www.boi.org.il/he/DataAndStatistics/Pages/SeriesData.aspx?SeriesCode={" \
             "serie_code}&DateStart=01/01/1900&DateEnd=01/01/2100&Level=4&Sid={sid} "


def get_series(sid):
    session = requests.Session()
    response = session.get(SEARCH_PAGE_RESULTS_URL.format(sid=sid))
    data = response.content
    resp = pq(data.decode(response.encoding))
    results = []
    for serie_code in resp.find("table tr input.hidSeriesCode"):
        results.append((sid, serie_code.attrib['value'], serie_code.getparent().find("input[@class=\"hidOutpuFreq\"]").attrib['value']))
    return results


def get_report(sid, serie_code):
    session = requests.Session()
    response = session.get(REPORT_URL.format(sid=sid, serie_code=serie_code))
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


def get_all_reports(series):
    for sid, code, freq in series:
        report = get_report(sid, code)
        yield [{
                "description": report['metadata'][u'תיאור'],
                "units": report['metadata'][u'יחידות'],
                "season_adjusted": report['metadata'][u'ניכוי עונתיות'],
                "date": value['date'],
                "value": value['value']
            } for value in report['values']]


def add_series_to_resources(series):
    for sid, code, freq in series:
        hexdigest = hashlib.md5(code.encode('utf-8')).hexdigest()
        if freq == u'חודשית (M)':
            date_format = '%m/%Y'
        elif freq == u'רבעונית (Q)':
            date_format = '%d/%m/%Y'
        elif freq == u'שנתית (A)':
            date_format = '%d/%m/%Y'
        else:
            # TODO: log the frequency so it can be fixed
            continue
        resource = {
            PROP_STREAMING: True,
            'name': hexdigest,
            'path': 'data/bank_of_israel/' + hexdigest + '.csv',
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


series = sum([get_series(sid) for sid in sids], [])
add_series_to_resources(series)
reports = get_all_reports(series)
spew(datapackage, chain(res_iter, reports))
