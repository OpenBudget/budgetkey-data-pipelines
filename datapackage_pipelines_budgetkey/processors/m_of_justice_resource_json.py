import re
import csv
import logging
from io import StringIO

from datapackage_pipelines.utilities.resources import PROP_STREAMING

from datapackage_pipelines_budgetkey.common.cookie_monster import cookie_monster_get

from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, res_iter = ingest()

key = params['key']
url_key = params['url-key']
resource_name = params['resource-name']

hdr_num = '{}_Number'.format(key)
hdr_name = '{}_Name'.format(key)
hdr_reg_date = '{}_Registration_Date'.format(key)

res = [
    r'"{}":([^,]*)'.format(hdr_num),
    r'"{}":((null)|(.+?[^\\]"))'.format(hdr_name),
    r'"{}":((null)|(.+?[^\\]"))'.format(hdr_reg_date),
]
res = [
    re.compile(r.format(url_key), re.MULTILINE) for r in res
    ]


def treat(data):
    for i, d in enumerate(data):
        d=d[0]
        if i == 0:
            yield d
        else:
            if d == 'null':
                yield None
            else:
                d = d[1:-1]
                yield d


def ifindall(r, data):
    for match in r.finditer(data):
        yield tuple(match.groups())

def get_entities():

    all_db_url = 'http://www.justice.gov.il/DataGov/Corporations/{}.json.txt'.format(url_key)

    data = cookie_monster_get(all_db_url)

    assert data is not None
    data = data.decode('utf-8', 'replace')

    logging.info('DECODED DATA %r', data[:1024])
    logging.info('DECODED STARTING LINES:\n%s', data[:1024].split('\n')[:-1])
    logging.info('DECODED ENDING LINES:\n%s', data[-1024:].split('\n')[1:])
    logging.info('DECODED DATA %r', data[:1024])
    logging.info('LENGTH DATA %d bytes', len(data))
    logging.info('LENGTH DATA %d lines', data.count('\r\n'))

    data = data.replace('\r\n',' ')
    data = data.replace('  ',' ')
    data = data.replace('\n',' ')

    datums = [
        ifindall(r, data)
        for r in res
    ]
    datums = zip(*datums)

    headers = [hdr_num, hdr_name, hdr_reg_date]
    for data in datums:
        yield dict(
            zip(headers, treat(data))
        )

resource = {
    'name': resource_name,
    PROP_STREAMING: True,
    'path': 'data/{}.csv'.format(resource_name),
    'schema': {
        'fields': [
            {'name': hdr_num, 'type': 'string'},
            {'name': hdr_name, 'type': 'string'},
            {'name': hdr_reg_date, 'type': 'string'},
        ]
    }
}

datapackage['resources'].append(resource)

spew(datapackage, [get_entities()])
