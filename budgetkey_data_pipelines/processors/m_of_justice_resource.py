import re
import requests
import csv
import logging
import time
from io import StringIO

from datapackage_pipelines.wrapper import ingest, spew

params, datapackage, res_iter = ingest()

key = params['key']
url_key = params['url-key']
resource_name = params['resource-name']
session = requests.Session()

cookie_re = re.compile("document.cookie='([^=]+)=([^;]+); path=/'")

def get_entities():

    all_db_url = 'http://www.justice.gov.il/DataGov/Corporations/{}.csv'.format(url_key)
    headers = {}
    while True:

        resp = requests.get(all_db_url, headers=headers)
        if 'accept-ranges' in resp.headers and not 'content-range' in resp.headers:
            content_length = resp.headers['content-length']
            headers['range'] = 'bytes=0-%s' % content_length
            logging.info('Accept Ranges %r', headers)
            continue

        data = resp.content
        logging.info('GOT DATA %r', data[:1024])

        data = data.decode('cp1255', 'replace')
        logging.info('GOT DATA %d bytes', len(data))

        found_cookies = cookie_re.findall(data)
        if len(found_cookies)>0:
            session.cookies.set(found_cookies[0], found_cookies[1], path='/')
            continue

        assert len(data) > 1024
        break
        
    repl1 = re.compile(",[\r\n\t ]+(?=[^5])")
    repl2 = re.compile("[\r\n\t ]+,")
    repl3 = re.compile("[\r\n]+(?=[^5])")

    l = 0
    while len(data) != l:
        l = len(data)
        data = repl1.sub(",", data)
        data = repl2.sub(",", data)
        data = repl3.sub(" ", data)

    reader = csv.DictReader(StringIO(data))
    for rec in reader:
        yield dict(
            (k.strip(), str(v).strip()) for k, v in rec.items()
        )
resource = {
    'name': resource_name,
    'path': 'data/{}.csv'.format(resource_name),
    'schema': {
        'fields': [
            {'name': '{}_Number'.format(key), 'type': 'string'},
            {'name': '{}_Name'.format(key), 'type': 'string'},
            {'name': '{}_Registration_Date'.format(key), 'type': 'string'},
        ]
    }
}

datapackage['resources'].append(resource)

spew(datapackage, [get_entities()])
