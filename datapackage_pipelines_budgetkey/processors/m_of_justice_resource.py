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


def get_entities():

    all_db_url = 'http://www.justice.gov.il/DataGov/Corporations/{}.csv'.format(url_key)

    data = cookie_monster_get(all_db_url)

    assert data is not None
    data = data.decode('cp1255', 'replace')

    logging.info('DECODED DATA %r', data[:1024])
    logging.info('DECODED STARTING LINES:\n%s', data[:1024].split('\n')[:-1])
    logging.info('DECODED ENDING LINES:\n%s', data[-1024:].split('\n')[1:])
    logging.info('DECODED DATA %r', data[:1024])
    logging.info('LENGTH DATA %d bytes', len(data))

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
    PROP_STREAMING: True,
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
