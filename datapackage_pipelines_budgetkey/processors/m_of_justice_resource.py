import csv
from io import RawIOBase

import itertools

import io
from datapackage_pipelines.utilities.resources import PROP_STREAMING

from datapackage_pipelines_budgetkey.common.cookie_monster import cookie_monster_iter

from datapackage_pipelines.wrapper import ingest, spew


def clean_machine(biter):
    last = None
    whitespace = b''
    newline = False
    out = b''
    for bstr in biter:
        for i in range(len(bstr)):
            b = bstr[i:i+1]
            if b in b'\r\n\t ':
                newline = b in b'\r\n'
                whitespace += b
            else:
                if whitespace:
                    if out:
                        yield out.decode('cp1255', 'replace').encode('utf8')
                        out = b''
                    if b != b'5':
                        if last != b',' and b != b',':
                            out += b' '
                    else:
                        if newline:
                            out += b'\n'
                        else:
                            out += b' '
                    whitespace = b''
                    newline = False
                last = b
                out += b
    if out:
        yield out.decode('cp1255', 'replace').encode('utf8')


class IterStreamer(RawIOBase):
    
    def __init__(self, data):
        super(IterStreamer, self).__init__()
        self.data = itertools.chain.from_iterable(data)
    
    def readable(self):
        return True
    
    def readinto(self, b):
        l = len(b)  # We're supposed to return at most this much
        output = list(itertools.islice(self.data, l))
        for i, x in enumerate(output):
            b[i] = x
        return len(output)


def get_entities(url_key):

    all_db_url = 'https://www.justice.gov.il/DataGov/Corporations/{}.csv'.format(url_key)

    data = cookie_monster_iter(all_db_url, chunk=100*1024)
    data = clean_machine(data)
    stream = io.TextIOWrapper(io.BufferedReader(IterStreamer(data), buffer_size=1024), encoding='utf8')

    def process_v(k, v):
        ret = str(v).strip()
        if k.endswith('Date'):
            ret = ret.split('.')[0]
        return ret

    reader = csv.DictReader(stream)
    for rec in reader:
        yield dict(
            (k.strip(), process_v(k, v)) for k, v in rec.items()
        )


def main():
    params, datapackage, res_iter = ingest()

    key = params['key']
    url_key = params['url-key']
    resource_name = params['resource-name']

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

    spew(datapackage, [get_entities(url_key)])

if __name__=='__main__':
    main()
