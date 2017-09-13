import logging
import requests
import tempfile
import tabulator
import itertools

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()

url = parameters.get('url')
resource = parameters.get('resource')
resource[PROP_STREAMING] = True

content = requests.get(url).content
content = content.replace(b'\n="', b'\n"')
content = content.replace(b',="', b',"')

out = tempfile.NamedTemporaryFile(suffix='.csv', delete=False)
out.write(content)

logging.info('downloaded from %s %d bytes: %r', url, len(content), content[:1000])

datapackage['resources'].append(resource)

stream = \
    tabulator.Stream('file://'+out.name, **parameters.get('tabulator', {}))\
    .open()
resource['schema'] = {
    'fields': [
        {'name': h, 'type': 'string'}
        for h in stream.headers
    ]
}

spew(datapackage,
     itertools.chain(res_iter, [stream.iter(keyed=True)]))
