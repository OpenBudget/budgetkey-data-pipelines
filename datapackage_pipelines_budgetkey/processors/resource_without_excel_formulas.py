import logging
import requests
import tempfile
import tabulator
import itertools

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest

from datapackage_pipelines_budgetkey.common.cookie_monster import cookie_monster_get


with tempfile.NamedTemporaryFile(suffix='.csv') as out:
    with ingest() as ctx:
        parameters, datapackage, res_iter = tuple(ctx)

        url = parameters.get('url')
        resource = parameters.get('resource')
        resource[PROP_STREAMING] = True

        content = requests.get(url, verify=False).content
        if len(content) < 1024:
            content = cookie_monster_get(url)

        content = content.replace(b'\n="', b'\n"')
        content = content.replace(b',="', b',"')

        out.write(content)
        out.flush()

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
        ctx.resource_iterator = itertools.chain(res_iter, [stream.iter(keyed=True)])
