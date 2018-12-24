from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING

import os
import logging
import itertools
from sqlalchemy import create_engine

def generate_sitemap(kind, db_table, doc_id):
    engine = create_engine(os.environ['DPP_DB_ENGINE'])
    rows = (dict(r) for r in engine.execute('select * from {}'.format(db_table)))
    doc_ids = [(doc_id.format(**r), r['__last_modified_at']) for r in rows]
    index = 0
    logging.info('Kind %s', kind)
    while len(doc_ids) > 0:
        batch = doc_ids[:10000]
        doc_ids = doc_ids[10000:]

        filename = '/var/datapackages/sitemaps/{}.{:04d}.xml'.format(kind, index)
        with open(filename, 'w') as out:
            out.write('''<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
''')
            for doc_id, last_modified in batch:
                if last_modified:
                    last_modified = last_modified.isoformat()[:10]
                    last_modified = '<lastmod>{}</lastmod>'.format(last_modified)
                else:
                    last_modified = ''
                doc_id = doc_id.replace('&', '&amp;')
                out.write('''   <url>
      <loc>https://next.obudget.org/i/{}</loc>{}     
   </url>
'''.format(doc_id, last_modified))
            out.write('''</urlset>''')

        logging.info('WRITTEN -> %s', filename)
        yield {'filename': filename}
        index += 1

def process_rows(res_iter):
    try:
        first = next(res_iter)
    except:
        first = []
    yield from itertools.chain(first, generate_sitemap(kind, db_table, doc_id))

if __name__ == '__main__':
    params, dp, res_iter = ingest()

    os.makedirs('/var/datapackages/sitemaps', exist_ok=True)

    kind = params['kind']
    db_table = params['db-table']
    doc_id = params['doc-id']

    if not dp.get('resources'):
        dp['resources'] = [
            {
                'name': 'sitemaps',
                'path': 'sitemaps.csv',
                PROP_STREAMING: True,
                'schema': {
                    'fields': [
                        {
                            'name': 'filename',
                            'type': 'string'
                        }
                    ]
                }
            }
        ]

    spew(dp, [process_rows(res_iter)])