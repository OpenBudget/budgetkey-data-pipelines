from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING

import os
import logging
import itertools
from sqlalchemy import create_engine


def generate_sitemap(kind, db_table, doc_id, page_title):
    engine = create_engine(os.environ['DPP_DB_ENGINE']).connect()
    index = 0
    offset = 0
    logging.info('Kind %s', kind)
    while True:
        rows = (dict(r)
                for r in engine.execute('select * from {} order by __hash '
                                        'limit 10000 offset {}'
                                        .format(db_table, offset)))
        batch = [(doc_id.format(**r),
                  r['__last_modified_at'],
                  page_title.format(**r))
                 for r in rows]
        if len(batch) == 0:
            break

        filename = '/var/datapackages/sitemaps/{}.{:04d}.xml'\
                   .format(kind, index)
        with open(filename, 'w') as out:
            out.write('''<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
''')
            for did, last_modified, _ in batch:
                if last_modified and last_modified.isoformat() > '2021-01-01':
                    last_modified = last_modified.isoformat()[:10]
                    last_modified = '<lastmod>{}</lastmod>'\
                                    .format(last_modified)
                else:
                    last_modified = '<lastmod>2021-01-01</lastmod>'
                did = did.replace('&', '&amp;')
                out.write('''   <url>
      <loc>https://next.obudget.org/i/{}</loc>{}
   </url>
'''.format(did, last_modified))
            out.write('''</urlset>''')

        html_filename = '/var/datapackages/sitemaps/{}.{:04d}.html'\
                        .format(kind, index)
        with open(html_filename, 'w') as out:
            out.write('''<!DOCTYPE html>
        <html><head><meta charset="utf-8"></head>
        <body><ul>
        ''')
            for did, _, pt in batch:
                did = did.replace('&', '&amp;')
                pt = pt.replace('&', '&amp;')
                out.write('''   <li><a href='https://next.obudget.org/i/{}'>{}</a></li>
'''.format(did, pt))
            out.write('''</ul></body></html>''')

        logging.info('WRITTEN -> %s', filename)
        yield {'filename': filename}

        index += 1
        offset += 10000


def process_rows(res_iter, kind, db_table, doc_id, page_title):
    try:
        first = next(res_iter)
    except Exception:
        first = []
    yield from itertools.chain(first,
                               generate_sitemap(kind, db_table,
                                                doc_id, page_title))


def main():
    params, dp, res_iter = ingest()

    os.makedirs('/var/datapackages/sitemaps', exist_ok=True)

    kind = params['kind']
    db_table = params['db-table']
    doc_id = params['doc-id']
    page_title = params['page-title']

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

    spew(dp, [process_rows(res_iter, kind, db_table, doc_id, page_title)])


if __name__ == '__main__':
    main()
