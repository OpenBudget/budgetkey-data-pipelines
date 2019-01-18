from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING

import os
import logging
import itertools
from sqlalchemy import create_engine


def generate_sitemap(kind, db_table, doc_id, page_title):
    engine = create_engine(os.environ['DPP_DB_ENGINE'])
    index = 0
    offset = 0
    logging.info('Kind %s', kind)
    while True:
        rows = (dict(r)
                for r in engine.execute('select * from {} limit 10000 '
                                        'offset {} order by __hash'
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
            for doc_id, last_modified, _ in batch:
                if last_modified:
                    last_modified = last_modified.isoformat()[:10]
                    last_modified = '<lastmod>{}</lastmod>'\
                                    .format(last_modified)
                else:
                    last_modified = '<lastmod>2019-01-15</lastmod>'
                doc_id = doc_id.replace('&', '&amp;')
                out.write('''   <url>
      <loc>https://next.obudget.org/i/{}</loc>{}
   </url>
'''.format(doc_id, last_modified))
            out.write('''</urlset>''')

        html_filename = '/var/datapackages/sitemaps/{}.{:04d}.html'\
                        .format(kind, index)
        with open(html_filename, 'w') as out:
            out.write('''<html><body><ul>''')
            for doc_id, _, page_title in batch:
                doc_id = doc_id.replace('&', '&amp;')
                page_title = page_title.replace('&', '&amp;')
                out.write('''   <li><a href='https://next.obudget.org/i/{}'>{}</a></li>
'''.format(doc_id, page_title))
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
