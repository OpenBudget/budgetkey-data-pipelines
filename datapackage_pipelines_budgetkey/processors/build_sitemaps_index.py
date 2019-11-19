from datapackage_pipelines.wrapper import ingest, spew

import os
import logging
import datetime

def generate_sitemap_index(rows):
    rows = list(next(rows))
    now = datetime.datetime.now().date().isoformat()
    with open('/var/datapackages/sitemaps/common.xml', 'w') as out:
        out.write('''<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
''')
        for href in ('', 's/'):
            out.write('''   <url>
      <loc>https://next.obudget.org/{}</loc>
      <lastmod>{}</lastmod>
   </url>
'''.format(href, now[:10]))
        out.write('''</urlset>''')

    with open('/var/datapackages/sitemaps/sitemap.xml', 'w') as out:
        out.write('''<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    <sitemap>
        <loc>https://next.obudget.org/datapackages/sitemaps/common.xml</loc>
    </sitemap>
''')
        for row in rows:
            out.write('''<sitemap>
      <loc>https://next.obudget.org/{}</loc>
   </sitemap>
'''.format(row['filename'].replace('/var/', '')))

        out.write('''</sitemapindex>''')

    with open('/var/datapackages/sitemaps/sitemap.html', 'w') as out:
        out.write('''<!DOCTYPE html>
    <html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"></head>
    <body><ul>
    ''')
        for row in rows:
            fn = row['filename'].replace('/var/datapackages/sitemaps/', '').replace('.xml', '.html')
            out.write('''<li><a href="{}">{}</a></li>
'''.format(fn, fn))

        out.write('''</ul></body></html>''')


if __name__ == '__main__':
    params, dp, res_iter = ingest()

    os.makedirs('/var/datapackages/sitemaps', exist_ok=True)

    generate_sitemap_index(res_iter)

    spew(dp, [[]])
