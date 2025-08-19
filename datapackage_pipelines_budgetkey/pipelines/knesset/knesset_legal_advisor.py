import dataflows as DF
from datapackage_pipelines_budgetkey.common.google_chrome import google_chrome_driver
from pyquery import PyQuery as pq
import hashlib
import os
import shutil

PREFIX = 'https://main.knesset.gov.il'
OUTPUT_PATH = '/var/datapackages/knesset/knesset_legal_advisor'

os.makedirs(OUTPUT_PATH, exist_ok=True)

def flow(*_):
    # Download the zip file
    gcl = google_chrome_driver(wait=False)
    main_index = gcl.html('https://main.knesset.gov.il/about/departments/pages/leg/ldguidelines.aspx')
    out = []
    count = 0
    existing = 0
    main_index = gcl.html(main_index)
    main_index = pq(main_index)
    anchors = main_index('a.LDDocLink')
    for anchor in anchors:
        anchor = pq(anchor)
        href = anchor.attr('href')
        if href.endswith('.pdf'):
            # print('HHH', href)
            # input('continue')
            if href.startswith('http'):
                url = href
            else:
                url = PREFIX + href
            title = anchor.text().strip()
            filename = hashlib.md5(url.encode()).hexdigest()[:16] + '.pdf'
            outpath = os.path.join(OUTPUT_PATH, filename)
            out.append(dict(
                url=url,
                title=title,
                filename=filename,
            ))
            # print('OUT', out)
            # assert False
            count += 1
            # print('HHH2', url, title, filename)
            # input('continue')
            if not os.path.exists(outpath):
                document = gcl.download(url, use_curl=True, outfile=filename)
                with open(document, 'rb') as i:
                    with open(outpath, 'wb') as o:
                        shutil.copyfileobj(i, o)
                print(out)
                existing += 1
        print(f'{href}: GOT {count} documents (out of which, {count-existing} are new)')
    gcl.teardown()

    return DF.Flow(
        out,
        DF.update_resource(-1, name='knesset_legal_advisor', path='index.csv', **{'dpp:streaming': True}),
        DF.dump_to_path(OUTPUT_PATH),
    )

if __name__ == '__main__':
    flow()