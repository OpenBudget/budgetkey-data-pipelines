import dataflows as DF
from datapackage_pipelines_budgetkey.common.google_chrome import google_chrome_driver
from pyquery import PyQuery as pq
import hashlib
import os
import shutil

PREFIX = 'https://main.knesset.gov.il'
CURRENT = '/Activity/committees/Ethics/pages/CommitteeDecisions25.aspx'
OUTPUT_PATH = '/var/datapackages/knesset/ethics_committee_decisions'

os.makedirs(OUTPUT_PATH, exist_ok=True)

def flow(*_):
    # Download the zip file
    gcl = google_chrome_driver(wait=False)
    main_index = gcl.html('https://main.knesset.gov.il/Activity/committees/Ethics/Pages/CommitteeDecisionsPast.aspx')
    hrefs = {CURRENT}
    main_index = pq(main_index)
    all_anchors = main_index('table a')
    for anchor in all_anchors:
        href = anchor.attrib['href']
        if href.startswith('/Activity/committees/Ethics/pages/CommitteeDecisions'):
            print(href)
            hrefs.add(href)
    out = []
    for href in hrefs:
        print(href)
        count = 0
        existing = 0
        year_index = gcl.html(PREFIX + href)
        year_index = pq(year_index)
        rows = year_index('tr')
        for row in rows:
            row = pq(row)
            anchor = row.find('a')
            date = row.find('.ComEthicsTdDate')
            if not anchor or not date:
                continue
            if len(anchor) > 1 or len(date) > 1:
                continue
            date = pq(date).text().strip()
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
                    date=date,
                ))
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
        DF.update_resource(-1, name='ethics_committee_decisions', path='index.csv', **{'dpp:streaming': True}),
        DF.dump_to_path(OUTPUT_PATH),
    )

if __name__ == '__main__':
    flow()