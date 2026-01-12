import dataflows as DF
# import requests
import shutil

from datapackage_pipelines_budgetkey.common.google_chrome import google_chrome_driver

YEAR = 2026
WEIRD_ZIP_FILE = f'https://main.knesset.gov.il/Activity/committees/Finance/Documents/p{YEAR}.zip'
OUT = '/var/datapackages/budget/national/changes/finance-committee.zip'

def flow(*_):
    # Download the zip file
    gcl = google_chrome_driver()
    archive_ = gcl.download(WEIRD_ZIP_FILE, outfile=f'p{YEAR}.zip')
    gcl.teardown()

    with open(OUT, 'wb') as f:
        with open(archive_, 'rb') as archive:
            shutil.copyfileobj(archive, f)

    # response = requests.get(WEIRD_ZIP_FILE)
    # with open(OUT, 'wb') as f:
    #     f.write(response.content)

    return DF.Flow(
        [dict(success=True)],    
        DF.update_resource(-1, name='zip', **{'dpp:streaming': True}),
    )

