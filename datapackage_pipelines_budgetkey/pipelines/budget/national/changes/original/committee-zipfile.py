import dataflows as DF
import requests

YEAR = 2025
WEIRD_ZIP_FILE = f'https://main.knesset.gov.il/Activity/committees/Finance/Documents/p{YEAR}.zip'
OUT = '/var/datapackages/budget/national/changes/finance-committee.zip'

def flow(*_):
    # Download the zip file
    response = requests.get(WEIRD_ZIP_FILE)
    with open(OUT, 'wb') as f:
        f.write(response.content)

    return DF.Flow(
        [dict(success=True)],    
        DF.update_resource(-1, name='zip', **{'dpp:streaming': True}),
    )

