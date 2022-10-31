import os
import logging
import requests

GUIDESTAR_USERNAME = os.environ['GUIDESTAR_USERNAME']
GUIDESTAR_PASSWORD = os.environ['GUIDESTAR_PASSWORD']

class GuidestarAPI():

    BASE = 'https://www.guidestar.org.il/services/apexrest/api'
    TIMEOUT = 30
    _headers = None

    def __init__(self):
        pass

    def login(self, username, password):
        resp = requests.post(f'{self.BASE}/login', json=dict(username=username, password=password)).json()
        sessionId = resp['sessionId']
        headers = dict(
            Authorization=f'Bearer {sessionId}'
        )
        return headers

    def headers(self):
        if self._headers is None:
            self._headers = self.login(GUIDESTAR_USERNAME, GUIDESTAR_PASSWORD)
        return self._headers

    def organization(self, regNum):
        try:
            resp = requests.get(f'{self.BASE}/organizations/{regNum}', headers=self.headers(), timeout=self.TIMEOUT)
            if resp.status_code == 200:
                row = resp.json()
                if row.get('errorMsg') is None:
                    return row
                else:
                    errorMsg = row['errorMsg']
                    logging.error(f'GUIDESTAR ERROR FETCHING ORGANIZATION {regNum}: {errorMsg}')
        except Exception as e:
            logging.error(f'GUIDESTAR EXCEPTION FETCHING ORGANIZATION {regNum}: {e}')