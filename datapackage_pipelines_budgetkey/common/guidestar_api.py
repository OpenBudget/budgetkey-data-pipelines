import os
from time import time
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
        row = requests.get(f'{self.BASE}/organizations/{regNum}', headers=self.headers(), timeout=self.TIMEOUT).json()
        if row.get('errorMsg') is not None:
            errorMsg = row['errorMsg']
            print(f'GUIDESTAR ERROR FETCHING ORGANIZATION {regNum}: {errorMsg}')
            return None
        return row
