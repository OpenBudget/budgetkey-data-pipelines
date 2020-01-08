import re
import requests
import time
import logging


class GuidestarApi():

    HEADERS = {
        'X-User-Agent': 'Visualforce-Remoting',
        'Origin': 'https://www.guidestar.org.il',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,he;q=0.8',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'Referer': 'https://www.guidestar.org.il/organization/580030104',
        'X-Requested-With': 'XMLHttpRequest',
        'Connection': 'keep-alive',
    }

    def __init__(self, guidestar_url):
        page = None
        while page is None:
            try:
                response = requests.get(guidestar_url)
                if response.status_code == 200:
                    page = response.text
            except Exception:
                time.sleep(60)

            if page is None:
                continue

        CSRF_RE = re.compile(r'\{"name":"getUserInfo","len":0,"ns":"","ver":43\.0,"csrf":"([^"]+)"\}')
        VID_RE = re.compile(r'RemotingProviderImpl\(\{"vf":\{"vid":"([^"]+)"')
        self.csrf = CSRF_RE.findall(page)[0]
        self.vid = VID_RE.findall(page)[0]

    def prepare(self):
        self.body = []
        return self

    def method(self, method_name, data, ver=39):
        self.body.append({
            'action': 'GSTAR_Ctrl',
            'method': method_name,
            'data': data,
            'type': 'rpc',
            'tid': 3 + len(self.body),
            'ctx': {
                'csrf': self.csrf,
                'ns': '',
                'vid': self.vid,
                'ver': ver
            }
        })
        return self

    def run(self):
        data = None
        for i in range(5):
            try:
                data = requests.post('https://www.guidestar.org.il/apexremote', 
                                     json=self.body, headers=self.HEADERS).json()
                break
            except Exception as e:
                logging.exception('Failed to fetch data %r: %r', e, self.body[0]['data'])
                time.sleep(60)
        if data is None:
            return
        for entry in data:
            if 'result' not in entry:
                logging.error('Malformed data %r', data)
                data = None
                break
        return data
