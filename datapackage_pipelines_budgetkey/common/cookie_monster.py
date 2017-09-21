import re
import requests

cookie_re = re.compile("document.cookie='([^=]+)=([^;]+); path=/'")


def cookie_monster_get(url):

    session = requests.Session()
    headers = {}

    while True:
        resp = session.get(url, headers=headers)
        if 'accept-ranges' in resp.headers and not 'content-range' in resp.headers:
            content_length = resp.headers['content-length']
            headers['range'] = 'bytes=0-%s' % content_length
            continue

        data = resp.content

        if len(data) < 200:
            decoded = data.decode('ascii')
            found_cookies = cookie_re.findall(decoded)
            if len(found_cookies) > 0:
                found_cookies = found_cookies[0]
                session.cookies.set(found_cookies[0], found_cookies[1], path='/')
                continue

            return None

        else:
            return data

