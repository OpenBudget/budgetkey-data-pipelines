import re
import requests

cookie_re = re.compile("document.cookie='([^=]+)=([^;]+); path=/'")


def cookie_monster_iter(url, chunk=1024*1024):

    session = requests.Session()
    headers = {'range': 'bytes=0-400'}
    content_length = None

    while content_length is None:
        resp = session.get(url, headers=headers)
        if 'accept-ranges' in resp.headers and not 'content-range' in resp.headers:
            if 'content-length' in resp.headers:
                content_length = max(100*1024*1024, int(resp.headers['content-length']))
            else:
                content_length = 100*1024*1024

        data = resp.content
        if len(data) < 200:
            decoded = data.decode('ascii')
            found_cookies = cookie_re.findall(decoded)
            if len(found_cookies) > 0:
                found_cookies = found_cookies[0]
                session.cookies.set(found_cookies[0], found_cookies[1], path='/')
                continue

            return
        else:
            break

    for ofs in range(0, 100*1024*1024, chunk):
        headers['range'] = 'bytes={}-{}'.format(ofs, ofs+chunk-1)
        resp = session.get(url, headers=headers)
        if resp.status_code not in (206,) or resp.headers.get('content-length') == 0:
            break
        yield resp.content
        if len(resp.content) > chunk:
            break


def cookie_monster_get(url):
    data = b''
    for content in cookie_monster_iter(url):
        data += content
    return data