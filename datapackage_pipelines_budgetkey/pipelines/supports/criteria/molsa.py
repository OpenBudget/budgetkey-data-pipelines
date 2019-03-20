from dataflows import Flow, printer, set_type
import requests
from pyquery import PyQuery as pq

BASE = 'https://www.molsa.gov.il'
URL = BASE + '/subsidizing/pages/supportshomepage.aspx'
HEADERS = {
    'Origin': 'https://www.molsa.gov.il',
    'Content-Type': 'application/x-www-form-urlencoded',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Referer': 'https://www.molsa.gov.il/subsidizing/pages/supportshomepage.aspx',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9,he;q=0.8',
}

DATA = {
    'ctl00_ucMainMenu1_RadMenu1_ClientState': '', 
    'ctl00$SPWebPartManager1$g_fac41b7b_0db1_4ebf_a1bd_dcb031cef425$SupportBudgetYear': 2015,
    'ctl00$SPWebPartManager1$g_fac41b7b_0db1_4ebf_a1bd_dcb031cef425$SupportPublish': 1,
    'ctl00$SPWebPartManager1$g_fac41b7b_0db1_4ebf_a1bd_dcb031cef425$GSupportTypeName': '',
    'ctl00$SPWebPartManager1$g_9d8680de_ba7b_4218_b33c_3b512d9099f1$ctl02': '', 
}


def rebase_url(url):
    if url.startswith('/'):
        return BASE + url
    else:
        return url


def get_results():
    page = requests.post(URL, data=DATA).text
    page = pq(page)
    rows = page.find('.ms-listviewtable tr')
    # headers = pq(rows[0])
    # headers = [pq(x).text() for x in headers.find('th')]
    # print(headers)
    headers = [
        'reason',
        'target_audience',
        'page_title',
        'year',
        'contact',
        'claim_date',
        'documents',
        'description',
        'decision',
    ]
    rows = rows[1:]
    for row in rows:
        content = [pq(x) for x in pq(row).find('td')]
        emails = content[4].find('a')
        row = dict(
            reason=content[0].text(),
            target_audience=content[1].text(),
            page_title=content[2].text(),
            contact=content[4].text(),
            contact_email=pq(emails[0]).attr('href').replace('mailto:', '') if len(emails) > 0 else None,
            claim_date=' '.join(content[5].text().split()[-2:]),
            documents=[
                dict(
                    link=rebase_url(pq(li).attr('href')),
                    description=pq(li).text(),
                )
                for li in content[6].find('a')
            ],
            description=content[7].text(),
            decision=content[8].text(),
        )
        yield row


def flow(*args):
    return Flow(
        get_results(),
        set_type('claim_date', type='datetime', format='%d/%m/%Y %H:%M'),
        printer()
    )


if __name__ == '__main__':
    flow().process()
