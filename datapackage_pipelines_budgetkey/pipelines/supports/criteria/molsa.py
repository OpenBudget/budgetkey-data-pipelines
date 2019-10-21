from dataflows import Flow, set_type, concatenate, update_resource
from datapackage_pipelines.utilities.resources import PROP_STREAMING
import requests
from pyquery import PyQuery as pq
from datapackage_pipelines_budgetkey.common.publication_id import calculate_publication_id

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
    rows = rows[1:]
    for row in rows:
        content = [pq(x) for x in pq(row).find('td')]
        emails = content[4].find('a')
        row = dict(
            publication_id=0,
            tender_id='0',
            tender_type='support_criteria',
            tender_type_he='מבחן תמיכה: ' + content[0].text(),
            publisher='משרד העבודה, הרווחה והשירותים החברתיים',
            target_audience=content[1].text(),
            page_title=content[2].text(),
            page_url=URL,
            contact=content[4].text(),
            contact_email=pq(emails[0]).attr('href').replace('mailto:', '') if len(emails) > 0 else None,
            start_date=None,
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
        update_resource(
            -1, name='molsa',
            **{
                PROP_STREAMING: True
            }
        ),
        calculate_publication_id(4),
        set_type('claim_date', resources='molsa',
                 type='datetime', format='%d/%m/%Y %H:%M'),
    )


if __name__ == '__main__':
    flow().process()
