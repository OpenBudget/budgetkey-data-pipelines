import requests
from pyquery import PyQuery as pq
from dataflows import Flow, update_resource
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines_budgetkey.common.publication_id import calculate_publication_id


def m_tmicha_scraper():

    url = 'https://www.health.gov.il/Subjects/Finance/Mtmicha/Pages/default.aspx'
    s = requests.Session()
    page = pq(s.get(url).text)

    id_ = 'div#ctl00_PlaceHolderMain_GovXContentSectionPanel_ctl00__ControlWrapper_RichHtmlField'
    data = page.find(id_)
    rows = data.find('a')
    total = 0

    for i in range(len(rows)):
        if rows[i].attrib['href'][-3:] == 'pdf':
            link = 'https://www.health.gov.il' + rows[i].attrib['href']
            title = rows[i].text
            yield dict(
                publication_id=0,
                tender_id='0',
                tender_type='support_criteria',
                tender_type_he='מבחני תמיכה',

                page_title=title,
                page_url=url,
                publisher='משרד הבריאות',

                start_date=None,

                target_audience='מוסדות ציבור',

                documents=[dict(link=link, description=title)],
            )
            total += 1
    assert total > 0


def flow(*args):
    return Flow(
        m_tmicha_scraper(),
        calculate_publication_id(5),
        update_resource(
            -1, name='support_criteria_from_ministry_of_health',
            **{
                PROP_STREAMING: True
            }
        )
    )
