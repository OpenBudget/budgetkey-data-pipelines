from pyquery import PyQuery as pq

from dataflows import Flow, printer, set_type, set_primary_key, \
        delete_fields, update_resource

from datapackage_pipelines.utilities.resources import PROP_STREAMING

from datapackage_pipelines_budgetkey.common.cookie_monster import cookie_monster_get
from datapackage_pipelines_budgetkey.common.publication_id import calculate_publication_id


URL = "https://www.justice.gov.il/Units/Tmihot/Pages/TestServies.aspx?WPID=WPQ8&PN={0}"


def extract_url(href):
    if href is None:
        return None
    marker = "OpenWindow(this,'"
    start = href.find(marker)
    assert start > 0
    start += len(marker)
    return href[start:-2]


def get_all_reports():
    pid = 1
    total = 0
    while True:
        response = cookie_monster_get(URL.format(pid))
        response = response.decode('utf8')
        response = pq(response)
        rows = response.find('#cqwpGridViewTable .zbTRBlue, #cqwpGridViewTable .Trtblclss')
        rows = list(rows)
        for r in rows:
            row = pq(r)
            cells = [pq(td) for td in row.find('td')]
            title = cells[1].text()
            date = cells[4].text()
            if date:
                revdate = date.split('-')
                revdate.reverse()
                revdate = '-'.join(revdate)
            rec = {
                "publication_id": 0,
                "tender_type": "support_criteria",
                "page_url": URL.format(pid),
                "tender_type_he": cells[3].text().replace('מבחני ', 'מבחן '),
                "tender_id": '0',
                "documents": [dict(
                    link=extract_url(pq(cells[0].find('a')).attr('href')),
                    description=title,
                    update_time=revdate
                )],
                "page_title": title,
                "publisher": cells[2].text().split(':')[-1],
                "start_date": date
            }
            yield rec
        total += len(rows)
        if len(rows) == 0:
            assert total > 0
            break
        pid += 1


def flow(*_):
    return Flow(
        get_all_reports(),
        calculate_publication_id(1),
        set_type('start_date', type='date', format='%d-%m-%Y'),
        set_primary_key(['publication_id']),
        update_resource(
            -1, name='criteria',
            **{
                PROP_STREAMING: True
            }
        ),
    )


if __name__ == '__main__':
    Flow(
        flow(),
        printer(),
    ).process()
