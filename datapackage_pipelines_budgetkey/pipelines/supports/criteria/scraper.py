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
            rec = {
                "publication_id": 0,
                "tender_type": "support_criteria",
                "tender_id": None,
                "documents": [dict(
                    link=extract_url(pq(cells[0].find('a')).attr('href')),
                    description=title,
                    update_date=date
                )],
                "page_title": title,
                "publisher": cells[2].text().split(':')[-1],
                "subject_list_keywords": cells[2].text().split(':')[:-1],
                "description": ','.join(cells[2].text().split(':')[:-1]),
                "reason": cells[3].text(),
                "start_date": date
            }
            yield rec
        if len(rows) == 0:
            break
        pid += 1


def flow(*_):
    return Flow(
        get_all_reports(),
        set_type('start_date', type='date', format='%d-%m-%Y'),
        set_type('subject_list_keywords', **{
            'type': 'array',
            'es:itemType': 'string',
            'es:title': True,
            'es:keyword': True,
        }),
        calculate_publication_id(1),
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
