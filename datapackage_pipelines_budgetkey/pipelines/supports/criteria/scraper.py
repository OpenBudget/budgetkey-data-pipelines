from itertools import chain

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from pyquery import PyQuery as pq

from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_budgetkey.common.cookie_monster import cookie_monster_get

parameters, datapackage, res_iter = ingest()

URL = "http://www.justice.gov.il/Units/Tmihot/Pages/TestServies.aspx?WPID=WPQ8&PN={0}"


def extract_url(href):
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
            rec = {
                "title": cells[0].text(),
                "paper_type": cells[1].text(),
                "office": cells[2].text(),
                "date": cells[3].text(),
                "pdf_url": extract_url(pq(cells[4].find('a')).attr('href'))
            }
            yield rec
        if len(rows) == 0:
            break
        pid += 1

resource = parameters['target-resource']
resource[PROP_STREAMING] = True
resource['schema'] = {
    'fields': [
        {
            'name': 'date',
            'type': 'date',
            'format': '%d-%m-%Y'
        },
        {
            'name': 'title',
            'type': 'string'
        },
        {
            'name': 'paper_type',
            'type': 'string'
        },
        {
            'name': 'office',
            'type': 'string'
        },
        {
            'name': 'pdf_url',
            'type': 'string',
            'format': 'uri'
        }
    ]
}
datapackage['resources'].append(resource)

spew(datapackage, chain(res_iter, [get_all_reports()]))