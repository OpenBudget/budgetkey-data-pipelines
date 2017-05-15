from itertools import chain

import requests
import logging
from pyquery import PyQuery as pq

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()

URL = "http://www.justice.gov.il/Units/Tmihot/Pages/TestServies.aspx?WPID=WPQ8&PN={0}"


def get_all_reports():
    num_of_page = 37
    session = requests.session()
    for pid in range(1, num_of_page):
        response = session.get(URL.format(pid), verify=False).text
        response = pq(response)
        rows = response.find('#cqwpGridViewTable .zbTRBlue, #cqwpGridViewTable .Trtblclss')
        rows = list(rows)
        print (len(rows))
        for r in rows:
            row = pq(r)
            cells = [pq(td) for td in row.find('td')]
            rec = {
                "title": cells[0].text(),
                "paper_type": cells[1].text(),
                "office": cells[2].text(),
                "date": cells[3].text(),
                "pdf-url": pq(cells[4].find('a')).attr('href').split("'")[-2]
            }
            logging.info(rec)
            yield rec


resource = parameters['target-resource']
resource['schema'] = {
    'fields': [
        {
            'name': 'date',
            'type': 'date',
            'format': 'fmt:%d-%m-%Y'
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
            'name': 'pdf-url',
            'type': 'string',
            'format': 'uri'
        }
    ]
}
datapackage['resources'].append(resource)

logging.info(datapackage)
spew(datapackage, chain(res_iter, [get_all_reports()]))