# from itertools import chain

import requests
import json

from pyquery import PyQuery as pq

import dataflows as DF

def get_offices():
    url='https://www.gov.il/he/Departments/DynamicCollectors/repository-of-answers'
    text=requests.get(url).text
    page = pq(text)
    el=page.find('form')[0]
    cfg = el.attrib['ng-init']
    cfg = json.loads(cfg.split("','',10,'',")[1].split(",'MultiAutoComplete')")[0])
    cfg = cfg[1]
    assert cfg['Name'] == 'Office'
    cfg = cfg['MultiChoiseValues']['Values']
    return dict(
        (c['Key'], c['Value'])
        for c in cfg
    )


def get_all():
    total = 100000
    skip = 0
    offices = get_offices()
    while skip < total:
        payload = dict(
            DynamicTemplateID='8132e331-eb58-474d-abe2-085d3f08c400',
            QueryFilters=dict(
                skip=dict(
                    Query=skip
                ),
                Info=dict(Query='1')
            ),
            From=skip
        )
        resp = requests.post('https://www.gov.il/he/api/DynamicCollector', json=payload).json()
        results = resp['Results']
        for r in results:
            base = r['UrlName']
            r = r['Data']
            if 'File' not in r:
                continue
            for f in r.pop('File'):
                f.update(r)
                if f['FileName'].endswith('pdf'):
                    continue
                if f['FileName'].endswith('docx'):
                    continue
                yield {
                    'report-url': f'https://www.gov.il/BlobFolder/dynamiccollectorresultitem/{base}/he/{f["FileName"]}',
                    'report-title': f['Title'],
                    'report-publisher': offices[f['Office'][0]],
                    'report-date': f.get('Date')
                }
        skip += len(results)
        total = resp['TotalResults']


def flow(parameters, *_):
    return DF.Flow(
        get_all(),
        DF.set_type('report-date', type='date', format='%Y-%m-%dT%H:%M:%SZ'),
        DF.update_resource(-1, **parameters['target-resource']),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )


if __name__ == '__main__':
    DF.Flow(
        flow({'target-resource': {'name': 'boop'}}),
        DF.printer()
    ).process()
