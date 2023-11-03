# from itertools import chain

from datapackage_pipelines_budgetkey.common.cookie_monster import cookie_monster_get
import requests
import json
import time

from pyquery import PyQuery as pq

import dataflows as DF

def get_offices():
    headers = {
        'User-Agent': 'kz-data-reader'
    }
    url='https://www.gov.il/he/Departments/DynamicCollectors/repository-of-answers'
    text = requests.get(url, headers=headers).text
    # text=requests.get(url, headers=headers).text
    page = pq(text)
    forms = page.find('div[name=form]')
    if len(forms) == 0:
        print('PAGE:', text[:1000])
        raise Exception('No forms found')
    el = forms[0]
    cfg = el.attrib['ng-init']
    cfg = cfg.split('OfficeMultiChoiseValues": ')[1]
    cfg = cfg.split(']')[0]
    cfg += ']}'
    cfg = json.loads(cfg)
    cfg = cfg['Values']
    # assert cfg['Name'] == 'Office'
    # cfg = cfg['MultiChoiseValues']['Values']
    print(f'GOT {len(cfg)} offices')
    return dict(
        (c['Key'], c['Value'])
        for c in cfg
    )


def get_all():
    total = 100000
    skip = 0
    retries = 5
    while True:
        try:
            print('Getting offices...')
            offices = get_offices()
            break
        except Exception as e:
            print('Failed to get offices...', e)
            time.sleep(180)
            retries -= 1
        assert retries > 0
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
        for i in range(5):
            try:
                resp = requests.post('https://www.gov.il/he/api/DynamicCollector', json=payload).json()
                break
            except Exception as e:
                print('Failed to get', str(e), payload)
                time.sleep(60)
        results = resp['Results']
        for r in results:
            base = r['UrlName']
            r = r['Data']
            if 'File' not in r:
                continue
            for f in r.pop('File'):
                f.update(r)
                if 'xls' not in f['FileName'].lower():
                    continue
                if 'Title' in f and f['Title']:
                    title = f['Title']
                    if 'Office' in f:
                        office = offices[f['Office'][0]]
                    else:
                        office = title.split('-')[-1].strip()
                    yield {
                        'report-url': f'https://www.gov.il/BlobFolder/dynamiccollectorresultitem/{base}/he/{f["FileName"]}',
                        'report-title': title,
                        'report-publisher': office,
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
