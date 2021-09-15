import os
import json
from dataflows.processors.select_fields import select_fields
import requests
import dataflows as DF


JWT=os.environ['DATA_INPUT_JWT']
resp = requests.get('https://data-input.obudget.org/auth/authorize?service=etl-server&jwt=' + JWT).json()
HEADERS = {
    'X-Auth': resp['token']
}
session = requests.Session()
session.headers = HEADERS

def services():
    records = session.get('https://data-input.obudget.org/api/datarecords/social_service').json()['result']
    return records


def update_cat_number():
    def func(rows):
        filename = 'קטלוג רווחה למערכת ההזנה 18.5.21.xlsx'
        cats = DF.Flow(
            DF.load(filename, name='welfare'),
            # DF.printer(),
            DF.rename_fields({
                'id': 'catalog_number',
                'שם השירות (ציבורי)': 'name'
            }, regex=False),
            DF.select_fields(['catalog_number', 'name']),
        ).results()[0][0]
        cats = dict((k.pop('name'), k) for k in cats)

        missing = []
        for row in rows:
            v = row['value']
            if v['office'] == 'משרד הרווחה':
                name = v['name']
                if name in cats:
                    rec = cats.pop(name)
                    cn = str(rec['catalog_number'])
                    if v.get('catalog_number') != cn:
                        v['catalog_number'] = cn
                        yield row
                else:
                    missing.append((name, v['id']))
        for x in missing:
            print('{} (https://data-input.obudget.org/he/datarecords/social_service/{})'.format(*x))
        # print('MISSING', len(missing), missing)
        # cats = list(cats.items())
        # print('CATS', len(cats), cats)

    return func

def update_from_file():
    filename = 'welfare_import_2/updates.json'
    updates = json.load(open(filename))
    print(len(updates))
    saved = []
    def func(rows):
        count = 0
        for row in rows:
            value = row['value']
            cn = value.get('catalog_number')
            if cn:
                cn = str(cn)
                if cn in updates:
                    update = updates[cn]
                    print('updating', count, cn) 
                    count += 1
                    saved.append(row)
                    value.update(update)
                    yield row
        json.dump(saved, open('saved.json', 'w'))
    return func


def save():
    def func(row):
        v = row['value']
        resp = session.post('https://data-input.obudget.org/api/datarecord/social_service', json=v)
        print('save', v['id'], resp)
    return func


def flow():
    return DF.Flow(
        services(),
        update_from_file(),
        # update_cat_number(),
        save()
    )


if __name__ == '__main__':
    DF.Flow(
        flow(),
        DF.printer(),
    ).process()
