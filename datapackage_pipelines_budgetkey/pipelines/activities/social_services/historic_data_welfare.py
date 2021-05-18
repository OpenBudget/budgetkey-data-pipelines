import os
import requests
from fuzzywuzzy import process
import dataflows as DF

JWT=os.environ['JWT']
resp = requests.get('https://data-input.obudget.org/auth/authorize?service=etl-server&jwt=' + JWT).json()
HEADERS = {
    'X-Auth': resp['token']
}
session = requests.Session()
session.headers = HEADERS

def datarecords(kind):
    records = session.get('https://data-input.obudget.org/api/datarecords/' + kind).json()['result']
    # print(records)
    return dict((x['name'], x) for x in map(lambda y: y['value'], records))

options = dict()
for k in ['target_audience', 'subject', 'intervention']:
    options[k] = datarecords(k)

def best_match(name, id, field='id'):
    options_ = options[id]
    best = process.extractOne(name, list(options_.keys()), score_cutoff=80)
    assert best is not None, 'Failed to find match for {}:{}'.format(id, name)
    return options_[best[0]][field]

def clean(row, f):
    if row.get(f):
        return row[f].strip()
    return None

filename = 'קטלוג רווחה למערכת ההזנה 18.5.21.xlsx'


FIELDS = [
    'activity_name',
    'activity_description',
    'history',
    'target_audience',
    'subject',
    'intervention'
]


def splitter(f, kind):
    def func(r):
        if r[f]:
            names = r[f].split('|')
            return [best_match(name, kind) for name in names]
        return []
    return func


def flow(*_):
    DF.Flow(
        DF.load(filename, name='welfare'),
        DF.add_field('activity_name', 'string', lambda r: r['שם השירות (ציבורי)']),
        DF.filter_rows(lambda r: r['activity_name']),
        DF.add_field('activity_description', 'array', lambda r: [r['תיאור השירות (תיאור קצר)'] + '\n' + r['השירות (מטרת השירות)']]),
        DF.add_field('history', 'array', lambda r: [
        dict(
            year=2019, 
            unit=clean(r, 'מינהל - שיוך ארגוני 1'),
            subunit=clean(r, 'אגף - שיוך ארגוני 2'),
            subsubunit=clean(r, 'יחידה - שיוך ארגוני 3'),
        ) 
        ]),
        DF.add_field('target_audience', 'array', splitter('אוכלוסייה', 'target_audience')),
        DF.add_field('subject', 'array', splitter('תחום ההתערבות', 'subject')),
        DF.add_field('intervention', 'array', splitter('אופן התערבות', 'intervention')),
        DF.select_fields(FIELDS),
        DF.add_field('publisher_name', 'string', 'משרד הרווחה'),
        DF.add_field('min_year', 'integer', 2019),
        DF.add_field('max_year', 'integer', 2019),
        DF.add_field('kind', 'string', 'gov_social_service'),
        DF.add_field('kind_he', 'string', 'שירות חברתי'),
        DF.printer(),
        DF.validate(),
        DF.dump_to_path('tmp/activities-welfare')
    ).process()
    return DF.Flow(
        DF.load('tmp/activities-welfare/datapackage.json'),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )



if __name__ == '__main__':
    DF.Flow(
        flow(),
        DF.dump_to_path('out'),
    ).process()
