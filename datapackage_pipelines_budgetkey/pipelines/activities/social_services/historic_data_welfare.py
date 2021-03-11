import dataflows as DF

filename = 'קטלוג לרווחה להעלאה למערכת הזנה.xlsx'


FIELDS = [
    'activity_name',
    'activity_description',
    'history',
    'target_audience',
    'subject',
    'intervention'
]


def splitter(f):
    def func(r):
        if r[f]:
            return r[f].split('|')
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
            unit=r['יחידה ארגונית נותנת השירות'].split('/')[0].strip(),
            subunit=r['יחידה ארגונית נותנת השירות'].split('/')[1].strip(),
            subsubunit=r['יחידה ארגונית נותנת השירות'].split('/')[1].strip(),
        ) 
        ]),
        DF.add_field('target_audience', 'array', splitter('אוכלוסייה')),
        DF.add_field('subject', 'array', splitter('תחום ההתערבות')),
        DF.add_field('intervention', 'array', splitter('אופן התערבות')),
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
