import dataflows as DF
import decimal


SOURCES = [
    dict(
        year=2018,
        filename='ספר שירותים 2018 - סופי.xlsx',
        sheets=[
            dict(
                office='משרד הבריאות',
                options=dict(
                    sheet='בריאות',
                    headers=2
                )
            ),
            # dict(
            #     office='משרד הרווחה',
            #     options=dict(
            #         sheet='רווחה',
            #         headers=2
            #     )
            # ),
            dict(
                office='משרד החינוך',
                options=dict(
                    sheet='חינוך',
                    headers=2
                )
            ),
            dict(
                office='משרד העליה והקליטה',
                options=dict(
                    sheet='עליה וקליטה',
                    headers=2
                )
            ),
        ]
    ),
    dict(
        year=2017,
        filename='ספר שירותים חברתיים 2017 לעיצוב.xlsx',
        sheets=[
            dict(
                office='משרד הבריאות',
                options=dict(
                    sheet='בריאות 2017',
                    headers=1
                )
            ),
            # dict(
            #     office='משרד הרווחה',
            #     options=dict(
            #         sheet='רווחה 2017 ',
            #         headers=1
            #     )
            # ),
            dict(
                office='משרד החינוך',
                options=dict(
                    sheet='חינוך 2017',
                    headers=1
                )
            ),
            dict(
                office='משרד העליה והקליטה',
                options=dict(
                    sheet='עלייה וקליטה 2017',
                    headers=1
                )
            ),
        ]
    ),


    dict(
        year=2016,
        filename='מיפוי שירותים חברתים בריאות 2016.xlsx',
        sheets=[
            dict(
                office='משרד הבריאות',
                options=dict(
                    sheet='גיליון1',
                    headers=2
                )
            ),
        ]
    ),
    # dict(
    #     year=2016,
    #     filename='מיפוי שירותים חברתיים רווחה 2016.xlsx',
    #     sheets=[
    #         dict(
    #             office='משרד הרווחה',
    #             options=dict(
    #                 sheet='רווחה',
    #                 headers=1
    #             )
    #         ),
    #     ]
    # ),
    dict(
        year=2016,
        filename='מיפוי שירותים חברתיים חינוך 2016.xlsx',
        sheets=[
            dict(
                office='משרד החינוך',
                options=dict(
                    sheet='מטויב',
                    headers=2
                )
            ),
        ]
    ),

    dict(
        year=2016,
        filename='מיפוי שירותים חברתיים קליטה 2016.xlsx',
        sheets=[
            dict(
                office='משרד העליה והקליטה',
                options=dict(
                    sheet='קליטה פתוח (2)',
                    headers=1
                )
            ),
        ]
    ),
]


# %%
URL_PATTERN = 'https://docs.google.com/spreadsheets/d/{id}/edit#gid={gid}'


# %%
loads = []
i = 0
for source in SOURCES:
    for sheet in source['sheets']:
        i += 1
        resource_name = 'res_{}'.format(i)
        url = source['filename']
        loads.append((
            resource_name,
            DF.Flow(
                DF.load(url, name=resource_name, **sheet.get('options', {})),
                DF.add_field('year', 'integer', source['year']),
                DF.add_field('publisher_name', 'string', sheet['office']),
            )
        ))

FIELD_MAPPING = dict(
    year=[],
    publisher_name=[],
    unit=['מינהל/ חטיבה', 'מינהל/ אגף', 'שם מינהל האגף', 'מנהל / אגף', 'מינהל'],
    subunit=['אגף', 'אגף/ מחלקה', 'שם האגף / מחלקה', 'אגף / מחלקה'],
    subsubunit=['מחלקה'],
    activity_name=['שם השירות', 'שם השירות החברתי', 'שם השירות  חברתי', 'שירות חברתי'],
    activity_description=['תיאור השירות'],
    allocated_budget=['תקציב (אלש"ח)', 'תקציב 2018 אלפי ₪', 'תקציב 2017 אלפי ₪',
                      'היקף התקשרות שנתי (אש"ח)', 'תקציב (מלש"ח)', 'תקציב', 'תקציב 2016 אלפי ₪'],
    num_beneficiaries=['מספר מקבלי שירות', 'מספר מקבלי השרות', 'מספר מקבלי  שירות'],
)


def multiply_budget(row):
    if row['allocated_budget']:
        row['allocated_budget'] = int(row['allocated_budget']) * 1000


def fill_org_hierarchy(rows):
    prev = {}
    for row in rows:
        unit = row.get('unit') or ''
        activity_name = row['activity_name']
        if 'סה״כ' in unit or 'סה"כ' in unit or 'סה"כ תקציב' in activity_name:
            continue
        for k, v in prev.items():
            if not row.get(k):
                row[k] = v
        if row['subsubunit'] == row['subunit']:
            row['subsubunit'] = None
        if row['subunit'] == row['unit']:
            row['subunit'] = None
        yield row
        prev = dict(
            (k, v)
            for k, v in row.items()
            if k.endswith('unit')
        )


def fix_beneficiaries(row):
    try:
        row['num_beneficiaries'] = '{:,}'.format(row['num_beneficiaries'])
    except:
        pass


def prepare():
    for resource_name, load in loads:
        DF.Flow(
            load,
            # DF.printer(tablefmt='html'),
            DF.concatenate(FIELD_MAPPING, dict(name=resource_name, path=resource_name+'.csv')),
            DF.set_type('activity_name', type='string', constraints=dict(required=True), on_error=DF.schema_validator.drop),
            DF.set_type('allocated_budget', type='number', groupChar=',', bareNumber=False),
            DF.set_type('num_beneficiaries', type='number', groupChar=',', bareNumber=False,
                        on_error=DF.schema_validator.ignore),
            fix_beneficiaries,
            DF.set_type('num_beneficiaries', type='string'),
            multiply_budget,
            fill_org_hierarchy,
            # DF.printer(tablefmt='html'),
            DF.dump_to_path('tmp/' + resource_name),
        ).process()


def flow(*_):
    prepare()
    yearly_fields = ['year', 'unit', 'subunit', 'subsubunit', 'allocated_budget', 'num_beneficiaries']
    DF.Flow(
        *[
            DF.load('tmp/' + resource_name + '/datapackage.json')
            for resource_name, _ in loads
        ],
        DF.concatenate(FIELD_MAPPING, dict(name='social_services', path='social_services.csv')),
        DF.sort_rows('{year}', reverse=True),
        DF.add_field('history', 'object', lambda r: dict(
            (k, r[k] if not isinstance(r[k], decimal.Decimal) else int(r[k])) for k in yearly_fields
        )),
        DF.printer(),
        DF.join_with_self('social_services', ['publisher_name', 'activity_name'], dict(
            publisher_name=None,
            activity_name=None,
            activity_description=dict(aggregate='set'),
            min_year=dict(name='year', aggregate='min'),
            max_year=dict(name='year', aggregate='max'),
            history=dict(aggregate='array'),
        )),
        DF.add_field('kind', 'string', 'gov_social_service'),
        DF.add_field('kind_he', 'string', 'שירות חברתי'),
        DF.printer(),
        DF.dump_to_path('tmp/activities')
    ).process()
    return DF.Flow(
        DF.load('tmp/activities/datapackage.json'),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )



if __name__ == '__main__':
    DF.Flow(
        flow(),
        DF.dump_to_path('out'),
    ).process()
