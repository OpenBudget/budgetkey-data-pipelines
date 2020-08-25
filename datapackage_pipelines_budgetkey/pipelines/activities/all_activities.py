import dataflows as DF


def flow(*_):
    return DF.Flow(
        DF.load('/var/datapackages/activities/social_services/historic_data/datapackage.json'),
        DF.concatenate(
            dict(
                kind=[], kind_he=[],
                activity_name=[], activity_description=[],
                publisher_name=[], history=[],
                max_year=[], min_year=[]
            ),
            dict(
                name='activities', path='activities.csv'
            )
        ),
        DF.set_primary_key(['kind', 'publisher_name', 'activity_name']),
        DF.set_type('activity_name', **{'es:title': True}),
        DF.set_type('activity_description', **{'es:itemType': 'string', 'es:boost': True}),
        DF.set_type('kind', **{'es:keyword': True, 'es:exclude': True}),
        DF.set_type('kind_he', **{'es:keyword': True, 'es:exclude': True}),
        DF.set_type('publisher_name', **{'es:keyword': True}),
        DF.set_type('history', **{
            'es:itemType': 'object',
            'es:schema': dict(
                fields=[
                    dict(name='year', type='integer'),
                    dict(name='unit', type='string'),
                    dict(name='subunit', type='string'),
                    dict(name='subsubunit', type='string'),
                    dict(name='allocated_budget', type='integer'),
                    dict(name='num_beneficiaries', type='string', **{'es:index': False}),
                ]
            )
        }),
        DF.add_field('score', 'number', 1, **{'es:score-column': True}),
        DF.update_resource(-1, **{'dpp:streaming': True}),
        DF.dump_to_path('/var/datapackages/activities/all'),
        DF.dump_to_sql(dict(
            activities={'resource-name': 'activities'}
        ))
    )


if __name__ == '__main__':
    flow().process()
