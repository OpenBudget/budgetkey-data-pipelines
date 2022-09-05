import datetime
import dataflows as DF

def unwind():
    def func(rows):
        for row in rows:
            if row.get('suppliers'):
                for supplier in row['suppliers']:
                    if supplier['entity_id']:
                        yield dict(
                            entity_id=supplier['entity_id'],
                            entity_kind=supplier['entity_kind'],
                            services=row['id'],
                            updated_at=row['updated_at'],
                        )
    return func

def flow(parameters, *_):
    prod = not parameters.get('debug', False)
    source = '/var/datapackages/activities/social_services/datapackage.json' if prod else 'https://next.obudget.org/datapackages/activities/social_services/datapackage.json'
    now = datetime.datetime.now()
    return DF.Flow(
        DF.load(source),
        DF.add_field('entity_id', 'string'),
        DF.add_field('entity_kind', 'string'),
        DF.add_field('services', 'string'),
        unwind(),
        DF.update_resource(-1, name='soproc_suppliers'),
        DF.join_with_self('soproc_suppliers', ['entity_id'], dict(
            entity_id=None,
            entity_kind=None,
            services=dict(aggregate='set'),
            updated_at=dict(aggregate='max')
        )),
        DF.add_field('soproc_supplier', 'boolean', True),
        DF.select_fields(['entity_id', 'soproc_supplier', 'services', 'updated_at', 'entity_kind']),
        
        DF.duplicate('soproc_suppliers', 'new_soproc_suppliers'),
        DF.update_resource(-1, path='new_soproc_suppliers.csv'),
        DF.filter_rows(lambda row: (now - row['updated_at']).days < 14, resources='new_soproc_suppliers'),

        DF.dump_to_path('/var/datapackages/activities/social_services_suppliers', format='json'),

        DF.delete_resource('new_soproc_suppliers'),
        DF.dump_to_path('/var/datapackages/activities/social_services_suppliers'),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )

if __name__=='__main__':
    parameters = dict(debug=True)
    DF.Flow(
        flow(parameters),
        DF.printer()
    ).process()