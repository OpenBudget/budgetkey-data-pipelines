import dataflows as DF

def unwind():
    def func(rows):
        for row in rows:
            if row.get('suppliers'):
                for supplier in row['suppliers']:
                    if supplier['entity_id']:
                        yield dict(
                            entity_id=supplier['entity_id'],
                            services=row['id'],
                        )
    return func

def flow(parameters, *_):
    prod = not parameters.get('debug', False)
    source = '/var/datapackages/activities/social_services/datapackage.json' if prod else 'https://next.obudget.org/datapackages/activities/social_services/datapackage.json'
    return DF.Flow(
        DF.load(source),
        DF.add_field('entity_id', 'string'),
        DF.add_field('services', 'string'),
        unwind(),
        DF.update_resource(-1, name='soproc_suppliers'),
        DF.join_with_self('soproc_suppliers', ['entity_id'], dict(
            entity_id=None,
            services=dict(aggregate='set'),
        )),
        DF.add_field('soproc_supplier', 'boolean', True),
        DF.select_fields(['entity_id', 'soproc_supplier', 'services']),
        DF.dump_to_path('/var/datapackages/activities/social_services_suppliers'),
    )

if __name__=='__main__':
    parameters = dict(debug=True)
    DF.Flow(
        flow(parameters),
        DF.printer()
    ).process()