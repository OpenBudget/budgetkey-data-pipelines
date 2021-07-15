import dataflows as DF

def unwind():
    def func(rows):
        for row in rows:
            if row.get('suppliers'):
                for supplier in row['suppliers']:
                    yield dict(
                        entity_id=supplier['entity_id'],
                        soproc_supplier=True,
                    )
    return func

def flow(*_):
    return DF.Flow(
        DF.load('/var/datapackages/activities/social_services/datapackage.json'),
        DF.add_field('entity_id', 'string'),
        DF.add_field('soproc_supplier', 'boolean'),
        unwind(),
        DF.select_fields(['entity_id', 'soproc_supplier']),
        DF.dump_to_path('/var/datapackages/activities/social_services_suppliers'),
    )
