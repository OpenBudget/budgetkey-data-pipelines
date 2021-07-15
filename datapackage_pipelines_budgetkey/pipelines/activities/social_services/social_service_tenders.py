import dataflows as DF

def unwind():
    def func(rows):
        for row in rows:
            if row.get('tenders'):
                for tender in row['tenders']:
                    key = tender['tender_key']
                    publication_id, tender_type, tender_id = key.split(':')
                    yield dict(
                        tender_id=tender_id,
                        tender_type=tender_type,
                        publication_id=publication_id,
                        tender_key = key,
                        soproc_tender=True,
                    )
    return func

def flow(*_):
    return DF.Flow(
        DF.load('/var/datapackages/activities/social_services/datapackage.json'),
        DF.add_field('tender_id', 'string'),
        DF.add_field('publication_id', 'string'),
        DF.add_field('tender_type', 'string'),
        DF.add_field('tender_key', 'string'),
        DF.add_field('soproc_tender', 'boolean'),
        unwind(),
        DF.select_fields(['tender_id', 'publication_id', 'tender_type', 'soproc_supplier']),
        DF.dump_to_path('/var/datapackages/activities/social_services_tenders'),
    )
