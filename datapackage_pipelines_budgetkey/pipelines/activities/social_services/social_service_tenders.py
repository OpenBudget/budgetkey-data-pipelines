import dataflows as DF

def unwind():
    def func(rows):
        for row in rows:
            if row.get('tenders'):
                for tender in row['tenders']:
                    key = tender['tender_key']
                    if key:
                        publication_id, tender_type, tender_id = key.split(':')
                        yield dict(
                            tender_id=tender_id,
                            tender_type=tender_type,
                            publication_id=publication_id,
                            tender_key = key,
                            soproc_tender=True,
                            soproc_id=row['id'],
                            soproc_name=row['name'],
                        )
    return func

def main(debug=False):
    BASE = '/var/datapackages/' if not debug else 'https://next.obudget.org/datapackages/'
    return DF.Flow(
        DF.load(BASE + 'activities/social_services/datapackage.json'),
        DF.add_field('tender_id', 'string'),
        DF.add_field('publication_id', 'string'),
        DF.add_field('tender_type', 'string'),
        DF.add_field('tender_key', 'string'),
        DF.add_field('soproc_tender', 'boolean'),
        DF.add_field('soproc_id', 'string'),
        DF.add_field('soproc_name', 'string'),
        unwind(),
        DF.select_fields(['tender_id', 'publication_id', 'tender_type', 'tender_key', 'soproc_tender', 'soproc_id', 'soproc_name']),
        DF.dump_to_path('/var/datapackages/activities/social_services_tenders'),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )

def flow(*_):
    return main()


if __name__ == '__main__':
    DF.Flow(
        main(debug=True),
        DF.printer()
    ).process()