import dataflows as DF
from hashlib import md5


DATACITY_DB = 'postgresql://readonly:readonly@db.datacity.org.il:5432/datasets'

MAPPING = {
    'process-code': 'tender_id',
    'process-title': 'description',
    'municipality-name': 'publisher',
    'process-procurer-unit-name': 'publisher_unit',

    'process-publication-date': 'publication_date',
    'process-last-update-date': 'last_update_date',
    'process-closing-date': 'claim_date',

    'process-source-link': 'page_url',
    'process-status': 'status',
    'process-decision': 'decision',
}

def calc_doc_id(row):
    params = [row.get(x) or '' for x in ('tender_id', 'description', 'publisher')]
    return md5(''.join(params).encode('utf-8')).hexdigest()[:8]

def flow(*_):
    QUERY = 'select * from muni_procurement'
    return DF.Flow(
        DF.load(DATACITY_DB, query=QUERY, name='muni_tenders'),
        DF.set_type('.*date', type='date'),

        DF.rename_fields(MAPPING),
        DF.select_fields(MAPPING.values()),

        DF.add_field('page_title', 'string', lambda r: 'מכרז של {publisher}: {description}'.format(**r)),

        DF.set_type('tender_id', transform=lambda v, row: calc_doc_id(row)),
        DF.add_field('tender_type', 'string', 'office'),
        DF.add_field('tender_type_he', 'string', 'מכרז מוניציפלי'),
        DF.add_field('publication_id', 'string', '0'),

        DF.add_field('score', 'number', 10),
        DF.update_resource(-1, **{'dpp:streaming': True}),
        DF.dump_to_path('/var/datapackages/procurement/municipal/datacity-tenders'),
        # DF.dump_to_sql(dict(
        #     muni_tenders={
        #         'resource-name': 'muni_procurement',
        #     }
        # ))
    )

if __name__ == '__main__':
    DF.Flow(
        flow(),
        DF.printer()
    ).process()