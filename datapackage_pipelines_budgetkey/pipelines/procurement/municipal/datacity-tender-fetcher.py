import dataflows as DF
from hashlib import md5


DATACITY_DB = 'postgresql://readonly:readonly@db.datacity.org.il:5432/datasets'

MAPPING = {
    'process-code': 'regulation',
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
    tender_id = row.get('regulation') or ''
    publisher = row.get('publisher') or ''
    description = row.get('description') or ''
    params = [tender_id, description, publisher]
    return md5(''.join(params).encode('utf-8')).hexdigest()[:8]

def flow(*_):
    QUERY = 'select * from muni_procurement'
    return DF.Flow(
        DF.load(DATACITY_DB, query=QUERY, name='muni_tenders'),
        *[
            DF.set_type(k, type='string')
            for k in MAPPING.keys()
            if not k.endswith('-date')
        ],
        DF.set_type('.*date', type='date'),

        DF.rename_fields(MAPPING),
        DF.select_fields(MAPPING.values()),

        DF.add_field('page_title', 'string', lambda r: 'מכרז של {publisher}: {description}'.format(**r)),
        DF.add_field('tender_id', 'string', default=calc_doc_id),
        DF.add_field('tender_type', 'string', 'office'),
        DF.add_field('tender_type_he', 'string', 'מכרז מוניציפלי'),
        DF.add_field('publication_id', 'string', '0'),

        DF.add_field('score', 'number', 10),

        DF.set_primary_key(['tender_id', 'tender_type', 'publication_id']),

        DF.update_resource(-1, **{'dpp:streaming': True}),
        DF.dump_to_path('/var/datapackages/procurement/municipal/datacity-tenders'),
        DF.dump_to_sql(dict(
            muni_tenders={
                'resource-name': 'muni_tenders',
            }
        ))
    )

if __name__ == '__main__':
    DF.Flow(
        flow(),
        DF.printer()
    ).process()