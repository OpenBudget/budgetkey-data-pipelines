import dataflows as DF

URL = 'https://storage.googleapis.com/mapathataf.firebasestorage.app/exports/facilities.csv'

# The export has a wide, evolving set of columns (e.g. official_moe, official_moe2, ...),
# so everything is loaded as a string and only well-known column families are cast.
# owner_* columns are free text entered by facility owners and are kept as strings.
NUMBER_FIELDS = r'.+_(lat|lng|coord_x|coord_y|from_age|to_age)'
INTEGER_FIELDS = r'.+_(total_places|available_places)_.+|.+_capacity'
BOOLEAN_FIELDS = r'.+_(licensing_not_needed|subsidized|app_publication|admission_committee)'
DATETIME_FIELDS = r'.+_updated_at'


def scrape(url=URL):
    return DF.Flow(
        DF.load(url, name='mapathataf-export', format='csv', infer_strategy=DF.load.INFER_STRINGS),
        DF.set_type(NUMBER_FIELDS, type='number', on_error=DF.schema_validator.clear),
        DF.set_type(INTEGER_FIELDS, type='integer', on_error=DF.schema_validator.clear),
        DF.set_type(BOOLEAN_FIELDS, type='boolean', on_error=DF.schema_validator.clear),
        DF.set_type(DATETIME_FIELDS, type='datetime', format='any', on_error=DF.schema_validator.clear),
        DF.set_primary_key(['workspace', 'id']),
        DF.update_resource(-1, name='mapathataf-export', path='mapathataf-export.csv'),
    )


def flow(*_):
    return DF.Flow(
        scrape(),
        DF.dump_to_path('/var/datapackages/facilities/mapathataf-export'),
        DF.dump_to_sql(dict(
            mapathataf_export={'resource-name': 'mapathataf-export'}
        )),
        DF.update_resource(-1, **{'dpp:streaming': True}),
    )


if __name__ == '__main__':
    DF.Flow(
        scrape(),
        DF.printer()
    ).process()
