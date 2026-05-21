import dataflows as DF
import dataflows_airtable as DFA

  #  __airtable_id      מזהה המכרז                   שם השירות    שם המכרז                                                                                                  מספר הליך רכש    שם המשרד      מינהל / חטיבה    יחידה           FirstPrintotSCORE    SecondPrintotSCORE    ThirdPrintotSCORE    FourthPrintotSCORE    FifthPrintotSCORE    SixthPrintotSCORE

FIELD_MAP = {
    'מזהה המכרז': 'tender_key',
    'שם המכרז': 'tender_name',
    'שם השירות': 'service_name',
    'מספר הליך רכש': 'tender_id',
    'שם המשרד': 'office',
    'מינהל / חטיבה': 'unit',
    'יחידה': 'subunit',
    'FirstPrintotSCORE': 'principle_score_1',
    'FirstPrin1SCORE': 'principle_score_1_1',
    'FirstPrin2SCORE': 'principle_score_1_2',
    'FirstPrin3SCORE': 'principle_score_1_3',
    'FirstPrin4SCORE': 'principle_score_1_4',
    'SecondPrintotSCORE': 'principle_score_2',
    'SecondPrin1SCORE': 'principle_score_2_1',
    'SecondPrin2SCORE': 'principle_score_2_2',
    'ThirdPrintotSCORE': 'principle_score_3',
    'ThirdPrin1SCORE': 'principle_score_3_1',
    'ThirdPrin2SCORE': 'principle_score_3_2',
    'FourthPrintotSCORE': 'principle_score_4',
    'FifthPrintotSCORE': 'principle_score_5',
    'SixthPrintotSCORE': 'principle_score_6',
    'SixthPrin1SCORE': 'principle_score_6_1',
    'SixthPrin2SCORE': 'principle_score_6_2',
    'SixthPrin3SCORE': 'principle_score_6_3',
    'is_flag': 'is_flag',
}
all_fields = list(FIELD_MAP.values())

def flow(*_):
    return DF.Flow(
        DFA.load_from_airtable(base='appkFwqZCU6MFquJh', table='מכרז דגל', view='RESULTS'),
        DFA.load_from_airtable(base='appkFwqZCU6MFquJh', table='מכרז בסיס', view='RESULTS'),
        DF.add_field('is_flag', 'boolean', default=True, resources='מכרז דגל'),
        DF.add_field('is_flag', 'boolean', default=False, resources='מכרז בסיס'),
        DF.rename_fields(FIELD_MAP),
        DF.concatenate(dict((f, []) for f in all_fields), dict(name='tender_measurement', path='tender_measurement.csv')),
        DF.set_type('principle_score_.+', type='number', on_error=DF.schema_validator.clear),
        # DF.dump_to_path('tmp_social_services_tender_measurements'),
        DF.dump_to_path('var/datapackages/activities/social_services_tender_measurements'),
        DF.dump_to_sql(dict(
            soproc_measurement={'resource-name': 'tender_measurement'}
        )),
        DF.update_resource('tender_measurement', **{'dpp:streaming': True}),
    )


if __name__ == '__main__':
    DF.Flow(
        flow(),
        DF.printer()
    ).process()