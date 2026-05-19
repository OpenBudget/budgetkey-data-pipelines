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
    'FirstPrintotSCORE': 'first_principle_score',
    'SecondPrintotSCORE': 'second_print_score',
    'ThirdPrintotSCORE': 'third_print_score',
    'FourthPrintotSCORE': 'fourth_print_score',
    'FifthPrintotSCORE': 'fifth_print_score',
    'SixthPrintotSCORE': 'sixth_print_score',
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