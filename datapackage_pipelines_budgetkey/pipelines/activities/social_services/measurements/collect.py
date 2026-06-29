import dataflows as DF
import dataflows_airtable as DFA

IDENTITY_FIELDS = {
    'מזהה המכרז': 'tender_key',
    'שם המכרז': 'tender_name',
    'שם השירות': 'service_name',
    'מספר הליך רכש': 'tender_id',
    'שם המשרד': 'office',
    'מינהל / חטיבה': 'unit',
    'יחידה': 'subunit',
}

SCORE_FIELDS = [
    'principle_score_1', 'principle_score_1_1', 'principle_score_1_2',
    'principle_score_1_3', 'principle_score_1_4',
    'principle_score_2', 'principle_score_2_1', 'principle_score_2_2',
    'principle_score_3', 'principle_score_3_1', 'principle_score_3_2',
    'principle_score_4', 'principle_score_5',
    'principle_score_6', 'principle_score_6_1', 'principle_score_6_2', 'principle_score_6_3',
    'core_aspect_score_1', 'core_aspect_score_2', 'core_aspect_score_3',
    'core_aspect_score_4', 'core_aspect_score_5', 'core_aspect_score_6',
]

OUTPUT_FIELDS = list(IDENTITY_FIELDS.values()) + ['is_flag'] + SCORE_FIELDS


def is_irrelevant(val):
    """multipleSelects field: any selected value means the question is marked irrelevant."""
    if val is None:
        return False
    if isinstance(val, list):
        return len(val) > 0
    return bool(val)


def safe_div(num, denom):
    return None if not denom else num / denom


def compute_scores(row):
    """
    Compute all principle scores from raw Q-field inputs using the Base table formulas.
    Scores are 0–1 ratios (total / max).
    """
    def q(name):
        v = row.get(name)
        return v if isinstance(v, (int, float)) else 0

    def irrel(name):
        return is_irrelevant(row.get(name))

    # Principle 1
    # FirstPrin1 = Q31*0.8 + Q32*1.2  (MAX 10)
    # FirstPrin2 = Q34*0.8 + Q35*1.2  (MAX 10)
    # FirstPrin3 = Q36                 (MAX 5)
    # FirstPrin4 = IF(Q52_irrel, Q39, Q52+Q39)  (MAX 5 or 10)
    p1_1 = q('Q31') * 0.8 + q('Q32') * 1.2
    p1_2 = q('Q34') * 0.8 + q('Q35') * 1.2
    p1_3 = q('Q36')
    if irrel('Q52 לא רלוונטי'):
        p1_4, p1_4_max = q('Q39'), 5
    else:
        p1_4, p1_4_max = q('Q52') + q('Q39'), 10
    p1_max = 25 + p1_4_max

    # Principle 2
    # SecondPrin1 = Q61+Q62+Q63  (MAX 15)
    # SecondPrin2 = Q67+Q610     (MAX 10)
    p2_1 = q('Q61') + q('Q62') + q('Q63')
    p2_2 = q('Q67') + q('Q610')

    # Principle 3
    # ThirdPrin1 = IF(Q81_irrel, 0, Q81)  (MAX 0 or 5)
    # ThirdPrin2 = Q74                     (MAX 5)
    if irrel('Q81 לא רלוונטי'):
        p3_1, p3_1_max = 0, 0
    else:
        p3_1, p3_1_max = q('Q81'), 5
    p3_2 = q('Q74')
    p3_max = p3_1_max + 5

    # Principle 4
    # FourthPrin = Q91  (MAX 5)
    p4 = q('Q91')

    # Principle 5
    # FifthPrin = Q102*0.8 + Q103*1.2  (MAX 10)
    p5 = q('Q102') * 0.8 + q('Q103') * 1.2

    # Principle 6
    # SixthPrin1 = Q111*0.8 + Q115*1.2  (MAX 10)
    # SixthPrin2 = Q117                  (MAX 5)
    # SixthPrin3 = Q119                  (MAX 5)
    p6_1 = q('Q111') * 0.8 + q('Q115') * 1.2
    p6_2 = q('Q117')
    p6_3 = q('Q119')

    return {
        'principle_score_1':   safe_div(p1_1 + p1_2 + p1_3 + p1_4, p1_max),
        'principle_score_1_1': safe_div(p1_1, 10),
        'principle_score_1_2': safe_div(p1_2, 10),
        'principle_score_1_3': safe_div(p1_3, 5),
        'principle_score_1_4': safe_div(p1_4, p1_4_max),
        'principle_score_2':   safe_div(p2_1 + p2_2, 25),
        'principle_score_2_1': safe_div(p2_1, 15),
        'principle_score_2_2': safe_div(p2_2, 10),
        'principle_score_3':   safe_div(p3_1 + p3_2, p3_max),
        'principle_score_3_1': safe_div(p3_1, p3_1_max),
        'principle_score_3_2': safe_div(p3_2, 5),
        'principle_score_4':   safe_div(p4, 5),
        'principle_score_5':   safe_div(p5, 10),
        'principle_score_6':   safe_div(p6_1 + p6_2 + p6_3, 20),
        'principle_score_6_1': safe_div(p6_1, 10),
        'principle_score_6_2': safe_div(p6_2, 5),
        'principle_score_6_3': safe_div(p6_3, 5),
        # Core aspect scores are the raw Q values for each principle's key question
        'core_aspect_score_1': row.get('Q35'),
        'core_aspect_score_2': row.get('Q67'),
        'core_aspect_score_3': row.get('Q74'),
        'core_aspect_score_4': row.get('Q91'),
        'core_aspect_score_5': row.get('Q103'),
        'core_aspect_score_6': row.get('Q117'),
    }


def flow(*_):
    return DF.Flow(
        DFA.load_from_airtable(base='appkFwqZCU6MFquJh', table='מכרז דגל', view='RESULTS'),
        DFA.load_from_airtable(base='appkFwqZCU6MFquJh', table='מכרז בסיס', view='RESULTS'),
        DF.add_field('is_flag', 'boolean', default=True, resources='מכרז דגל'),
        DF.add_field('is_flag', 'boolean', default=False, resources='מכרז בסיס'),
        *[DF.add_field(f, 'number', lambda row, _f=f: compute_scores(row).get(_f))
          for f in SCORE_FIELDS],
        DF.rename_fields(IDENTITY_FIELDS),
        DF.concatenate(
            dict((f, []) for f in OUTPUT_FIELDS),
            dict(name='tender_measurement', path='tender_measurement.csv')
        ),
        DF.set_type('principle_score_.+', type='number', on_error=DF.schema_validator.clear),
        DF.set_type('core_aspect_score_.+', type='number', on_error=DF.schema_validator.clear),
        # DF.dump_to_path('tmp_social_services_tender_measurements'),
        DF.dump_to_path('/var/datapackages/activities/social_services_tender_measurements'),
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
