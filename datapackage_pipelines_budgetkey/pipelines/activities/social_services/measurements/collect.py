import logging

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


# --- Answer coding ---------------------------------------------------------
# The spec ("מדדים לאיכות תהליך הרכש החברתי - מסמך אפיון למערכת ציינון ממוחשבת",
# מכון ERI, ספטמבר 2024, section "שלב א' - קידוד") defines a 0..4 scale:
#
#   לא מתקיים כלל = 0 | מועטה = 1 | בינונית = 2 | רבה = 3 | רבה מאוד = 4
#
# Airtable stores the answers as 1..5, so we shift on read. Scoring on the
# stored scale is not "the same thing rescaled" - it is a shift, and it puts an
# artificial floor under every published percentage:
#
#   score_stored = sum(w*(x+1)) / (5*sum(w)) = 0.8 * score_spec + 0.2
#
# A tender answering "לא מתקיים כלל" everywhere used to score 20%, and the 70%
# threshold was in practice bought at 62.5%.
ANSWER_OFFSET = 1  # subtract from the Airtable value to reach the spec scale
ANSWER_MIN = 0
ANSWER_MAX = 4

# An unanswered question is a data problem, not a low rating. When True, a
# sub-principle containing an unanswered question yields None (unknown) rather
# than a silently deflated score. Set to False only as a deliberate, temporary
# fallback - and note that it is NOT the previous behaviour either: the old code
# coerced a blank to 0 on the 1..5 scale, i.e. below the lowest valid answer.
STRICT_MISSING = True

# Whether "Q39 לא רלוונטי" drops Q39 from its sub-principle the way
# "Q52 לא רלוונטי" and "Q81 לא רלוונטי" do. The field exists in the base table
# and is ticked on a handful of records, but the spec does not define this
# branch, so behaviour is left unchanged pending a decision.
HANDLE_Q39_IRRELEVANT = False

_WARNED = set()


def is_irrelevant(val):
    """multipleSelects field: any selected value means the question is marked irrelevant."""
    if val is None:
        return False
    if isinstance(val, list):
        return len(val) > 0
    return bool(val)


def safe_div(num, denom):
    if num is None or not denom:
        return None
    return num / denom


def _warn_once(tender_key, field, message):
    key = (tender_key, field, message)
    if key not in _WARNED:
        _WARNED.add(key)
        logging.warning('tender %s: %s %s', tender_key, field, message)


def answer(row, field):
    """
    Read one answer and return it on the spec 0..4 scale, or None when it is
    missing or out of range.
    """
    value = row.get(field)
    tender_key = row.get('מזהה המכרז')
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        _warn_once(tender_key, field, 'has no numeric answer')
        return None if STRICT_MISSING else ANSWER_MIN
    coded = value - ANSWER_OFFSET
    if not ANSWER_MIN <= coded <= ANSWER_MAX:
        _warn_once(tender_key, field, 'is outside the 1..5 answer scale (got %r)' % (value,))
        return None
    return coded


def weighted(row, items):
    """
    Weighted total and maximum for one sub-principle.

    `items` is a list of (field, weight). The maximum is derived from the
    weights (sum(w) * ANSWER_MAX) and is never a stored constant - the bug this
    replaces was fourteen hardcoded maxima computed against a 5-point ceiling
    that had drifted away from the spec.

    Returns (None, None) if any answer is unknown, so the uncertainty
    propagates instead of being rounded down to zero.
    """
    total = 0.0
    maximum = 0.0
    for field, weight in items:
        value = answer(row, field)
        if value is None:
            return None, None
        total += weight * value
        maximum += weight * ANSWER_MAX
    return total, maximum


def combine(*parts):
    """Sum sub-principle (total, max) pairs into a principle-level pair."""
    if any(total is None for total, _ in parts):
        return None, None
    return sum(total for total, _ in parts), sum(maximum for _, maximum in parts)


# A sub-principle that is entirely "not relevant": contributes to neither the
# numerator nor the denominator, per the spec.
NOT_RELEVANT = (0.0, 0.0)


def compute_scores(row):
    """
    Compute all principle scores from raw Q-field inputs using the Base table formulas.
    Scores are 0-1 ratios (total / max), on the spec's 0..4 answer scale.
    """
    def irrel(name):
        return is_irrelevant(row.get(name))

    # Principle 1
    # FirstPrin1 = Q31*0.8 + Q32*1.2
    # FirstPrin2 = Q34*0.8 + Q35*1.2
    # FirstPrin3 = Q36
    # FirstPrin4 = IF(Q52_irrel, Q39, Q52+Q39)
    p1_1 = weighted(row, [('Q31', 0.8), ('Q32', 1.2)])
    p1_2 = weighted(row, [('Q34', 0.8), ('Q35', 1.2)])
    p1_3 = weighted(row, [('Q36', 1.0)])
    p1_4_items = [('Q39', 1.0)] if irrel('Q52 לא רלוונטי') else [('Q52', 1.0), ('Q39', 1.0)]
    if HANDLE_Q39_IRRELEVANT and irrel('Q39 לא רלוונטי'):
        p1_4_items = [item for item in p1_4_items if item[0] != 'Q39']
    p1_4 = weighted(row, p1_4_items) if p1_4_items else NOT_RELEVANT
    p1 = combine(p1_1, p1_2, p1_3, p1_4)

    # Principle 2
    # SecondPrin1 = Q61+Q62+Q63
    # SecondPrin2 = Q67+Q610
    p2_1 = weighted(row, [('Q61', 1.0), ('Q62', 1.0), ('Q63', 1.0)])
    p2_2 = weighted(row, [('Q67', 1.0), ('Q610', 1.0)])
    p2 = combine(p2_1, p2_2)

    # Principle 3
    # ThirdPrin1 = IF(Q81_irrel, dropped, Q81)
    # ThirdPrin2 = Q74
    p3_1 = NOT_RELEVANT if irrel('Q81 לא רלוונטי') else weighted(row, [('Q81', 1.0)])
    p3_2 = weighted(row, [('Q74', 1.0)])
    p3 = combine(p3_1, p3_2)

    # Principle 4
    # FourthPrin = Q91
    p4 = weighted(row, [('Q91', 1.0)])

    # Principle 5
    # FifthPrin = Q102*0.8 + Q103*1.2
    p5 = weighted(row, [('Q102', 0.8), ('Q103', 1.2)])

    # Principle 6
    # SixthPrin1 = Q111*0.8 + Q115*1.2
    # SixthPrin2 = Q117
    # SixthPrin3 = Q119
    p6_1 = weighted(row, [('Q111', 0.8), ('Q115', 1.2)])
    p6_2 = weighted(row, [('Q117', 1.0)])
    p6_3 = weighted(row, [('Q119', 1.0)])
    p6 = combine(p6_1, p6_2, p6_3)

    return {
        'principle_score_1':   safe_div(*p1),
        'principle_score_1_1': safe_div(*p1_1),
        'principle_score_1_2': safe_div(*p1_2),
        'principle_score_1_3': safe_div(*p1_3),
        'principle_score_1_4': safe_div(*p1_4),
        'principle_score_2':   safe_div(*p2),
        'principle_score_2_1': safe_div(*p2_1),
        'principle_score_2_2': safe_div(*p2_2),
        'principle_score_3':   safe_div(*p3),
        'principle_score_3_1': safe_div(*p3_1),
        'principle_score_3_2': safe_div(*p3_2),
        'principle_score_4':   safe_div(*p4),
        'principle_score_5':   safe_div(*p5),
        'principle_score_6':   safe_div(*p6),
        'principle_score_6_1': safe_div(*p6_1),
        'principle_score_6_2': safe_div(*p6_2),
        'principle_score_6_3': safe_div(*p6_3),
        # Core aspect scores are each principle's key question, on the same 0..4
        # scale as everything else. NOTE: these used to be emitted as the raw
        # 1..5 Airtable value; any consumer that maps them to labels must be
        # updated in the same release.
        'core_aspect_score_1': answer(row, 'Q35'),
        'core_aspect_score_2': answer(row, 'Q67'),
        'core_aspect_score_3': answer(row, 'Q74'),
        'core_aspect_score_4': answer(row, 'Q91'),
        'core_aspect_score_5': answer(row, 'Q103'),
        'core_aspect_score_6': answer(row, 'Q117'),
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
