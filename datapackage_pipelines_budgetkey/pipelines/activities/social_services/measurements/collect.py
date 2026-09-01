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

# Per-principle count of criteria answered "לא מתקיים כלל" or "מתקיים במידה
# מועטה". Not a score - it exists so the consumer can apply the spec's cap on
# the top category. See low_answers().
COUNT_FIELDS = ['low_answer_count_%d' % i for i in range(1, 7)]

OUTPUT_FIELDS = list(IDENTITY_FIELDS.values()) + ['is_flag'] + SCORE_FIELDS + COUNT_FIELDS


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

# The three base questions offering "לא רלוונטי", per נספח ב' of the spec,
# which marks the answer scale of each: Q52, Q39 and Q81. Coding rule שלב א':
# 'לא רלוונטי' יקודד כערך חסר (NA), and NA answers are excluded from both the
# numerator and the maximum (היגד שסומן כ"לא רלוונטי" לא יחושב כחלק מציון
# איכות הרכש למכרז הנדון).
#
# Section ב'/2 spells out the drop for Q52 and Q81 only; Q39 is missing there
# purely because FirstPrin_4MAX was written with one conditional. Leaving Q39
# unhandled did not merely inflate its denominator - a Q39 marked NA is stored
# blank, so the whole of principle 1 came out unknown.
IRRELEVANT_FLAGS = {
    'Q52': 'Q52 לא רלוונטי',
    'Q39': 'Q39 לא רלוונטי',
    'Q81': 'Q81 לא רלוונטי',
}

# 'לא מתקיים כלל' (0) and 'מתקיים במידה מועטה' (1).
LOW_ANSWER_MAX = 1

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


def low_answers(row, *item_lists):
    """
    How many of one principle's criteria were answered 0 or 1.

    The spec's categorical conversion caps a principle that has more than one
    such answer at the middle category, however high its percentage:

        כאשר בכל מקרה בו יותר מקריטריון אחד בעקרון קיבל את הציון אפס או אחד
        (לא מתקיים או מתקיים במידה מועטה) - עקרון זה לא יוכל לקבל את הדרגה
        המקסימלית ("מתקיים") וגם אם יגיע ל-70% מהציון - יסווג כ"מתקיים במידה
        בינונית".

    The rule needs the individual answers, which do not survive into the
    published table, so the count is emitted alongside the score. Questions
    marked "לא רלוונטי" are already absent from `item_lists` and so cannot
    count against the tender.

    Returns None when any answer in the principle is unknown - the count would
    be a lower bound, and a lower bound must not be used to cap a score.
    """
    count = 0
    for items in item_lists:
        for field, _ in items:
            value = answer(row, field)
            if value is None:
                return None
            if value <= LOW_ANSWER_MAX:
                count += 1
    return count


def compute_scores(row):
    """
    Compute all principle scores from raw Q-field inputs using the Base table formulas.
    Scores are 0-1 ratios (total / max), on the spec's 0..4 answer scale.
    """
    def relevant(*items):
        """Drop the questions this respondent marked 'לא רלוונטי'."""
        return [(field, weight) for field, weight in items
                if not is_irrelevant(row.get(IRRELEVANT_FLAGS.get(field)))]

    # Principle 1
    # FirstPrin1 = Q31*0.8 + Q32*1.2
    # FirstPrin2 = Q34*0.8 + Q35*1.2
    # FirstPrin3 = Q36
    # FirstPrin4 = Q52 + Q39, less whichever of the two is marked NA
    p1_1_items = relevant(('Q31', 0.8), ('Q32', 1.2))
    p1_2_items = relevant(('Q34', 0.8), ('Q35', 1.2))
    p1_3_items = relevant(('Q36', 1.0))
    p1_4_items = relevant(('Q52', 1.0), ('Q39', 1.0))

    # Principle 2
    # SecondPrin1 = Q61+Q62+Q63
    # SecondPrin2 = Q67+Q610
    p2_1_items = relevant(('Q61', 1.0), ('Q62', 1.0), ('Q63', 1.0))
    p2_2_items = relevant(('Q67', 1.0), ('Q610', 1.0))

    # Principle 3
    # ThirdPrin1 = Q81 (ThirdPrin1MAX = NA when Q81 is marked NA, i.e. the
    # sub-principle has no score at all and principle 3 is Q74 alone)
    # ThirdPrin2 = Q74
    p3_1_items = relevant(('Q81', 1.0))
    p3_2_items = relevant(('Q74', 1.0))

    # Principle 4
    # FourthPrin = Q91
    p4_items = relevant(('Q91', 1.0))

    # Principle 5
    # FifthPrin = Q102*0.8 + Q103*1.2
    p5_items = relevant(('Q102', 0.8), ('Q103', 1.2))

    # Principle 6
    # SixthPrin1 = Q111*0.8 + Q115*1.2
    # SixthPrin2 = Q117
    # SixthPrin3 = Q119
    p6_1_items = relevant(('Q111', 0.8), ('Q115', 1.2))
    p6_2_items = relevant(('Q117', 1.0))
    p6_3_items = relevant(('Q119', 1.0))

    def score(items):
        return weighted(row, items) if items else NOT_RELEVANT

    p1_1, p1_2, p1_3, p1_4 = (score(i) for i in
                              (p1_1_items, p1_2_items, p1_3_items, p1_4_items))
    p2_1, p2_2 = score(p2_1_items), score(p2_2_items)
    p3_1, p3_2 = score(p3_1_items), score(p3_2_items)
    p4 = score(p4_items)
    p5 = score(p5_items)
    p6_1, p6_2, p6_3 = score(p6_1_items), score(p6_2_items), score(p6_3_items)

    p1 = combine(p1_1, p1_2, p1_3, p1_4)
    p2 = combine(p2_1, p2_2)
    p3 = combine(p3_1, p3_2)
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
        # Not scores: the input the consumer needs to cap a principle at the
        # middle category. See low_answers().
        'low_answer_count_1': low_answers(row, p1_1_items, p1_2_items, p1_3_items, p1_4_items),
        'low_answer_count_2': low_answers(row, p2_1_items, p2_2_items),
        'low_answer_count_3': low_answers(row, p3_1_items, p3_2_items),
        'low_answer_count_4': low_answers(row, p4_items),
        'low_answer_count_5': low_answers(row, p5_items),
        'low_answer_count_6': low_answers(row, p6_1_items, p6_2_items, p6_3_items),
    }


def flow(*_):
    return DF.Flow(
        DFA.load_from_airtable(base='appkFwqZCU6MFquJh', table='מכרז דגל', view='RESULTS'),
        DFA.load_from_airtable(base='appkFwqZCU6MFquJh', table='מכרז בסיס', view='RESULTS'),
        DF.add_field('is_flag', 'boolean', default=True, resources='מכרז דגל'),
        DF.add_field('is_flag', 'boolean', default=False, resources='מכרז בסיס'),
        *[DF.add_field(f, 'number', lambda row, _f=f: compute_scores(row).get(_f))
          for f in SCORE_FIELDS],
        *[DF.add_field(f, 'integer', lambda row, _f=f: compute_scores(row).get(_f))
          for f in COUNT_FIELDS],
        DF.rename_fields(IDENTITY_FIELDS),
        DF.concatenate(
            dict((f, []) for f in OUTPUT_FIELDS),
            dict(name='tender_measurement', path='tender_measurement.csv')
        ),
        DF.set_type('principle_score_.+', type='number', on_error=DF.schema_validator.clear),
        DF.set_type('core_aspect_score_.+', type='number', on_error=DF.schema_validator.clear),
        DF.set_type('low_answer_count_.+', type='integer', on_error=DF.schema_validator.clear),
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
