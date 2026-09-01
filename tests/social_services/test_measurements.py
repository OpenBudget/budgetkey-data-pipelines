"""
Tests for the social services tender measurement scoring.

The scoring must follow "מדדים לאיכות תהליך הרכש החברתי - מסמך אפיון למערכת
ציינון ממוחשבת", מכון ERI, ספטמבר 2024. The invariant that matters most is the
one the previous implementation broke: the scale starts at zero.
"""
import pytest

from datapackage_pipelines_budgetkey.pipelines.activities.social_services.measurements.collect import (
    ANSWER_MAX,
    ANSWER_MIN,
    COUNT_FIELDS,
    SCORE_FIELDS,
    answer,
    combine,
    compute_scores,
    is_irrelevant,
    safe_div,
    weighted,
)

ALL_QUESTIONS = [
    'Q31', 'Q32', 'Q34', 'Q35', 'Q36', 'Q39', 'Q52',
    'Q61', 'Q62', 'Q63', 'Q67', 'Q610',
    'Q74', 'Q81', 'Q91',
    'Q102', 'Q103',
    'Q111', 'Q115', 'Q117', 'Q119',
]

PRINCIPLE_SCORES = [
    'principle_score_1', 'principle_score_2', 'principle_score_3',
    'principle_score_4', 'principle_score_5', 'principle_score_6',
]


def row(value=None, **overrides):
    """A tender row with every question set to `value` (on the stored 1..5 scale)."""
    data = dict((q, value) for q in ALL_QUESTIONS)
    data['מזהה המכרז'] = 'test:1/2026'
    data.update(overrides)
    return data


# --- the floor -------------------------------------------------------------

def test_lowest_answers_score_zero():
    """The whole point: 'לא מתקיים כלל' everywhere is 0%, not the old 20%."""
    scores = compute_scores(row(1))
    for field in PRINCIPLE_SCORES:
        assert scores[field] == 0.0, field


def test_highest_answers_score_one():
    scores = compute_scores(row(5))
    for field in PRINCIPLE_SCORES:
        assert scores[field] == 1.0, field


def test_middle_answer_is_half():
    scores = compute_scores(row(3))
    for field in PRINCIPLE_SCORES:
        assert scores[field] == pytest.approx(0.5), field


@pytest.mark.parametrize('stored', [1, 2, 3, 4, 5])
def test_scores_are_affine_against_the_old_scale(stored):
    """
    The old implementation returned 0.8 * correct + 0.2 for a uniform answer.
    Pinning that relationship documents why published history can be corrected
    arithmetically instead of being recomputed.
    """
    correct = compute_scores(row(stored))['principle_score_1']
    old = stored / 5.0
    assert old == pytest.approx(0.8 * correct + 0.2)


# --- maxima are derived, never stored --------------------------------------

def test_maximum_is_four_times_the_weight_sum():
    _, maximum = weighted(row(3), [('Q31', 0.8), ('Q32', 1.2)])
    assert maximum == pytest.approx(ANSWER_MAX * 2.0)


def test_weights_are_applied():
    total, maximum = weighted(row(1, Q31=5, Q32=1), [('Q31', 0.8), ('Q32', 1.2)])
    assert total == pytest.approx(0.8 * 4)
    assert maximum == pytest.approx(8.0)


# --- unanswered questions ---------------------------------------------------

def test_blank_answer_yields_unknown_not_zero():
    """
    Q61-Q63 are blank on every flagship record. They used to be coerced to 0,
    which silently removed up to 27 percentage points from principle 2.
    """
    scores = compute_scores(row(5, Q61=None, Q62=None, Q63=None))
    assert scores['principle_score_2_1'] is None
    assert scores['principle_score_2'] is None
    assert scores['principle_score_1'] == 1.0, 'other principles are unaffected'


def test_flagship_blank_shape_observed_in_production():
    """
    All 6 flagship ("מכרז דגל") records in soproc_measurement have BOTH Q36 and
    Q61-Q63 blank, while no base record has either. Under the old coercion this
    showed up as principle_score_1_3 == 0.0 and principle_score_2_1 == 0.0 --
    values that are arithmetically unreachable on the stored 1..5 scale, and the
    reason flagship tenders averaged 0.455 on principle 1 against 0.681 for base
    tenders. Both gaps must surface as unknown, not as a zero.
    """
    scores = compute_scores(row(5, Q36=None, Q61=None, Q62=None, Q63=None))
    assert scores['principle_score_1_3'] is None
    assert scores['principle_score_2_1'] is None
    assert scores['principle_score_1'] is None
    assert scores['principle_score_2'] is None
    assert scores['principle_score_1_1'] == 1.0, 'unaffected sub-principles still score'
    assert scores['principle_score_6'] == 1.0, 'unaffected principles still score'


def test_blank_answer_does_not_leak_into_the_principle_total():
    scores = compute_scores(row(5, Q31=None))
    assert scores['principle_score_1_1'] is None
    assert scores['principle_score_1'] is None
    assert scores['principle_score_1_2'] == 1.0


def test_out_of_range_answer_is_rejected():
    assert answer(row(5, Q31=0), 'Q31') is None
    assert answer(row(5, Q31=6), 'Q31') is None
    assert answer(row(5, Q31=''), 'Q31') is None


def test_booleans_are_not_answers():
    assert answer(row(5, Q31=True), 'Q31') is None


@pytest.mark.parametrize('stored,coded', [(1, 0), (2, 1), (3, 2), (4, 3), (5, 4)])
def test_stored_values_shift_by_one(stored, coded):
    assert answer(row(5, Q31=stored), 'Q31') == coded
    assert ANSWER_MIN <= coded <= ANSWER_MAX


# --- "not relevant" ---------------------------------------------------------

def test_irrelevant_question_leaves_both_total_and_maximum():
    """Q52 marked irrelevant: the sub-principle is Q39 alone, out of 4 not 8."""
    scores = compute_scores(row(1, Q39=4, **{'Q52 לא רלוונטי': ['x']}))
    assert scores['principle_score_1_4'] == pytest.approx(3.0 / 4.0)


def test_irrelevant_question_is_not_scored_as_zero():
    relevant = compute_scores(row(5))['principle_score_1']
    irrelevant = compute_scores(row(5, Q52=None, **{'Q52 לא רלוונטי': ['x']}))['principle_score_1']
    assert relevant == irrelevant == 1.0


def test_irrelevant_sub_principle_yields_none():
    scores = compute_scores(row(5, Q81=None, **{'Q81 לא רלוונטי': ['x']}))
    assert scores['principle_score_3_1'] is None
    assert scores['principle_score_3'] == 1.0, 'principle 3 falls back to Q74 alone'


def test_is_irrelevant_semantics():
    assert is_irrelevant(['anything']) is True
    assert is_irrelevant([]) is False
    assert is_irrelevant(None) is False


def test_q39_irrelevant_drops_the_question():
    """
    נספח ב' marks Q52, Q39 and Q81 as the three base questions offering
    "לא רלוונטי"; section ב'/2 only writes the Q52 branch of FirstPrin_4MAX.
    Q39 must drop the same way: the sub-principle becomes Q52 alone, out of 4.
    """
    scores = compute_scores(row(1, Q52=4, **{'Q39 לא רלוונטי': ['x']}))
    assert scores['principle_score_1_4'] == pytest.approx(3.0 / 4.0)


def test_q39_irrelevant_no_longer_sinks_the_whole_principle():
    """
    The regression this fixes: a Q39 marked NA is stored blank, so leaving it
    in the sub-principle turned it into an unknown answer and principle 1 came
    out None for the whole tender rather than being scored on Q52.
    """
    scores = compute_scores(row(5, Q39=None, **{'Q39 לא רלוונטי': ['x']}))
    assert scores['principle_score_1_4'] == 1.0
    assert scores['principle_score_1'] == 1.0


def test_both_q52_and_q39_irrelevant_leaves_the_sub_principle_unscored():
    scores = compute_scores(row(5, Q52=None, Q39=None,
                                **{'Q52 לא רלוונטי': ['x'], 'Q39 לא רלוונטי': ['x']}))
    assert scores['principle_score_1_4'] is None
    assert scores['principle_score_1'] == 1.0, 'principle 1 falls back to the other three'


# --- the cap on the top category -------------------------------------------

def test_low_answer_count_counts_zeros_and_ones():
    """0 and 1 count ('לא מתקיים כלל', 'מועטה'); 2 and up do not."""
    scores = compute_scores(row(5, Q31=1, Q32=2, Q34=3))
    assert scores['low_answer_count_1'] == 2


def test_low_answer_count_is_zero_when_nothing_is_low():
    scores = compute_scores(row(3))
    assert [scores['low_answer_count_%d' % i] for i in range(1, 7)] == [0] * 6


def test_low_answer_count_covers_every_question_in_the_principle():
    scores = compute_scores(row(1))
    assert scores['low_answer_count_1'] == 7, 'Q31 Q32 Q34 Q35 Q36 Q52 Q39'
    assert scores['low_answer_count_2'] == 5, 'Q61 Q62 Q63 Q67 Q610'
    assert scores['low_answer_count_3'] == 2, 'Q81 Q74'
    assert scores['low_answer_count_4'] == 1, 'Q91'
    assert scores['low_answer_count_5'] == 2, 'Q102 Q103'
    assert scores['low_answer_count_6'] == 4, 'Q111 Q115 Q117 Q119'


def test_low_answer_count_ignores_irrelevant_questions():
    """A question marked NA cannot count against the tender."""
    scores = compute_scores(row(5, Q81=None, **{'Q81 לא רלוונטי': ['x']}))
    assert scores['low_answer_count_3'] == 0
    low = compute_scores(row(1, Q81=None, **{'Q81 לא רלוונטי': ['x']}))
    assert low['low_answer_count_3'] == 1, 'Q74 alone'


def test_low_answer_count_is_unknown_when_an_answer_is():
    """A lower bound must not be used to cap a score."""
    scores = compute_scores(row(1, Q31=None))
    assert scores['low_answer_count_1'] is None
    assert scores['low_answer_count_2'] == 5, 'other principles still count'


def test_a_high_scoring_principle_can_still_be_capped():
    """
    The case the rule exists for: two criteria at 0 or 1, yet the principle is
    still over 70%. The score stays as it is - the consumer applies the cap.
    """
    scores = compute_scores(row(5, Q36=1, Q52=2))
    assert scores['principle_score_1'] > 0.70
    assert scores['low_answer_count_1'] == 2


# --- core aspects -----------------------------------------------------------

def test_core_aspects_are_on_the_spec_scale():
    """
    A stored 2 is 'בינונית' on the spec scale, and must be emitted as 1... no:
    stored 2 -> coded 1 -> 'מועטה'. The point is that consumers must map the
    coded value, not the stored one.
    """
    scores = compute_scores(row(2))
    assert scores['core_aspect_score_1'] == 1
    assert compute_scores(row(3))['core_aspect_score_1'] == 2


def test_core_aspects_track_their_questions():
    scores = compute_scores(row(1, Q35=5, Q67=4, Q74=3, Q91=2, Q103=5, Q117=1))
    assert scores['core_aspect_score_1'] == 4
    assert scores['core_aspect_score_2'] == 3
    assert scores['core_aspect_score_3'] == 2
    assert scores['core_aspect_score_4'] == 1
    assert scores['core_aspect_score_5'] == 4
    assert scores['core_aspect_score_6'] == 0


# --- helpers ----------------------------------------------------------------

def test_safe_div_guards_both_arguments():
    assert safe_div(None, 8) is None
    assert safe_div(4, 0) is None
    assert safe_div(0.0, 8) == 0.0


def test_combine_propagates_unknown():
    assert combine((1.0, 4.0), (None, None)) == (None, None)
    assert combine((1.0, 4.0), (2.0, 8.0)) == (3.0, 12.0)


def test_every_output_field_is_computed():
    scores = compute_scores(row(3))
    assert sorted(scores) == sorted(SCORE_FIELDS + COUNT_FIELDS)
