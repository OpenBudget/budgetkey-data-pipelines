import sys
from pathlib import Path

HERE = Path(__file__).parents[2] / 'datapackage_pipelines_budgetkey/pipelines/budget/municipal/budget_files'
sys.path.insert(0, str(HERE))

import datacity_export as x  # noqa: E402


def line(file_url, code, amount, kind='proposed', direction=None, sheet='s', year=2024, authority=70, fmt='xlsx',
         title='t'):
    direction = direction or ('income' if code[1] in '12345' else 'expense')
    return dict(file_url=file_url, code=code, amount=str(amount), column_kind=kind, direction=direction, sheet=sheet,
                fiscal_year=str(year), authority_code=str(authority), format=fmt, doc_type='budget', title=title)


def aggregate(lines):
    agg = x.Aggregates()
    for ln in lines:
        agg.add(ln)
    return agg.candidates()


def by_part(lines):
    out = {}
    for ln in lines:
        out.setdefault((ln['file_url'], ln['column_kind'], ln['direction']), []).append(x.line_of(ln))
    return out


def test_codes():
    assert x.cbs_symbol(5506) == '06' and x.cbs_symbol(70) == '0070' and x.cbs_symbol(5000) == '5000'
    # Ofakim (a city, 31) and Nahal Sorek (a regional council, 5531) are distinct
    assert x.cbs_symbol(31) == '0031' and x.cbs_symbol(5531) == '31'
    assert x.card_code('81111/110/5') == '1811115110'
    assert x.card_code('1611000110') == '1611000110'
    assert x.card_code('61011119') == '1610100111' and x.card_code('11001218') == '1110000121'


def test_phase_years():
    lines = [line('a', '1611000110', 5e6, kind=k) for k in ('proposed', 'approved', 'executed')]
    keys = {k[:3] for k in aggregate(lines)}
    assert keys == {(70, 2024, 'מקורי'), (70, 2023, 'מאושר'), (70, 2022, 'ביצוע')}


def test_spreadsheet_beats_pdf_and_covered_phases_are_skipped():
    lines = [line('a.pdf', '1611000110', 9e6, fmt='pdf'), line('b.xlsx', '1611000110', 5e6)]
    cands = aggregate(lines)
    chosen = x.choose(cands, covered=set())
    assert [p['file_url'] for p in chosen[(70, 2024)]['מקורי']] == ['b.xlsx']
    assert x.choose(cands, covered={('0070', 2024, 'מקורי')}) == {}


def test_small_sides_are_not_budgets():
    assert x.choose(aggregate([line('a', '1611000110', 1000)]), set()) == {}


def test_separate_income_file_is_added_when_it_balances():
    lines = [line('exp', '1611000110', 5e6), line('exp', '1711000110', 5e6),
             line('inc', '1111000100', 10e6)]
    parts = x.choose(aggregate(lines), set())[(70, 2024)]['מקורי']
    assert sorted((p['file_url'], p['direction']) for p in parts) == [('exp', 'expense'), ('inc', 'income')]


def test_second_file_added_only_when_it_brings_the_sides_closer():
    split = [line('salaries', '1611000110', 4e6), line('other', '1711000750', 6e6),
             line('salaries', '1111000100', 10e6)]
    parts = x.choose(aggregate(split), set())[(70, 2024)]['מקורי']
    assert {p['file_url'] for p in parts if p['direction'] == 'expense'} == {'salaries', 'other'}

    summary = [line('detail', '1611000110', 5e6), line('detail', '1711000110', 5e6),
               line('detail', '1111000100', 10e6), line('summary', '1600000000', 10e6)]
    parts = x.choose(aggregate(summary), set())[(70, 2024)]['מקורי']
    assert {p['file_url'] for p in parts if p['direction'] == 'expense'} == {'detail'}


def test_copied_sheet_is_left_out():
    lines = [line('a', c, 5e6, sheet='main') for c in ('1611000110', '1711000110', '1811000110')] + \
            [line('a', c, 5e6, sheet='pivot') for c in ('1611000110', '1711000110')]
    part = x.choose(aggregate(lines), set())[(70, 2024)]['מקורי'][0]
    assert part['dropped_sheets'] == ['pivot'] and part['lines'] == 3


def test_rows_signs_and_merged_codes():
    lines = [line('a', '1611000110', 3e6), line('a', '1611000110', 2e6), line('a', '1711000110', 5e6),
             line('a', '1111000100', -10e6), line('a', '1.611.000', 7)]
    parts = x.choose(aggregate(lines), set())[(70, 2024)]['מקורי']
    rows, summary = x.phase_rows(parts, by_part(lines), 'מקורי', 2024, 'אשדוד')
    values = {r['card-code']: r['value'] for r in rows}
    assert values == {'1611000110': 5e6, '1711000110': 5e6, '1111000100': -10e6}
    assert (summary['expense'], summary['income'], summary['merged_lines'], summary['invalid_codes']) == \
        (10e6, -10e6, 1, 1)
    assert set(rows[0]) == set(x.FIELDS) and rows[0]['municipality-name'] == 'אשדוד'


def test_negative_expenses_are_flipped_and_zero_lines_kept():
    lines = [line('a', '1611000110', -6e6), line('a', '1711000110', 0), line('a', '1111000100', 6e6)]
    parts = x.choose(aggregate(lines), set())[(70, 2024)]['מקורי']
    rows, _ = x.phase_rows(parts, by_part(lines), 'מקורי', 2024, 'אשדוד')
    assert {r['card-code']: r['value'] for r in rows} == {'1611000110': 6e6, '1711000110': 0, '1111000100': -6e6}


def phase(rows, expense, income=None):
    return dict(rows=rows, expense=expense, income=-expense if income is None else income)


def test_implausible_phases_are_found():
    # Beersheba 2013: one phase read in the wrong unit; Kfar Saba 2012: a chapter summary
    budgets = {(2012, 'ביצוע'): phase(5958, 1.16e9), (2013, 'מאושר'): phase(5958, 1.24e9),
               (2013, 'מקורי'): phase(5559, 1.23e12), (2012, 'מאושר'): phase(23, 4.6e6, 0)}
    assert x.implausible(budgets, anchor=1.45e9) == {(2013, 'מקורי'): '991.9 times the typical budget',
                                                    (2012, 'מאושר'): '23 lines'}


def test_implausible_phases_reference():
    # Harish outgrew its 2015 budget: its complete budgets agree with each other, not with the anchor
    harish = {(2025, 'מאושר'): phase(881, 4.3e8), (2026, 'מקורי'): phase(1037, 4.8e8), (2021, 'מקורי'): phase(334, 5.1e7, 0)}
    assert x.implausible(harish, anchor=2.6e7) == {(2021, 'מקורי'): '0.1062 times the typical budget'}
    # Kiryat Malachi's one complete budget is 30 times its anchor: the anchor wins
    malachi = {(2020, 'מקורי'): phase(1110, 5.5e9)}
    assert list(x.implausible(malachi, anchor=1.8e8)) == [(2020, 'מקורי')]
    # Without an anchor, a single complete budget is the reference; with nothing complete, sizes go unjudged
    assert list(x.implausible({(2023, 'מקורי'): phase(936, 6.8e8), (2022, 'מאושר'): phase(111, 4e6)})) == [(2022, 'מאושר')]
    assert x.implausible({(2022, 'מאושר'): phase(111, 4e6, 0)}) == {}
