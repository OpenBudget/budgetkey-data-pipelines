from datapackage_pipelines_budgetkey.pipelines.analysis import figures as F


def test_format_value():
    assert F.format_value(5.12e9, 'currency') == '5.1 מיליארד ₪'
    assert F.format_value(2e6, 'currency') == '2 מיליון ₪'
    assert F.format_value(942485, 'currency') == '942,485 ₪'
    assert F.format_value(12.5, 'percent') == '12.5%'
    assert F.format_value('2025-09-07', 'date') == '07/09/2025'
    assert F.format_value(None, 'currency') == '—'


def chart(**kw):
    fig = dict(kind='chart', name='c', dataset='budget_items_data', sql="select ... where code = '24.07'",
               chart_type='bar', x='year', series=[dict(column='a', label='תקציב מקורי')], title='T')
    fig.update(kw)
    return fig


def test_time_series_is_always_a_line():
    rows = [dict(year=2024, a=1), dict(year=2025, a=2)]
    assert F.chart_descriptor(chart(), rows)['chart'][0]['type'] == 'scatter'
    assert F.chart_descriptor(chart(x='name'), [dict(name='x', a=1)])['chart'][0]['type'] == 'bar'


def test_pie_folds_small_slices():
    rows = [dict(name=str(i), a=100 - i) for i in range(20)]
    trace = F.chart_descriptor(chart(chart_type='pie', x='name'), rows)['chart'][0]
    assert len(trace['labels']) == F.MAX_PIE_SLICES and trace['labels'][-1] == 'אחר'


def test_table_must_link():
    fig = dict(kind='table', name='t', sql='x', columns=[dict(column='title', label='שם')])
    try:
        F.check_rows(fig, [dict(title='x', item_url='u')])
        assert False
    except F.FigureError:
        pass
    fig['columns'][0]['link_column'] = 'item_url'
    F.check_rows(fig, [dict(title='x', item_url='u')])


def test_anomalies():
    rows = [dict(year=2018, a=1e9), dict(year=2019, a=1.05e9), dict(year=2020, a=5e9), dict(year=2026, a=None)]
    found = F.anomalies(chart(), rows, latest_year=2026)
    assert len(found) == 1 and '2019' in found[0] and '2020' in found[0]
    found = F.anomalies(chart(), rows[:3] + [dict(year=2021, a=None), dict(year=2022, a=5e9)], latest_year=2026)
    assert any('no value for 2021' in a for a in found)


def test_missing_code_years_ignores_small_lines():
    amounts = {'20.62': {2020: 9e9, 2021: 9e9}, '36.41': {2018: 1e9, 2019: 1e9, 2020: 1e9, 2021: 1e9},
               '36.41.01.08': {2018: 1e6}}
    missing = F.missing_code_years(['20.62', '36.41', '36.41.01.08'], [2018, 2019, 2020, 2021], amounts)
    assert missing == {'20.62': [2018, 2019]}
    assert F.safe_range([2018, 2019, 2020, 2021], missing) == (2020, 2021)


def trend(estimate_before=2020):
    rows = [dict(year=y, allocated=a, revised=a, used=a) for y, a in
            [(2018, 1e9), (2019, 1.1e9), (2020, None), (2021, 9e9), (2022, 9.5e9)]]
    rows[2]['revised'] = rows[2]['used'] = 8.8e9
    return dict(rows=rows, estimate_before=estimate_before)


def test_trend_descriptor_shades_estimates():
    d = F.trend_descriptor(dict(title='T'), trend())
    assert [t['name'] for t in d['chart']] == [label for _, label in F.TREND_SERIES]
    # category axis: shapes are positioned by index (2018, 2019 are indices 0, 1)
    assert d['layout']['shapes'][0]['x0'] == -0.5 and d['layout']['shapes'][0]['x1'] == 1.5
    assert F.clean_title('תקציב בריאות הנפש (במיליארדי ש״ח)') == 'תקציב בריאות הנפש'
    assert 'shapes' not in F.trend_descriptor(dict(title='T'), trend(None))['layout']


def test_trend_notes_and_anomalies():
    notes = F.trend_notes(trend())
    assert any('2020' in n and 'הערכה' in n for n in notes)
    assert F.KNOWN_GAPS[2020] in notes
    # the jump into 2020 is in the estimated years, and 2020's missing original budget is a known gap
    assert F.trend_anomalies(dict(title='T'), trend(), latest_year=2022) == []


def test_pie_merges_labels_and_drops_empty_slices():
    rows = [dict(name='משרדים אחרים', a=8e6), dict(name='משרדים אחרים', a=0.0), dict(name='א', a=5e6), dict(name='ב', a=None)]
    trace = F.chart_descriptor(chart(chart_type='pie', x='name'), rows)['chart'][0]
    assert trace['labels'] == ['משרדים אחרים', 'א'] and trace['values'] == [8e6, 5e6]


def test_table_formatting_and_empty_amounts():
    fig = dict(kind='table', name='t', columns=[
        dict(column='title', label='שם', link_column='item_url'),
        dict(column='decision_number', label='מספר', format='number'),
        dict(column='approved', label='אושר', format='currency'),
        dict(column='paid', label='שולם', format='currency'),
    ])
    rows = [dict(title='א', item_url='u1', decision_number=3406, approved=1e6, paid=0),
            dict(title='ב', item_url='u2', decision_number=12, approved=0, paid=None)]
    md = F.render_table(fig, rows)
    assert '3406' in md and '3,406' not in md            # identifiers aren't thousands-separated
    assert 'שולם' not in md                               # all-zero money column dropped
    assert '[ב]' not in md                                # row with no amounts dropped
