from datapackage_pipelines_budgetkey.pipelines.analysis import render as R

FIGS = [
    dict(kind='value', name='total'),
    dict(kind='budget_trend', name='trend'),
    dict(kind='table', name='programs', columns=[dict(column='title', label='שם', link_column='item_url')]),
]
GOOD = '\n'.join([
    '## בקצרה', 'התקציב עומד על {{value:total}} בשנת 2026.',
    '## כמה כסף ולאן', '{{chart:trend}}',
    '## מי אחראי', 'משרד הבריאות.',
    '## התכניות העיקריות', '{{table:programs}}',
])


def test_check_template_accepts_a_good_page():
    assert R.check_template(GOOD, FIGS, lambda u: True) == []


def test_check_template_problems():
    bad = GOOD.replace('{{value:total}}', '{{value:missing}}').replace('## מי אחראי', '## משהו אחר')
    bad += '\nהוצאו 3.5 מיליארד ₪ ו-[קישור](https://next.obudget.org/i/abc).'
    problems = ' | '.join(R.check_template(bad, FIGS, lambda u: False))
    for expected in ('never defined', 'Missing required section "## מי אחראי"', 'Unexpected sections',
                     '3.5 מיליארד', 'is invented'):
        assert expected in problems


def test_literal_amounts_allow_years():
    assert R.literal_amounts('בשנים 2015-2026 ובהחלטה 550') == []
    assert R.literal_amounts('סך של 1,234,567 ש"ח ו-12% מהתקציב') != []


def test_autolink_links_first_plain_mention_only():
    template = '## בקצרה\nהמקלטים לנשים מוכות פועלים. מקלטים לנשים מוכות שוב.\n| מקלטים לנשים מוכות | x |'
    out = R.autolink(template.replace('המקלטים', 'את מקלטים'), {'מקלטים לנשים מוכות': 'https://x/1'})
    assert out.count('[מקלטים לנשים מוכות](https://x/1)') == 1
    assert out.split('\n')[2] == '| מקלטים לנשים מוכות | x |'


def test_unlink_invalid_keeps_text():
    text = 'ב[משרד התיירות](https://next.obudget.org/i/bad) ו[תקומה](https://next.obudget.org/i/good)'
    assert R.unlink_invalid(text, lambda u: u.endswith('good')) == 'במשרד התיירות ו[תקומה](https://next.obudget.org/i/good)'


def test_caption_markdown():
    caption = dict(items=[dict(title='גננות', item_url='https://x/1', code='20.62.01.02')], notes=['הערה'])
    assert R.caption_markdown(caption) == '*הסעיפים התקציביים בתרשים:* [גננות](https://x/1) (20.62.01.02)\n\n*הערה*'
