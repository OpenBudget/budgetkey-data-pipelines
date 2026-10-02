import datetime

from datapackage_pipelines_budgetkey.pipelines.analysis import pages as P


def issue(category, quote, replacement, severity='blocking'):
    return dict(category=category, severity=severity, quote=quote, problem='p', replacement=replacement)


def test_apply_fixes():
    template = 'הירידה נובעת ברובה מהמלונות ({{value:hotels}}).\nשורה אחרת.'
    fixed, applied, remaining = P.apply_fixes(template, [
        issue('contradiction', 'הירידה נובעת ברובה מהמלונות ({{value:hotels|1.4 מיליארד ₪}}).',
              'הירידה נובעת בחלקה מהמלונות ({{value:hotels|1.4 מיליארד ₪}}).'),
        issue('neutrality', 'שורה אחרת.', 'שורה עם {{value:invented}}.'),        # adds a placeholder
        issue('relevance', 'שורה אחרת.', 'x'),                                      # not a sentence fix
    ], lambda u: True)
    assert fixed == 'הירידה נובעת בחלקה מהמלונות ({{value:hotels}}).\nשורה אחרת.'
    assert len(applied) == 1 and len(remaining) == 2


def test_fix_template():
    assert P.fix_template('```markdown\nטקסט\n## כמה כסף ולאן\n```').startswith('## בקצרה\nטקסט')


def test_search_text():
    body = 'א [ב](https://x) **ג**\n```plotly\n{"a": 1}\n```\n| ד |'
    assert P.search_text(body) == 'א ב ג ד'


def test_plan():
    now = datetime.datetime(2026, 10, 2)
    version = P.prompt_version()
    fresh = dict(status='published', prompt_version=version, generated_at=now, latest_year=2026)
    previous = {
        'fresh': dict(fresh, slug='fresh'),
        'old': dict(fresh, slug='old', generated_at=now - datetime.timedelta(days=400)),
        'new-year': dict(fresh, slug='new-year', latest_year=2025),
        'failed': dict(fresh, slug='failed', status='failed'),
        'prompts': dict(fresh, slug='prompts', prompt_version='x'),
    }
    topics = [dict(slug=s) for s in ['brand-new', 'fresh', 'old', 'new-year', 'failed', 'prompts']]
    generate, refresh = P.plan(topics, previous, 2026, now=now)
    assert [t['slug'] for t, _ in generate][0] == 'brand-new'
    assert {t['slug'] for t, _ in generate} == {'brand-new', 'old', 'new-year', 'failed', 'prompts'}
    assert [t['slug'] for t, _ in refresh] == ['fresh']


def test_plan_cap_defers_to_refresh(monkeypatch):
    monkeypatch.setattr(P, 'MAX_GENERATIONS_PER_RUN', 1)
    now = datetime.datetime(2026, 10, 2)
    stale = dict(status='published', prompt_version='old', generated_at=now, latest_year=2026)
    generate, refresh = P.plan([dict(slug='a'), dict(slug='b')], {'b': dict(stale, slug='b')}, 2026, now=now)
    assert [t['slug'] for t, _ in generate] == ['a'] and [t['slug'] for t, _ in refresh] == ['b']
