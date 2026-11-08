"""The indexer step that puts related_items links on ES docs: offline, against a sqlite fixture table."""
import sqlite3

import dataflows as DF
import pytest

from datapackage_pipelines_budgetkey.pipelines.budgetkey.elasticsearch import add_related_items as A


def link(source, target, part=None, relation='mentioned', confidence=0.9, date='2024-01-01', year=None, code=None,
         source_kind=None, target_kind=None, title=None):
    return dict(source_doc_id=source, source_kind=source_kind or source.split('/')[0], source_part_id=part,
                source_title='title of ' + source, source_date=date, target_doc_id=target,
                target_kind=target_kind or {'org': 'entities'}.get(target.split('/')[0], target.split('/')[0]),
                target_title=title or 'title of ' + target, target_year=year, target_code=code, relation=relation,
                excerpt='excerpt ' + (part or source), confidence=confidence)


AUDIT = 'state_comptroller_audits/10'
LINKS = [
    # an audit, as a source: one org through two findings, a decision, a budget item, an earlier audit
    link(AUDIT, 'org/government_office/500100029', '10_1', 'audited', 0.95, '2024-05-01'),
    link(AUDIT, 'org/government_office/500100029', '10_2', 'mentioned', 0.9, '2024-05-01'),
    link(AUDIT, 'org/association/580000001', '10_2', 'mentioned', 0.85, '2024-05-01', title='עמותה'),
    link(AUDIT, 'gov_decisions/abc', None, 'cites', 0.98, '2024-05-01'),
    link(AUDIT, 'budget/00204902/2019', '10_1', 'funded', 0.95, '2024-05-01', year=2021, code='00204902'),
    link(AUDIT, 'state_comptroller_audits/3', None, 'follows_up', 0.95, '2024-05-01'),
    # the earlier audit is a target too
    # decisions citing each other and the org
    link('gov_decisions/abc', 'org/government_office/500100029', None, 'responsible', 0.93, '2020-03-01'),
    link('gov_decisions/def', 'gov_decisions/abc', None, 'amends', 0.98, '2022-07-15'),
    link('gov_decisions/ghi', 'budget/00204902/2010', None, 'funded', 0.95, '2011-01-01', year=2011,
         code='00204902'),
    link('gov_decisions/jkl', 'budget/00204902/2019', None, 'funded', 0.95, '2019-06-01', year=2019,
         code='00204902'),
    # issue lifted from the audit's finding
    link('state_comptroller_issues/7', 'org/government_office/500100029', '10_1', 'audited', 0.95, '2024-05-01'),
    # below a min-confidence
    link('gov_decisions/low', 'org/government_office/500100029', None, 'mentioned', 0.5, '2025-01-01'),
]


@pytest.fixture
def engine(tmp_path):
    path = tmp_path / 'links.db'
    conn = sqlite3.connect(str(path))
    conn.execute('create table related_items ({})'.format(', '.join(A.FIELDS)))
    conn.executemany('insert into related_items values ({})'.format(', '.join('?' * len(A.FIELDS))),
                     [[r[f] for f in A.FIELDS] for r in LINKS])
    conn.commit()
    conn.close()
    return 'sqlite:///' + str(path)


def run(engine, resource, pattern, rows, **params):
    params = dict(resource=resource, **{'doc-id-pattern': pattern, 'db-engine': engine}, **params)
    results, dp, _ = DF.Flow(rows, DF.update_resource(-1, name=resource), A.flow(params)).results()
    return results[0], dp.resources[0].descriptor['schema']['fields']


def test_prefix():
    assert A.doc_id_prefix('org/{kind}/{id}') == 'org/'
    assert A.doc_id_prefix('budget/{code}/{year}') == 'budget/'
    with pytest.raises(AssertionError):
        A.doc_id_prefix('{kind}/{id}')


def test_source_aggregates_parts_and_sorts_by_kind(engine):
    rows, fields = run(engine, 'state_comptroller_audits', 'state_comptroller_audits/{chapter_id}',
                       [dict(chapter_id='10'), dict(chapter_id='3'), dict(chapter_id='99')])
    audit, earlier, unlinked = rows
    related = audit['related_items']
    assert [e['doc_id'] for e in related] == [
        'org/government_office/500100029', 'org/association/580000001', 'gov_decisions/abc',
        'state_comptroller_audits/3', 'budget/00204902/2019']
    org = related[0]
    assert org['relation'] == 'audited' and org['relations'] == ['audited', 'mentioned']
    assert org['parts'] == ['10_1', '10_2'] and org['confidence'] == 0.95
    assert related[4]['year'] == 2021 and related[4]['code'] == '00204902'
    assert audit['mentioned_in'] == [] and audit['mentioned_in_count'] == 0
    # the earlier audit lists the one following it up
    assert earlier['related_items'] == []
    assert [(e['doc_id'], e['relation']) for e in earlier['mentioned_in']] == [(AUDIT, 'follows_up')]
    assert unlinked['related_items'] == [] and unlinked['mentioned_in'] == [] and unlinked['mentioned_in_count'] == 0
    by_name = {f['name']: f for f in fields}
    for name in ('related_items', 'mentioned_in'):
        assert by_name[name]['type'] == 'array' and by_name[name]['es:itemType'] == 'object'
        assert by_name[name]['es:index'] is False and by_name[name]['es:schema']['fields']
    assert by_name['mentioned_in_count']['type'] == 'integer'


def test_target_newest_first_capped_and_counted(engine):
    rows, _ = run(engine, 'entities', 'org/{kind}/{id}', [dict(kind='government_office', id='500100029')],
                  **{'max-mentioned-in': 2, 'min-confidence': 0.8})
    row = rows[0]
    mentioned = row['mentioned_in']
    # one entry per source doc (the audit's two findings fold), newest first; the issue and the audit share a date
    assert [e['doc_id'] for e in mentioned] == [AUDIT, 'state_comptroller_issues/7']
    assert row['mentioned_in_count'] == 3
    audit = mentioned[0]
    assert audit['relation'] == 'audited' and audit['part_id'] == '10_1' and audit['parts'] == ['10_1', '10_2']
    assert audit['excerpt'] == 'excerpt 10_1' and audit['date'] == '2024-05-01'
    assert audit['kind'] == 'state_comptroller_audits' and audit['title'] == 'title of ' + AUDIT


def test_decision_is_source_and_target(engine):
    rows, _ = run(engine, 'gov_decisions', 'gov_decisions/{id}', [dict(id='abc')])
    row = rows[0]
    assert [e['doc_id'] for e in row['related_items']] == ['org/government_office/500100029']
    assert [e['doc_id'] for e in row['mentioned_in']] == [AUDIT, 'gov_decisions/def']
    assert row['mentioned_in'][1]['relation'] == 'amends'


def test_budget_folds_the_code_in_other_items(engine):
    rows, _ = run(engine, 'budget', 'budget/{code}/{year}', [dict(code='00204902', year=2019),
                                                             dict(code='00204902', year=2010)],
                  **{'code-field': 'code'})
    item_2019, item_2010 = rows
    assert [(e['doc_id'], e['year'], e['same_item']) for e in item_2019['mentioned_in']] == [
        (AUDIT, 2021, True), ('gov_decisions/jkl', 2019, True), ('gov_decisions/ghi', 2011, False)]
    assert item_2019['mentioned_in_count'] == 3
    assert [(e['doc_id'], e['same_item']) for e in item_2010['mentioned_in']] == [
        ('gov_decisions/ghi', True), (AUDIT, False), ('gov_decisions/jkl', False)]
    # without code-field, only the item's own links
    rows, _ = run(engine, 'budget', 'budget/{code}/{year}', [dict(code='00204902', year=2010)])
    assert [e['doc_id'] for e in rows[0]['mentioned_in']] == ['gov_decisions/ghi']


def test_deterministic(engine):
    rows = [dict(kind='government_office', id='500100029'), dict(kind='association', id='580000001')]
    first, _ = run(engine, 'entities', 'org/{kind}/{id}', [dict(r) for r in rows])
    second, _ = run(engine, 'entities', 'org/{kind}/{id}', [dict(r) for r in reversed(rows)])
    assert str(first) == str(list(reversed(second)))


@pytest.mark.parametrize('db', ['missing_table', 'no_engine'])
def test_never_fails_indexing(tmp_path, monkeypatch, db):
    monkeypatch.delenv('DPP_DB_ENGINE', raising=False)
    engine = 'sqlite:///' + str(tmp_path / 'empty.db') if db == 'missing_table' else None
    rows, fields = run(engine, 'entities', 'org/{kind}/{id}', [dict(kind='association', id='1'), dict(id='2')])
    assert all(r['related_items'] == [] and r['mentioned_in'] == [] and r['mentioned_in_count'] == 0 for r in rows)
    assert {'related_items', 'mentioned_in', 'mentioned_in_count'} <= {f['name'] for f in fields}


def test_other_resources_untouched(engine):
    results, dp, _ = DF.Flow(
        [dict(id='abc')], DF.update_resource(-1, name='gov_decisions'),
        [dict(x=1)], DF.update_resource(-1, name='other'),
        A.flow({'resource': 'gov_decisions', 'doc-id-pattern': 'gov_decisions/{id}', 'db-engine': engine}),
    ).results()
    assert results[1] == [dict(x=1)]
    assert [f['name'] for f in dp.resources[1].descriptor['schema']['fields']] == ['x']
