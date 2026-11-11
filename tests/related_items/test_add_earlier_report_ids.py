"""The indexer step that puts the resolved chapter's doc id on an audit's earlier_reports: offline, with sqlite."""
import json
import sqlite3

import dataflows as DF

from datapackage_pipelines_budgetkey.pipelines.budgetkey.elasticsearch import add_earlier_report_ids as E

FOLLOWED = dict(relation='followed_up', report='דוח מיוחד - פעולות ממשלת ישראל והיערכותה למשבר האקלים',
                title='פעולות ממשלת ישראל והיערכותה למשבר האקלים', year=2021)
EXTENDED = dict(relation='followed_up', report='דוח מיוחד - ביקורת מעקב מורחבת',
                title='פעולות ממשלת ישראל והיערכותה למשבר האקלים - ביקורת מעקב מורחבת', year=2024)
CITED = dict(relation='referenced', report='דוח שנתי - מאי 2023', title='המועצה הלאומית לכלכלה', year=2023)
OLD = dict(relation='referenced', report='דוח שנתי 40', title='פרק ישן', year=1990)


def mention(source, ref, target, status='linked', kind='comptroller_report'):
    return (source, kind, status, target, json.dumps(ref, ensure_ascii=False))


def engine(tmp_path, rows):
    path = tmp_path / 'db.sqlite'
    conn = sqlite3.connect(str(path))
    conn.execute('create table related_items_mentions '
                 '(source_doc_id text, mention_kind text, status text, target_doc_id text, details text)')
    conn.executemany('insert into related_items_mentions values (?, ?, ?, ?, ?)', rows)
    conn.commit()
    conn.close()
    return 'sqlite:///' + str(path)


def run(db, rows):
    out = DF.Flow(
        rows, DF.update_resource(-1, name='state_comptroller_audits'),
        E.flow({'resource': 'state_comptroller_audits', 'doc-id-pattern': 'state_comptroller_audits/{chapter_id}',
                'db-engine': db}),
    ).results()[0][0]
    return {r['chapter_id']: r['earlier_reports'] for r in out}


def test_linked_references_get_their_doc_id(tmp_path):
    src = 'state_comptroller_audits/10625'
    db = engine(tmp_path, [
        mention(src, FOLLOWED, 'state_comptroller_audits/7358'),
        mention(src, CITED, 'state_comptroller_audits/7610'),
        # not resolved / no page: never a doc_id, even when a target is named
        mention(src, EXTENDED, None, status='no_match'),
        mention(src, OLD, 'state_comptroller_audits/40', status='not_indexed'),
        # another source's identical reference doesn't leak
        mention('state_comptroller_audits/1', OLD, 'state_comptroller_audits/41'),
        # another mention kind
        mention(src, OLD, 'org/government_office/1', kind='entity'),
    ])
    refs = run(db, [dict(chapter_id=10625, earlier_reports=[FOLLOWED, EXTENDED, CITED, OLD]),
                    dict(chapter_id=3, earlier_reports=[]),
                    dict(chapter_id=4, earlier_reports=None)])
    assert [r['doc_id'] for r in refs[10625]] == [
        'state_comptroller_audits/7358', None, 'state_comptroller_audits/7610', None]
    assert refs[10625][0]['title'] == FOLLOWED['title'] and refs[10625][0]['relation'] == 'followed_up'
    assert refs[3] == [] and refs[4] == []


def test_year_as_string_still_matches():
    resolved = E.resolved_index([dict(source_doc_id='s', target_doc_id='t', details=dict(CITED, year='2023'))])
    assert E.with_doc_ids([CITED], resolved['s'])[0]['doc_id'] == 't'


def test_unreadable_table_leaves_doc_ids_null(tmp_path):
    refs = run('sqlite:///' + str(tmp_path / 'missing' / 'db.sqlite'), [dict(chapter_id=1, earlier_reports=[CITED])])
    assert refs[1] == [dict(CITED, doc_id=None)]
