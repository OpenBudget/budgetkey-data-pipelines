"""add_missing_columns: lets an update-mode dump.to_sql write a resource that gained fields."""
import importlib.util
from pathlib import Path

import dataflows as DF
import pytest
from sqlalchemy import create_engine, inspect

spec = importlib.util.spec_from_file_location(
    'add_missing_columns', Path(__file__).parents[2] / 'datapackage_pipelines_budgetkey/processors/add_missing_columns.py')
amc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(amc)


def dump(url, rows, fields):
    return DF.Flow(rows, DF.update_resource(-1, name='docs'), *[DF.set_type(n, type=t) for n, t in fields],
                   DF.dump_to_sql(dict(mirror={'resource-name': 'docs', 'mode': 'update', 'update_keys': ['id']}),
                                  engine=url))


def test_new_fields_get_columns(tmp_path):
    url = 'sqlite:///%s' % (tmp_path / 'db.sqlite')
    dump(url, [dict(id=1, title='a')], [('id', 'integer'), ('title', 'string')]).process()
    rows = [dict(id=1, title='a', related_items=[dict(doc_id='org/x/1')], mentioned_in_count=2)]
    fields = [('id', 'integer'), ('title', 'string'), ('related_items', 'array'), ('mentioned_in_count', 'integer')]
    with pytest.raises(Exception):
        dump(url, [dict(r) for r in rows], fields).process()  # what the indexers would hit
    _, dp, _ = DF.Flow([dict(r) for r in rows], DF.update_resource(-1, name='docs'),
                       *[DF.set_type(n, type=t) for n, t in fields]).results()
    engine = create_engine(url)
    amc.add_missing_columns(engine, {'mirror': {'resource-name': 'docs'}}, dp.descriptor)
    assert {c['name'] for c in inspect(engine).get_columns('mirror')} >= {'related_items', 'mentioned_in_count'}
    dump(url, [dict(r) for r in rows], fields).process()
    amc.add_missing_columns(engine, {'mirror': {'resource-name': 'docs'}}, dp.descriptor)  # idempotent


def test_missing_table_is_left_to_dump(tmp_path):
    engine = create_engine('sqlite:///%s' % (tmp_path / 'db.sqlite'))
    assert amc.missing_columns(engine, 'nothing_here', [dict(name='a', type='string')]) == []
