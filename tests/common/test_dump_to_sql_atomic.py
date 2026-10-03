import dataflows as DF
import pytest
from sqlalchemy import create_engine, inspect, text

from datapackage_pipelines_budgetkey.common.dump_to_sql_atomic import dump_to_sql_atomic


@pytest.fixture
def engine(tmp_path):
    return create_engine('sqlite:///%s' % (tmp_path / 'db.sqlite'))


def write(engine, n, start=0, fail_at=None, mode='rewrite'):
    def maybe_fail(rows):
        for i, row in enumerate(rows):
            if i == fail_at:
                raise RuntimeError('interrupted')
            yield row
    DF.Flow([dict(id=i) for i in range(start, start + n)], DF.update_resource(-1, name='r'),
            DF.set_primary_key(['id']), maybe_fail,
            dump_to_sql_atomic({'t': {'resource-name': 'r', 'mode': mode}}, engine=engine)).process()


def count(engine):
    with engine.connect() as c:
        return c.execute(text('select count(*) from t')).scalar()


def test_rewrite_swaps_in_when_complete(engine):
    write(engine, 10)
    write(engine, 3, start=100)
    assert count(engine) == 3
    assert 't__new' not in inspect(engine).get_table_names()


def test_interrupted_rewrite_keeps_the_live_table(engine):
    write(engine, 10)
    with pytest.raises(Exception):
        write(engine, 50, start=100, fail_at=20)
    assert count(engine) == 10
    write(engine, 4, start=200)                     # the next run recovers over the leftover staging table
    assert count(engine) == 4


def test_update_mode_writes_in_place(engine):
    write(engine, 5, mode='update')
    write(engine, 8, mode='update')
    assert count(engine) == 8
    assert 't__new' not in inspect(engine).get_table_names()
