"""A drop-in DF.dump_to_sql for tables written in `rewrite` mode, without the empty window.

DF.dump_to_sql in rewrite mode drops the table and refills it row by row. While it writes, readers see an empty
or partial table, and if the run is interrupted (a deploy restarting the pipelines, a crash, OOM) the table stays
empty until the next successful run - which is how budget_items_data went empty in production.

dump_to_sql_atomic writes each rewrite-mode table to `<table>__new` and, only once every resource has been fully
written, swaps it in within one transaction. An interrupted run leaves the live table as it was. Tables in
`update` mode are written in place, exactly as with DF.dump_to_sql.

    DF.Flow(..., dump_to_sql_atomic({'my_table': {'resource-name': 'my-resource'}}))
"""
import logging

from dataflows.processors.dumpers.to_sql import SQLDumper
from sqlalchemy import text

STAGING_SUFFIX = '__new'


class AtomicSQLDumper(SQLDumper):

    def __init__(self, tables, engine='env://DATAFLOWS_DB_ENGINE', **options):
        self.swaps = {}         # staging table -> live table
        staged = {}
        for table, spec in tables.items():
            if spec.get('mode', 'rewrite') == 'rewrite':
                staged[table + STAGING_SUFFIX] = spec
                self.swaps[table + STAGING_SUFFIX] = table
            else:
                staged[table] = spec
        super().__init__(staged, engine=engine, **options)

    def finalize(self):
        # Called once, after every resource was written in full; never reached when the run is interrupted.
        super().finalize()
        with self.engine.begin() as conn:
            for staging, table in self.swaps.items():
                swap(conn, staging, table)
                logging.info('Swapped %s into %s', staging, table)


def swap(conn, staging, table):
    """Replaces `table` with `staging` inside the caller's transaction, renaming the staging table's indexes too,
    so the next run's staging table can create indexes with the same names."""
    conn.execute(text('DROP TABLE IF EXISTS "%s"' % table))
    conn.execute(text('ALTER TABLE "%s" RENAME TO "%s"' % (staging, table)))
    if conn.dialect.name == 'postgresql':
        indexes = conn.execute(text("SELECT indexname FROM pg_indexes WHERE tablename = :t"), {'t': table})
        for (index,) in list(indexes):
            if staging in index:
                conn.execute(text('ALTER INDEX "%s" RENAME TO "%s"' % (index, index.replace(staging, table))))


def dump_to_sql_atomic(tables, engine='env://DATAFLOWS_DB_ENGINE', **options):
    return AtomicSQLDumper(tables, engine=engine, **options)
