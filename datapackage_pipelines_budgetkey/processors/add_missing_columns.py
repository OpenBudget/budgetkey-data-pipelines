"""Adds to existing DB tables the columns their resources gained, before a dump.to_sql in update mode.

dump.to_sql in update mode keeps an existing table as it is and fails on fields it doesn't have ("Unconsumed column
names"), so a new field on an indexed type (e.g. related_items) would break its indexer until someone altered the
table by hand. This adds the missing columns, with the types tableschema_sql gives them; it never drops or changes a
column, and does nothing for a table that doesn't exist yet (dump.to_sql creates it).

Parameters: tables - the same {table name: {resource-name: ...}} mapping dump.to_sql gets.
"""
import logging
import os

from datapackage_pipelines.wrapper import ingest, spew
from sqlalchemy import create_engine, inspect, text

POSTGRES_TYPES = {
    'string': 'TEXT', 'integer': 'INTEGER', 'number': 'NUMERIC', 'boolean': 'BOOLEAN', 'date': 'DATE',
    'datetime': 'TIMESTAMP WITHOUT TIME ZONE', 'time': 'TIME WITHOUT TIME ZONE', 'array': 'JSONB', 'object': 'JSONB',
    'geojson': 'JSONB', 'year': 'INTEGER', 'yearmonth': 'TEXT', 'duration': 'TEXT', 'geopoint': 'TEXT', 'any': 'TEXT',
}
OTHER_TYPES = dict(POSTGRES_TYPES, number='FLOAT', array='TEXT', object='TEXT', geojson='TEXT',
                   datetime='TIMESTAMP', time='TIME')


def missing_columns(engine, table, fields):
    """[(name, SQL type)] of the fields the existing table lacks; [] when there's no such table."""
    inspector = inspect(engine)
    if not inspector.has_table(table):
        return []
    existing = {c['name'] for c in inspector.get_columns(table)}
    types = POSTGRES_TYPES if engine.dialect.name == 'postgresql' else OTHER_TYPES
    return [(f['name'], types.get(f.get('type', 'string'), 'TEXT')) for f in fields if f['name'] not in existing]


def add_missing_columns(engine, tables, datapackage):
    resources = {r['name']: r for r in datapackage['resources']}
    for table, config in tables.items():
        resource = resources.get(config['resource-name'])
        if resource is None:
            continue
        columns = missing_columns(engine, table, resource['schema']['fields'])
        if not columns:
            continue
        quote = engine.dialect.identifier_preparer.quote
        with engine.begin() as conn:
            for name, sql_type in columns:
                logging.info('Adding column %s %s to %s', name, sql_type, table)
                conn.execute(text('ALTER TABLE %s ADD COLUMN %s %s' % (quote(table), quote(name), sql_type)))


if __name__ == '__main__':
    parameters, datapackage, resource_iterator = ingest()
    add_missing_columns(create_engine(os.environ['DPP_DB_ENGINE']), parameters['tables'], datapackage)
    spew(datapackage, resource_iterator)
