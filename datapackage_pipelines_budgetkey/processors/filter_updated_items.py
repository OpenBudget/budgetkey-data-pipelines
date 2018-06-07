import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.sql import text

from datapackage_pipelines.wrapper import spew, ingest


def get_connection_string():
    connection_string = os.environ.get("DPP_DB_ENGINE")
    assert connection_string is not None, \
        "Couldn't connect to DB - " \
        "Please set your '%s' environment variable" % "DPP_DB_ENGINE"
    return connection_string


def filter_resource(rows, stmt, k_fields, v_fields):
    logging.info('connecting to %s', get_connection_string())
    conn = None
    engine = None
    for row in rows:
        if engine is None:
            engine = create_engine(get_connection_string())
            conn = engine.connect()
            logging.info('connected to db')            
        try:
            params = dict((k, row[k]) for k in k_fields)
            db_values = conn.execute(stmt, **params).first()
            if db_values:
                v_values = [row[v] for v in v_fields]
                if all((db_value == v_value)
                       for db_value, v_value
                       in zip(db_values, v_values)):
                    continue
                logging.info('queried with params %r', params)
                logging.info('GOT %r', db_values)
                logging.info('INCOMING %r', v_values)
                logging.info('NEW %r != %r', db_values, v_values)
                yield row
            else:
                logging.info('NEW ROW %r', v_values)
                yield row
        except Exception:
            logging.exception('Failure!')


def process_resources(res_iter, resource_name, stmt, k_fields, v_fields):
    processed = False
    for res in res_iter:
        if res.spec['name'] == resource_name:
            logging.info('processing %s', res.spec['name'])
            processed = True
            yield filter_resource(res, stmt, k_fields, v_fields)
        else:
            logging.info('skipping %s', res.spec['name'])
            yield res
    assert processed


if __name__ == '__main__':

    logging.info('starting')
    params, dp, res_iter = ingest()
    logging.info('got params %r', params)

    resource_name = params['resource']
    db_table = params['db_table']
    k_fields = params['key_fields']
    v_fields = params['value_fields']

    stmt_fields = ','.join('"{}"'.format(v) for v in v_fields)
    stmt_condition = ' and '.join('("{}"=:{})'.format(k, k) for k in k_fields)
    stmt = text(' '.join([
        f'select {stmt_fields} from "{db_table}"',
        f'where {stmt_condition}',
        f'order by __updated_timestamp desc limit 1'
    ]))

    logging.info('statement %s', stmt)

    spew(dp, process_resources(res_iter, resource_name, stmt, k_fields, v_fields))