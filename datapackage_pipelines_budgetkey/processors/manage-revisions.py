import os
import logging
import datetime
import hashlib
from kvfile import KVFile

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines_budgetkey.common.line_selector import LineSelector

from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError, OperationalError

now = datetime.datetime.now()


def get_connection_string():
    connection_string = os.environ.get("DPP_DB_ENGINE")
    assert connection_string is not None, \
        "Couldn't connect to DB - " \
        "Please set your '%s' environment variable" % "DPP_DB_ENGINE"
    return connection_string


def calc_key(row, key_fields):
    key = '|'.join(str(row[k]) for k in key_fields)
    return key


def calc_hash(row, hash_fields):
    hash_fields = sorted(hash_fields)
    hash_src = '|'.join(str(row[k]) for k in hash_fields)
    if len(hash_src) > 0:
        hash = hashlib.md5(hash_src.encode('utf8')).hexdigest()
    else:
        hash = ''
    return hash


def get_all_existing_ids(connection_string, db_table, key_fields, STATUS_FIELD_NAMES):
    db_fields = key_fields + STATUS_FIELD_NAMES
    stmt = text(' '.join([
        'select',
        ','.join(db_fields),
        'from',
        db_table
    ]))

    with create_engine(connection_string).connect() as engine:
        ret = KVFile()
        
        try:
            rows = engine.execute(stmt)
            for row in rows:
                rec = dict(zip(db_fields, row))
                existing_id = dict(
                    (k, v) for k, v in rec.items()
                    if k in STATUS_FIELD_NAMES
                )
                key = calc_key(rec, key_fields)
                ret.set(key, existing_id)
        except ProgrammingError:
            logging.exception('Failed to fetch existing keys')
        except OperationalError:
            logging.exception('Failed to fetch existing keys')

    return ret


def process_resource(res, key_fields, hash_fields, existing_ids, prefix, max_staleness):

    count_existing = 0
    count_modified = 0
    count_new = 0
    count_total = 0
    count_stale = 0

    ls = LineSelector()

    for i, row in enumerate(res):
        key = calc_key(row, key_fields)
        hash = calc_hash(row, hash_fields)
        count_total += 1

        debug = ls(i)

        if debug:
            logging.info('#%d: KEY: %r HASH: %r', i, key, hash)
        try:
            existing_id = existing_ids.get(key)
            row.update(existing_id)
            days_since_last_update = (now - existing_id[prefix+'__last_updated_at']).days
            days_since_last_update = max(0, days_since_last_update)
            next_update_days = existing_id[prefix+'__next_update_days']
            next_update_days = min(next_update_days, max_staleness)
            is_stale = days_since_last_update > next_update_days
            overdue = max(1, days_since_last_update - next_update_days)
            staleness = int(100000+100000/(1+overdue))
            if debug:
                logging.info('#%d: PROPS: %r', i, existing_id)
                logging.info('#%d: >> is_stale: %r, staleness: %r, next_update_days: %r',
                             i, is_stale, staleness, next_update_days)
            if is_stale:
                count_stale += 1
            row.update({
                prefix+'__is_new': False,
                prefix+'__is_stale': is_stale,
                prefix+'__staleness': staleness,
                prefix+'__last_updated_at': now,
                prefix+'__hash': hash,
            })
            if hash == existing_id[prefix+'__hash']:
                row.update({
                    prefix+'__next_update_days': days_since_last_update + 2
                })
                count_existing += 1
                # if not is_stale and days_since_last_update < 7:
                #     continue
            else:
                row.update({
                    prefix+'__last_modified_at': now,
                    prefix+'__next_update_days': 1
                })
                count_modified += 1

        except KeyError:
            row.update({
                prefix+'__is_new': True,
                prefix+'__is_stale': True,
                prefix+'__staleness': 100000,
                prefix+'__last_updated_at': now,
                prefix+'__last_modified_at': now,
                prefix+'__created_at': now,
                prefix+'__next_update_days': 1,
                prefix+'__hash': hash,
            })
            count_new += 1
        if row[prefix+'__is_stale']:
            if debug:
                logging.info('#%d>> %r', i, dict(
                    (k, v) for k, v in row.items()
                    if k.startswith(prefix + '__')
                ))
        yield row

    logging.info('MANAGE REVISION STATS:')
    logging.info('| TOTAL  : %7d', count_total)
    logging.info('| NEW    : %7d', count_new)
    logging.info('| CHANGED: %7d', count_modified)
    logging.info('| SAME   : %7d', count_existing)
    logging.info('| STALE  : %7d', count_stale)


def process_resources(res_iter, resource_name, key_fields, hash_fields, existing_ids, prefix, max_staleness):
    for res in res_iter:
        if res.spec['name'] == resource_name:
            yield process_resource(res, key_fields, hash_fields, existing_ids, prefix, max_staleness)
            break
        else:
            yield res
    yield from res_iter


def main():
    parameters, dp, res_iter = ingest()

    connection_string = get_connection_string()

    existing_ids = None
    resource_name = parameters['resource-name']
    input_key_fields = parameters['key-fields']
    input_hash_fields = parameters.get('hash-fields')
    prefix = parameters.get('prefix', '')
    max_staleness = parameters.get('max-staleness', 90)

    STATUS_FIELDS = [
        {'name': prefix+'__last_updated_at',  'type': 'datetime'},
        {'name': prefix+'__last_modified_at', 'type': 'datetime'},
        {'name': prefix+'__created_at',       'type': 'datetime'},
        {'name': prefix+'__is_new',           'type': 'boolean'},
        {'name': prefix+'__is_stale',         'type': 'boolean'},
        {'name': prefix+'__staleness',        'type': 'integer'},
        {'name': prefix+'__next_update_days', 'type': 'integer'},
        {'name': prefix+'__hash',             'type': 'string', 'es:index': False},
    ]
    STATUS_FIELD_NAMES = list(f['name'] for f in STATUS_FIELDS)

    for res in dp['resources']:
        if resource_name == res['name']:
            if input_hash_fields is None:
                input_hash_fields = set(f['name'] for f in res['schema']['fields'])
            input_hash_fields = set(input_hash_fields) - set(input_key_fields)
            if len(input_hash_fields.intersection(STATUS_FIELD_NAMES)) == 0:
                res['schema']['fields'].extend(STATUS_FIELDS)
            input_hash_fields = set(input_hash_fields) - set(STATUS_FIELD_NAMES)

            db_key_fields = parameters.get('db-key-fields', input_key_fields)
            # db_hash_fields = parameters.get('db-hash-fields', input_hash_fields)

            existing_ids = \
                get_all_existing_ids(connection_string,
                                     parameters['db-table'],
                                     db_key_fields,
                                     [
                                         prefix + '__last_updated_at',
                                         prefix + '__next_update_days',
                                         prefix + '__hash',
                                         prefix + '__created_at',
                                         prefix + '__last_modified_at',
                                     ]
                                    )
            break

    assert existing_ids is not None
    logging.info('Found %d ids', len(list(existing_ids.keys())))

    spew(dp, process_resources(res_iter,
                               resource_name,
                               input_key_fields,
                               input_hash_fields,
                               existing_ids,
                               prefix,
                               max_staleness))
    existing_ids.close()


main()
