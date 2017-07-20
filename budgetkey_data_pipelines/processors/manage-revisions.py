import os
import logging
import datetime
import hashlib

from jsontableschema_sql.storage import Storage

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.kvstore import DB

from sqlalchemy import create_engine

now = datetime.datetime.now()
STATUS_FIELDS = [
    {'name': '__last_updated_at',  'type': 'datetime'},
    {'name': '__last_modified_at', 'type': 'datetime'},
    {'name': '__is_new',           'type': 'boolean'},
    {'name': '__is_stale',         'type': 'boolean'},
    {'name': '__next_update_days', 'type': 'integer'}
]
STATUS_FIELD_NAMES = list(f['name'] for f in STATUS_FIELDS)


def get_connection_string():
    connection_string = os.environ.get("DPP_DB_ENGINE")
    assert connection_string is not None, \
        "Couldn't connect to DB - " \
        "Please set your '%s' environment variable" % "DPP_DB_ENGINE"
    return connection_string


def calc_key_and_hash(row, key_fields, hash_fields):
    key = '|'.join(str(row[k]) for k in key_fields)
    hash = '|'.join(str(row[k]) for k in hash_fields)
    if len(hash) > 0:
        hash = hashlib.md5(hash.encode('utf8')).hexdigest()
    return key, hash


def get_all_existing_ids(connection_string, db_table, key_fields, hash_fields):
    ret = DB()
    storage = Storage(create_engine(connection_string))

    if db_table in storage.buckets:
        descriptor = storage.describe(db_table)
        db_fields = [f['name'] for f in descriptor['fields']]
        for rec in storage.iter(db_table):
            rec = dict(zip(db_fields, rec))
            existing_id = dict(
                (k, v) for k, v in rec.items()
                if k in STATUS_FIELD_NAMES
            )
            key, hash = calc_key_and_hash(rec, key_fields, hash_fields)
            existing_id['__hash'] = hash
            ret.set(key, existing_id)

    return ret


def process_resource(res, key_fields, hash_fields, existing_ids):
    for row in res:
        key, hash = calc_key_and_hash(row, key_fields, hash_fields)

        try:
            existing_hash = existing_ids.get(key)
            days_since_last_update = (now - existing_hash['__last_updated_at']).days
            is_stale = days_since_last_update > existing_hash['__next_update_days']
            row.update({
                '__is_new': False,
                '__is_stale': is_stale,
                '__last_updated_at': now,
            })
            if hash == existing_hash:
                row.update({
                    '__next_update_days': days_since_last_update + 1
                })
            else:
                row.update({
                    '__last_modified_at': now,
                    '__next_update_days': 1
                })

        except KeyError:
            row.update({
                '__is_new': True,
                '__is_stale': True,
                '__last_updated_at': now,
                '__last_modified_at': now,
                '__next_update_days': 1
            })

        yield row


def process_resources(res_iter, resource_name, key_fields, hash_fields, existing_ids):
    for res in res_iter:
        if res.spec['name'] == resource_name:
            yield process_resource(res, key_fields, hash_fields, existing_ids)
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

    for res in dp['resources']:
        if resource_name == res['name']:
            if input_hash_fields is None:
                input_hash_fields = set(f['name'] for f in res['schema']['fields'])
            input_hash_fields = set(input_hash_fields) - set(input_key_fields)
            if len(input_hash_fields.intersection(STATUS_FIELD_NAMES)) == 0:
                res['schema']['fields'].extend(STATUS_FIELDS)

            db_key_fields = parameters.get('db-key-fields', input_key_fields)
            db_hash_fields = parameters.get('db-hash-fields', input_hash_fields)

            existing_ids = \
                get_all_existing_ids(connection_string,
                                     parameters['db-table'],
                                     db_key_fields,
                                     db_hash_fields)
            break

    assert existing_ids is not None
    logging.info('Found %d ids', len(list(existing_ids.keys())))

    spew(dp, process_resources(res_iter,
                               resource_name,
                               input_key_fields,
                               input_hash_fields,
                               existing_ids))


main()
