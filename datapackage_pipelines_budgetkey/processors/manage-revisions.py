import os
import logging
import datetime
import hashlib

from tableschema_sql.storage import Storage

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.kvstore import DB

from sqlalchemy import create_engine

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


def get_all_existing_ids(connection_string, db_table, key_fields, hash_fields, array_fields, STATUS_FIELD_NAMES):
    ret = DB()
    storage = Storage(create_engine(connection_string))

    if db_table in storage.buckets:
        descriptor = storage.describe(db_table)
        for field in descriptor['fields']:
            if field['name'] in array_fields:
                field['type'] = 'array'
        storage.describe(db_table, descriptor)
        db_fields = [f['name'] for f in descriptor['fields']]
        for rec in storage.iter(db_table):
            rec = dict(zip(db_fields, rec))
            existing_id = dict(
                (k, v) for k, v in rec.items()
                if k in STATUS_FIELD_NAMES
            )
            key = calc_key(rec, key_fields)
            ret.set(key, existing_id)

    return ret


def process_resource(res, key_fields, hash_fields, existing_ids, prefix):
    for row in res:
        key = calc_key(row, key_fields)
        hash = calc_hash(row, hash_fields)

        try:
            existing_id = existing_ids.get(key)
            days_since_last_update = (now - existing_id[prefix+'__last_updated_at']).days
            is_stale = days_since_last_update > existing_id[prefix+'__next_update_days']
            row.update({
                prefix+'__is_new': False,
                prefix+'__is_stale': is_stale,
                prefix+'__last_updated_at': now,
                prefix+'__hash': hash,
            })
            if hash == existing_id[prefix+'__hash']:
                row.update({
                    prefix+'__next_update_days': days_since_last_update + 2
                })
            else:
                row.update({
                    prefix+'__last_modified_at': now,
                    prefix+'__next_update_days': 1
                })

        except KeyError:
            row.update({
                prefix+'__is_new': True,
                prefix+'__is_stale': True,
                prefix+'__last_updated_at': now,
                prefix+'__last_modified_at': now,
                prefix+'__next_update_days': 1,
                prefix+'__hash': hash,
            })

        yield row


def process_resources(res_iter, resource_name, key_fields, hash_fields, existing_ids, prefix):
    for res in res_iter:
        if res.spec['name'] == resource_name:
            yield process_resource(res, key_fields, hash_fields, existing_ids, prefix)
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
    array_fields = parameters.get('array-fields', [])
    prefix = parameters.get('prefix', '')

    STATUS_FIELDS = [
        {'name': prefix+'__last_updated_at',  'type': 'datetime'},
        {'name': prefix+'__last_modified_at', 'type': 'datetime'},
        {'name': prefix+'__is_new',           'type': 'boolean'},
        {'name': prefix+'__is_stale',         'type': 'boolean'},
        {'name': prefix+'__next_update_days', 'type': 'integer'},
        {'name': prefix+'__hash',             'type': 'string'},
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
            db_hash_fields = parameters.get('db-hash-fields', input_hash_fields)

            schema_array_fields = [
                field['name']
                for field in res['schema']['fields']
                if field['type'] == 'array'
            ]

            existing_ids = \
                get_all_existing_ids(connection_string,
                                     parameters['db-table'],
                                     db_key_fields,
                                     db_hash_fields,
                                     array_fields + schema_array_fields,
                                     STATUS_FIELD_NAMES)
            break

    assert existing_ids is not None
    logging.info('Found %d ids', len(list(existing_ids.keys())))

    spew(dp, process_resources(res_iter,
                               resource_name,
                               input_key_fields,
                               input_hash_fields,
                               existing_ids,
                               prefix))


main()
