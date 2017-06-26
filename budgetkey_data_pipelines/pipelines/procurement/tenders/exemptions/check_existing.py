import os
import logging
from urllib.parse import urlparse, parse_qs

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, ProgrammingError

from datapackage_pipelines.wrapper import ingest, spew


def initialize_db_session():
    connection_string = os.environ.get("DPP_DB_ENGINE")
    assert connection_string is not None, \
        "Couldn't connect to DB - " \
        "Please set your '%s' environment variable" % "DPP_DB_ENGINE"
    engine = create_engine(connection_string)
    return sessionmaker(bind=engine)()


def get_all_existing_ids(db_session, db_table):
    try:
        return [o[0] for o in db_session.query("publication_id from {0}".format(db_table))]
    except (OperationalError, ProgrammingError) as e:
        # this is probably due to the table not existing but even if there is another problem -
        # dump.to_sql will handle it. it's safe to let that processor handle the specifics of sql errors
        # (in case the problem is not table does not exist)
        logging.info("OperationError in getting all existing ids, this is fine and most likely due to table not existing")
        logging.debug(str(e))
        return []


def is_new_exemption_url(url, existing_ids):
    exemption_id = parse_qs(urlparse(url).query)["pID"][0]
    return int(exemption_id) not in existing_ids


def process_resource(res, existing_ids):
    for row in res:
        row['is_new'] = is_new_exemption_url(row['url'], existing_ids)
        yield row


def process_resources(res_iter, existing_ids):
    first = next(res_iter)
    yield process_resource(first, existing_ids)
    yield from res_iter


def main():
    parameters, dp, res_iter = ingest()

    # initialize a database session, which we will use later to fetch all existing ids
    db_session = initialize_db_session()
    existing_ids = get_all_existing_ids(db_session, parameters['db-table'])
    logging.info('Found %d ids: %r...', len(existing_ids), existing_ids[:20])

    dp['resources'][0]['schema']['fields'].append({
        'name': 'is_new',
        'type': 'boolean'
    })
    spew(dp, process_resources(res_iter, existing_ids))


main()
