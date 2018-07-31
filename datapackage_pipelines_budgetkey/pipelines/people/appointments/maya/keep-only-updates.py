import logging
import os
from datapackage_pipelines.wrapper import process
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, ProgrammingError



def get_connection_string():
    connection_string = os.environ.get("DPP_DB_ENGINE")
    assert connection_string is not None, \
        "Couldn't connect to DB - " \
        "Please set your '%s' environment variable" % "DPP_DB_ENGINE"
    return connection_string


def get_all_existing_ids():
    engine = create_engine(get_connection_string())
    ret = set()
    try:
        rows = engine.execute("SELECT s3_object_name FROM maya_notification_list")
        ret = set([row['s3_object_name'] for row in rows])
    except (OperationalError, ProgrammingError):
        #ProgrammingError is for postgres and OperationError is on sqlite
        logging.warning('Failed to fetch table maya_notification_list')
    return ret


all_existing_ids = get_all_existing_ids()


def process_row(row, *_):
    s3_object_name = row['s3_object_name']
    if s3_object_name in all_existing_ids:
        return None
    return row


process(process_row=process_row)

