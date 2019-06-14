
from dataflows import Flow, printer, update_resource, set_primary_key, load, add_field
import logging
import os

import time
import requests
from pyquery import PyQuery as pq

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError
from datapackage_pipelines_budgetkey.common.object_storage import object_storage
from datapackage_pipelines_budgetkey.pipelines.maya.maya_parser import PARSER_VERSION, parse_maya_html_form

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_connection_string():
    connection_string = os.environ.get("DATAFLOWS_DB_ENGINE", default=None)
    assert connection_string is not None, \
        "Couldn't connect to DB - " \
        "Please set your '%s' environment variable" % "DATAFLOWS_DB_ENGINE"
    return connection_string


def get_all_existing_ids():
    engine = create_engine(get_connection_string())
    ret = set()
    try:
        rows = engine.execute(text("SELECT s3_object_name FROM maya_notifications where parser_version=:v"), v=PARSER_VERSION)
        ret.update(row['s3_object_name'] for row in rows)
    except (OperationalError, ProgrammingError) as e:
        #ProgrammingError is for postgres and OperationError is on sqlite
        logging.warning('Failed to fetch table maya_notifications',e)
    return ret






###
#  The tase web site may encode files either in utf-8 or in windows-1255
#  however it never returns ContentEncoding header in the response. And although the page includes
#  a meta tag in the header with a content Type the value there is always windows-1255 regardless
#  of the actualy encoding in the file

#  The ugly solution is simply to try
###
def get_charset(conn, default="windows-1255"):
    try:
        #If utf-8 file then parsing will find the element and it should contain the an aleph.
        #in case of windows-1255 the content will be junk or impossible to find
        conn.encoding = "utf-8"
        if 'א' in pq(pq(conn.text).find("#HeaderProof")[0]).text():
            return "utf-8"
    except:
        pass
    return default


def store_on_s3(rows):
    session = requests.Session()
    for row in rows:
        s3_object_name = row['s3_object_name']

        if not object_storage.exists(s3_object_name):
            url = row['url']
            conn = session.get(url)
            time.sleep(3)
            if not conn.status_code == requests.codes.ok:
                continue

            charset = get_charset(conn)
            conn.encode = charset
            object_storage.write(s3_object_name,
                                 data=conn.content,
                                 public_bucket=True,
                                 create_bucket=True,
                                 content_type="text/html; charset={}".format(charset))
        yield row


def parse_notification(rows):
    for row in rows:
        row.update(parse_maya_html_form(object_storage.urlfor(row['s3_object_name'])))
        row['parser_version'] = PARSER_VERSION
        yield row


def remove_already_parsed(rows):
    all_existing_ids = get_all_existing_ids()

    for row in rows:
        s3_object_name = row['s3_object_name']
        if not s3_object_name in all_existing_ids:
            yield row

def flow(*_):

    return Flow(
        remove_already_parsed,
        store_on_s3,
        add_field('parser_version', 'number'),
        add_field('id', 'string'),
        add_field('company', 'string'),
        add_field('type', 'string'),
        add_field('fix_for', 'string'),
        add_field('document', 'object'),
        set_primary_key(['s3_object_name']),
        parse_notification,

        update_resource(
            -1, name='maya_notification_parse_updates', path="data/maya_notification_parse_updates.csv",
        ),
    )


if __name__ == '__main__':
    Flow(
        load('/var/datapackages/maya/scrape_maya_notification_list/datapackage.json'),
        flow(),
        printer(),
    ).process()
