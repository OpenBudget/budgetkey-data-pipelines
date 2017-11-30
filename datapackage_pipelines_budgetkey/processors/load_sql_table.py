from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew
from itertools import chain
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData
import os, logging


parameters, datapackage, resources = ingest()


def initialize_db_session():
    connection_string = os.environ.get("DPP_DB_ENGINE")
    assert connection_string is not None, \
        "Couldn't connect to DB - " \
        "Please set your '%s' environment variable" % "DPP_DB_ENGINE"
    engine = create_engine(connection_string)
    return sessionmaker(bind=engine)()


def get_resource():
    session = initialize_db_session()
    meta = MetaData(bind=session.connection())
    meta.reflect()
    table = meta.tables.get(parameters["table"])
    if table is None:
        logging.warning("table does not exist '{}'".format(parameters["table"]))
    else:
        query_args = []
        for column_name, column_params in parameters["columns"].items():
            if not hasattr(table.c, column_name):
                raise Exception("missing column in table: {} / {}".format(parameters["table"], column_name))
            else:
                query_args.append(getattr(table.c, column_name))
        for i, row in enumerate(session.query(*query_args).yield_per(1000)):
            if i % 1000 == 0:
                logging.info('LOADED %d ROWS', i)
            yield {column_name: getattr(row, column_name)
                   for column_name, column_params
                   in parameters["columns"].items()}


def get_schema():
    fields = []
    for column_name, column_params in parameters["columns"].items():
        fields.append({"name": column_name, "type": "string"})
    return {"fields": fields}


datapackage["resources"].append({"name": parameters["table"],
                                 "path": parameters["table"]+".csv",
                                 "schema": get_schema(),
                                 PROP_STREAMING: True})


spew(datapackage, chain(resources, [get_resource()]))
