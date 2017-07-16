import os
import logging
from urllib.parse import urlparse, parse_qs

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError, ProgrammingError

from budgetkey_data_pipelines.common.resource_filter_processor import ResourceFilterProcessor
from budgetkey_data_pipelines.pipelines.procurement.tenders.add_publisher_urls_resource import \
    PUBLISHER_URLS_TABLE_SCHEMA


CHECK_EXISTING_TABLE_SCHEMA = dict(PUBLISHER_URLS_TABLE_SCHEMA,
                                   fields=PUBLISHER_URLS_TABLE_SCHEMA["fields"] + [{"name": "is_new",
                                                                                   "type": "boolean"}])


class CheckExistingProcessor(ResourceFilterProcessor):

    def __init__(self, **kwargs):
        super(CheckExistingProcessor, self).__init__(default_input_resource="tender-urls",
                                                     default_output_resource="tender-urls",
                                                     default_replace_resource=True,
                                                     table_schema=CHECK_EXISTING_TABLE_SCHEMA,
                                                     **kwargs)
        self.existing_ids = self.get_all_existing_ids()
        logging.info('Found %d ids: %r...', len(self.existing_ids), self.existing_ids[:20])

    def get_all_existing_ids(self):
        db_session = self.initialize_db_session()
        db_conn = db_session.connection()
        sql_query = text("select publication_id, tender_type from {0}".format(self.parameters['db-table']))
        try:
            return [(int(publication_id), tender_type)
                    for publication_id, tender_type
                    in db_conn.execute(sql_query)]
        except (OperationalError, ProgrammingError) as e:
            # this is probably due to the table not existing but even if there is another problem -
            # dump.to_sql will handle it. it's safe to let that processor handle the specifics of sql errors
            # (in case the problem is not table does not exist)
            logging.info(
                "OperationError in getting all existing ids, this is fine and most likely due to table not existing")
            logging.debug(str(e))
            return []

    def initialize_db_session(self):
        connection_string = os.environ.get("DPP_DB_ENGINE")
        assert connection_string is not None, \
            "Couldn't connect to DB - " \
            "Please set your '%s' environment variable" % "DPP_DB_ENGINE"
        engine = create_engine(connection_string)
        return sessionmaker(bind=engine)()

    def filter_resource_data(self, data, parameters):
        for row in data:
            row["is_new"] = self.is_new_exemption_url(row["url"], row["tender_type"])
            yield row

    def is_new_exemption_url(self, url, tender_type):
        exemption_id = parse_qs(urlparse(url).query)["pID"][0]
        return (int(exemption_id), tender_type) not in self.existing_ids


if __name__ == "__main__":
    CheckExistingProcessor().spew()
