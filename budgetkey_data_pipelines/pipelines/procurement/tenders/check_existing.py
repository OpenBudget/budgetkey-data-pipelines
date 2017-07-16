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


def publication_id_from_url(url):
    return int(parse_qs(urlparse(url).query)["pID"][0])

def tender_id_from_url(url):
    tmp = urlparse(url).path.split("/")
    tender_num, ext = tmp[-1].split(".")
    tender_group = tmp[-3]
    tender_id = "{}-{}".format(tender_group, tender_num)
    if ext != "aspx":
        raise Exception("invalid url {}".format(url))
    else:
        return tender_id


class CheckExistingProcessor(ResourceFilterProcessor):

    def __init__(self, **kwargs):
        super(CheckExistingProcessor, self).__init__(default_input_resource="tender-urls",
                                                     default_output_resource="tender-urls",
                                                     default_replace_resource=True,
                                                     table_schema=CHECK_EXISTING_TABLE_SCHEMA,
                                                     **kwargs)
        self.central_tender_ids = []
        self.office_publication_ids = []
        self.exemption_publication_ids = []
        for publication_id, tender_type, tender_id in self.get_all_existing_ids():
            if tender_type == "central":
                self.central_tender_ids.append(tender_id)
            elif tender_type == "exemptions":
                self.exemption_publication_ids.append(publication_id)
            elif tender_type == "office":
                self.office_publication_ids.append(publication_id)
            else:
                raise Exception("Invalid tender_type: {}".format(tender_type))
        logging.info("Found %d central tender ids: %r...", len(self.central_tender_ids), self.central_tender_ids[:20])
        logging.info("Found %d office publication ids: %r...", len(self.office_publication_ids), self.office_publication_ids[:20])
        logging.info("Found %d exemption publication ids: %r...", len(self.exemption_publication_ids), self.exemption_publication_ids[:20])

    def get_all_existing_ids(self):
        db_session = self.initialize_db_session()
        db_conn = db_session.connection()
        sql_query = text("select publication_id, tender_type, tender_id from {0}".format(self.parameters['db-table']))
        try:
            return [(int(publication_id), tender_type, tender_id)
                    for publication_id, tender_type, tender_id
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
        if tender_type == "office":
            return publication_id_from_url(url) not in self.office_publication_ids
        elif tender_type == "exemptions":
            return publication_id_from_url(url) not in self.exemption_publication_ids
        elif tender_type == "central":
            return tender_id_from_url(url) not in self.central_tender_ids
        else:
            raise Exception("unknown tender_type {}".format(tender_type))


if __name__ == "__main__":
    CheckExistingProcessor().spew()
