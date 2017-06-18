import requests
import os
import logging
from urllib.parse import urlparse, parse_qs
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import OperationalError
from budgetkey_data_pipelines.common.resource_filter_processor import ResourceFilterProcessor


DEFAULT_INPUT_RESOURCE = "publisher-urls"
DEFAULT_OUTPUT_RESOURCE = "publisher-urls-downloaded-data"

TABLE_SCHEMA = {"fields": [{"name": "pid", "title": "publisher id", "type": "integer"},
                           {"name": "url", "title": "exemption page url", "type": "string"},
                           {"name": "data", "title": "exemption page html data", "type": "string"}]}


class DownloadExemptionPagesDataProcessor(ResourceFilterProcessor):

    def __init__(self, timeout=180, url_prefix="http://www.mr.gov.il", **kwargs):
        self._timeout = timeout
        self._url_prefix = url_prefix
        super(DownloadExemptionPagesDataProcessor, self).__init__(
            default_input_resource=DEFAULT_INPUT_RESOURCE,
            default_output_resource=DEFAULT_OUTPUT_RESOURCE,
            default_replace_resource=True,
            table_schema=TABLE_SCHEMA,
            **kwargs
        )
        if "db-table" in self.parameters:
            # initialize a database session, which we will use later to fetch all existing ids
            self._initialize_db_session()

    def filter_resource_data(self, data, parameters):
        for exemption in data:
            publisher_id = exemption["id"]
            url = exemption["url"]
            if self._is_new_exemption_url(url):
                yield self._get_exemption_data(publisher_id, url)

    def _is_new_exemption_url(self, url):
        exemption_id = self._get_exemption_id(url)
        return self._is_new_exemption_id(exemption_id)

    def _get_exemption_id(self, url):
        return parse_qs(urlparse(url).query)["pID"][0]

    def _initialize_db_session(self):
        self.db_session = sessionmaker(bind=create_engine(os.environ.get("DPP_DB_ENGINE")))()

    def _get_all_existing_ids(self, db_table):
        try:
            return [o[0] for o in self.db_session.query("publication_id from {0}".format(db_table))]
        except OperationalError as e:
            # this is probably due to the table not existing but even if there is another problem -
            # dump.to_sql will handle it. it's safe to let that processor handle the specifics of sql errors
            # (in case the problem is not table does not exist)
            logging.info("OperationError in getting all existing ids, this is fine and most likely due to table not existing")
            logging.debug(str(e))
            return []

    def _is_new_exemption_id(self, id):
        if "db-table" in self.parameters:
            if not hasattr(self, "all_existing_ids"):
                self.all_existing_ids = self._get_all_existing_ids(self.parameters["db-table"])
                self.engine = create_engine(os.environ.get("DPP_DB_ENGINE"))
            return int(id) not in self.all_existing_ids
        else:
            return True

    def _get_exemption_data(self, publisher_id, url):
        url = "{}{}".format(self._url_prefix, url)
        return {"pid": publisher_id,
                "url": url,
                "data": self._get_url_response_text(url, self._timeout)}

    def _get_url_response_text(self, url, timeout):
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.text


if __name__ == "__main__":
    DownloadExemptionPagesDataProcessor.main()
