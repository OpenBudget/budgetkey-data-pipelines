from datapackage_pipelines_budgetkey.common.resource_filter_processor import ResourceFilterProcessor
from datapackage_pipelines_budgetkey.pipelines.procurement.tenders.publishers_scraper import PublisherScraper
import os
import logging

from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError, OperationalError

PUBLISHER_URLS_TABLE_SCHEMA = {"fields": [{"name": "id", "type": "integer", "title": "Publisher Id"},
                                          {"name": "url", "type": "string", "title": "Page Url"},
                                          {"name": "tender_type", "type": "string", "enum": ["office", "exemptions"]}]}


def resource_filter(resource_data, parameters):
    existing_ids = set()
    if 'db-table' in parameters:
        db_table = parameters.pop('db-table')
        connection_string = os.environ['DPP_DB_ENGINE'].replace('@postgres', '@data-next.obudget.org')
        engine = create_engine(connection_string).connect()
        try:
            existing_ids = set([
                r[0] for r in 
                engine.execute('select distinct url from %s' % db_table)
            ])
            logging.info('GOT %d existing URLs', len(existing_ids))
        except:
            logging.exception('Failed to fetch existing ids')

    if os.environ.get("OVERRIDE_PUBLISHER_URLS_MAX_PAGES", "") != "":
        max_pages = os.environ["OVERRIDE_PUBLISHER_URLS_MAX_PAGES"]
    else:
        max_pages = parameters.get("max_pages", "-1")
    publisher_scraper_kwargs = {
        "max_pages": int(max_pages),
        "tender_type": parameters["tender_type"],
        "max_retries": 1
    }
    if os.environ.get("OVERRIDE_PUBLISHER_URLS_LIMIT_PUBLISHER_IDS", "") != "":
        limit_publisher_ids = map(int, os.environ["OVERRIDE_PUBLISHER_URLS_LIMIT_PUBLISHER_IDS"].split(","))
    else:
        limit_publisher_ids = None
    publisher_scraper_class = parameters.get("publisher_scraper_class", PublisherScraper)
    for publisher in resource_data:
        publisher_id = publisher["id"]
        if not limit_publisher_ids or publisher_id in limit_publisher_ids:
            logging.info("processing publisher {}".format(publisher_id))
            scraper = publisher_scraper_class(publisher_id, **publisher_scraper_kwargs)
            urls = scraper.get_urls()
            for url in urls:
                yield {"id": publisher_id, "url": url, "tender_type": parameters["tender_type"]}


if __name__ == "__main__":
    ResourceFilterProcessor(default_input_resource="publishers",
                            default_output_resource="publisher-urls",
                            default_replace_resource=True,
                            table_schema=PUBLISHER_URLS_TABLE_SCHEMA,
                            resource_filter=resource_filter).spew()
