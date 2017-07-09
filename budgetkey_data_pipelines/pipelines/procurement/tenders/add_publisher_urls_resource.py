from budgetkey_data_pipelines.common.resource_filter_processor import ResourceFilterProcessor
from budgetkey_data_pipelines.pipelines.procurement.tenders.publishers_scraper import PublisherScraper
import os
import logging


def resource_filter(resource_data, parameters):
    if os.environ.get("OVERRIDE_PUBLISHER_URLS_MAX_PAGES", "") != "":
        max_pages = os.environ["OVERRIDE_PUBLISHER_URLS_MAX_PAGES"]
    else:
        max_pages = parameters.get("max_pages", "-1")
    publisher_scraper_kwargs = {
        "max_pages": int(max_pages)
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
                yield {"id": publisher_id, "url": url}


if __name__ == "__main__":
    ResourceFilterProcessor(default_input_resource="publishers",
                            default_output_resource="publisher-urls",
                            default_replace_resource=True,
                            table_schema={"fields": [{"name": "id", "type": "integer", "title": "Publisher Id"},
                                                     {"name": "url", "type": "string", "title": "Exemption Url"}]},
                            resource_filter=resource_filter).spew()
