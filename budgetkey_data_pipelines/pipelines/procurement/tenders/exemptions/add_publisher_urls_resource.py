from budgetkey_data_pipelines.common.resource_filter_processor import ResourceFilterProcessor
from budgetkey_data_pipelines.pipelines.procurement.tenders.exemptions.exemptions_scraper import ExemptionsPublisherScraper
import os
import logging

# TODO: find a better way to have a "mock" processor
# currently this is the best way I found to do it, it allows to test this processor using the fixture tests
# this completely overrides the scraping from html pages, that's why I separated the scraping into a self-contained class
# the ExemptionsPublisherScraper has it's own unit test - that way we have complete coverage
MOCK_URLS = {10: ['/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596877',
                  '/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596879',
                  '/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596880', ],
             71: ['/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596739',
                  '/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596740',
                  '/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596741', ],
             96: ['/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596751', ],
             45: []}

def resource_filter(resource_data, parameters):
    kwargs = {
        "max_pages": int(os.environ.get("EXEMPTION_PUBLISHERS_MAX_PAGES", parameters.get("max_pages", -1)))
    }
    limit_publisher_ids = os.environ.get("EXEMPTIONS_LIMIT_PUBLISHER_IDS", None)
    if limit_publisher_ids:
        limit_publisher_ids = limit_publisher_ids.split(",")
    logging.info("limit_publisher_ids={}".format(limit_publisher_ids))
    for publisher in resource_data:
        publisher_id = publisher["id"]
        if not limit_publisher_ids or publisher_id in map(int, limit_publisher_ids):
            logging.info("processing publisher {}".format(publisher_id))
            scraper = ExemptionsPublisherScraper(publisher_id, **kwargs)
            urls = scraper.get_urls() if not parameters.get("mock") else MOCK_URLS[publisher_id]
            for url in urls:
                yield {"id": publisher_id, "url": url}

ResourceFilterProcessor(default_input_resource="publishers",
                        default_output_resource="publisher-urls",
                        default_replace_resource=True,
                        table_schema={"fields": [{"name": "id", "type": "integer", "title": "Publisher Id"},
                                                 {"name": "url", "type": "string", "title": "Exemption Url"}]},
                        resource_filter=resource_filter).spew()
