from budgetkey_data_pipelines.pipelines.procurement.tenders.add_publisher_urls_resource import resource_filter
import os


class MockPublishersScraperClass(object):

    def __init__(self, publisher_id, **kwargs):
        self.publisher_id = publisher_id
        self.tender_type = kwargs.get("tender_type")

    def get_urls(self):
        return ["URL1-{}".format(self.tender_type),
                "URL2-{}".format(self.tender_type)]


def reset_environ():
    if "OVERRIDE_PUBLISHER_URLS_MAX_PAGES" in os.environ: del os.environ["OVERRIDE_PUBLISHER_URLS_MAX_PAGES"]
    if "OVERRIDE_PUBLISHER_URLS_LIMIT_PUBLISHER_IDS" in os.environ: del os.environ[
        "OVERRIDE_PUBLISHER_URLS_LIMIT_PUBLISHER_IDS"]


def test_exemptions():
    reset_environ()
    # the add publisher urls processor is just a proxy for the scrapers class
    # in this test we provide a dummy scraper class - just to check that it calls it correctly
    publisher_urls = list(resource_filter([{"id": 71}, {"id": 13}],
                                          parameters={"publisher_scraper_class": MockPublishersScraperClass,
                                                      "tender_type": "exemptions"}))
    assert publisher_urls == [{'id': 71, 'url': 'URL1-exemptions', 'tender_type': 'exemptions'},
                              {'id': 71, 'url': 'URL2-exemptions', 'tender_type': 'exemptions'},
                              {'id': 13, 'url': 'URL1-exemptions', 'tender_type': 'exemptions'},
                              {'id': 13, 'url': 'URL2-exemptions', 'tender_type': 'exemptions'}]

def test_office():
    reset_environ()
    # the add publisher urls processor is just a proxy for the scrapers class
    # in this test we provide a dummy scraper class - just to check that it calls it correctly
    publisher_urls = list(resource_filter([{"id": 71}, {"id": 13}],
                                          parameters={"publisher_scraper_class": MockPublishersScraperClass,
                                                      "tender_type": "office"}))
    assert publisher_urls == [{'id': 71, 'url': 'URL1-office', 'tender_type': 'office'},
                              {'id': 71, 'url': 'URL2-office', 'tender_type': 'office'},
                              {'id': 13, 'url': 'URL1-office', 'tender_type': 'office'},
                              {'id': 13, 'url': 'URL2-office', 'tender_type': 'office'}]
