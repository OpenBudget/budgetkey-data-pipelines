from budgetkey_data_pipelines.pipelines.procurement.tenders.add_publisher_urls_resource import resource_filter


class MockPublishersScraperClass(object):

    def __init__(self, publisher_id, **kwargs):
        self.publisher_id = publisher_id

    def get_urls(self):
        return [{"id": self.publisher_id, "url": "URL1"}, {"id": self.publisher_id, "url": "URL2"}]


def test():
    # the add publisher urls processor is just a proxy for the scrapers class
    # in this test we provide a dummy scraper class - just to check that it calls it correctly
    publisher_urls = list(resource_filter([{"id": 71}, {"id": 13}],
                                          parameters={"publisher_scraper_class": MockPublishersScraperClass}))
    assert publisher_urls == [{'id': 71, 'url': {'id': 71, 'url': 'URL1'}},
                              {'id': 71, 'url': {'id': 71, 'url': 'URL2'}},
                              {'id': 13, 'url': {'id': 13, 'url': 'URL1'}},
                              {'id': 13, 'url': {'id': 13, 'url': 'URL2'}}]
