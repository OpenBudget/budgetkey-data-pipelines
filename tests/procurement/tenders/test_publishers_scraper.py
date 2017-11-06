import os
from datapackage_pipelines_budgetkey.pipelines.procurement.tenders.publishers_scraper import PublisherScraper


class MockPublishersScraper(PublisherScraper):

    def _get_page_text(self, form_data=None):
        file_name = "publisher_{}_scraper_{}_page_{}".format(self._publisher_id, self._tender_type, getattr(self, "_cur_page_num", None))
        fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures")
        page_text_file = os.path.join(fixtures_dir, file_name)
        if not os.path.exists(page_text_file):
            with open(page_text_file, "w", encoding="utf-8") as f:
                f.write(super(MockPublishersScraper, self)._get_page_text(form_data))
        with open(page_text_file, encoding="utf-8") as f:
            return f.read()


def test_exemptions():
    scraper = MockPublishersScraper(publisher_id=119)
    urls = list(scraper.get_urls())
    assert len(urls) == 15
    assert urls[0] == "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=599916"
    assert urls[14] == "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=586429"


def test_office():
    scraper = MockPublishersScraper(publisher_id=96, tender_type="office")
    urls = list(scraper.get_urls())
    assert len(urls) == 23
    assert urls[0] == "/officestenders/Pages/officetender.aspx?pID=598495"
    assert urls[22] == "/officestenders/Pages/officetender.aspx?pID=505193"
