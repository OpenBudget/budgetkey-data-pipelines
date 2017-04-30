from budgetkey_data_pipelines.pipelines.procurement.tenders.exemptions.exemptions_scraper import ExemptionsPublisherScraper
import os


class MockExemptionsPublisherScraper(ExemptionsPublisherScraper):

    def _get_search_page_text(self):
        with open(os.path.join(os.path.dirname(__file__), "SearchExemptionMessages.aspx")) as f:
            return f.read()

    def _get_post_page_text(self, form_data):
        publisher_id = form_data["ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$ddlPublisher"]
        with open(os.path.join(os.path.dirname(__file__), "SearchExemptionMessages.aspx.publisher{}".format(publisher_id))) as f:
            return f.read()

def test():
    scraper = MockExemptionsPublisherScraper(10)
    actual = [url for url in scraper.get_urls()]
    expected = []
    assert actual == expected
