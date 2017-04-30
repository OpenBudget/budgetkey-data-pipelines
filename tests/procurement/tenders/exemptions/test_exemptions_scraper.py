from budgetkey_data_pipelines.pipelines.procurement.tenders.exemptions.exemptions_scraper import ExemptionsPublisherScraper
import os
import json


class MockExemptionsPublisherScraper(ExemptionsPublisherScraper):
    """
    opens files from local filesystem instead of from the source
    """

    def __init__(self, publisher_id, write_prefix=None):
        self.write_prefix = write_prefix
        super(MockExemptionsPublisherScraper, self).__init__(publisher_id)

    def _get_search_page_text(self):
        filename = "SearchExemptionMessages.aspx"
        if self.write_prefix:
            with open(os.path.join(os.path.dirname(__file__), "{}{}".format(self.write_prefix, filename)), "w") as f:
                real_text = super(MockExemptionsPublisherScraper, self)._get_search_page_text()
                f.write(real_text)
                return real_text
        else:
            with open(os.path.join(os.path.dirname(__file__), filename)) as f:
                return f.read()

    def _get_post_page_text(self, form_data):
        filename = "SearchExemptionMessages.aspx.publisher{}-page{}".format(self._publisher_id, self._cur_page_num)
        if self.write_prefix:
            with open(os.path.join(os.path.dirname(__file__), "{}{}".format(self.write_prefix, filename)), "w") as f:
                real_text = super(MockExemptionsPublisherScraper, self)._get_post_page_text(form_data)
                f.write(json.dumps(form_data))
                f.write("\n\n")
                f.write(real_text)
                return real_text
        else:
            with open(os.path.join(os.path.dirname(__file__), filename)) as f:
                return f.read()

def test():
    # 10 =  המשרד לביטחון פנים - משטרת ישראל
    actual_urls = list(MockExemptionsPublisherScraper(10).get_urls())
    assert_publisher_10_urls(actual_urls)

# this test is skipped because it does real conncetion to gov website
# it can be used locally to regenerate the mock files or to test functionality of the real website
def skip_test_no_mock():
    # 10 =  המשרד לביטחון פנים - משטרת ישראל
    actual_urls = []
    # gets all the exemptions, wait until the first one in the mock test, then get 12 urls and stop
    for url in MockExemptionsPublisherScraper(10, "dump_").get_urls():
        is_first_url = url == "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596877"
        if is_first_url or (len(actual_urls) > 0 and len(actual_urls) <= 12):
            actual_urls.append(url)
        elif len(actual_urls) >= 12:
            break
    assert_publisher_10_urls(actual_urls)

def assert_publisher_10_urls(actual_urls):
    assert actual_urls == [
        '/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596877',
        '/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596879',
        '/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596880',
        '/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596739',
        '/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596740',
        '/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596741',
        '/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596751',
        '/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596752',
        '/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596753',
        "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596755",
        '/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596686',
        '/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596700',
        '/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=596714'
    ]
