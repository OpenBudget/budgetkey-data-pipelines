from budgetkey_data_pipelines.pipelines.procurement.tenders.exemptions.exemptions_scraper import ExemptionsPublisherScraper, TooManyFailuresException
import os
import json
from requests.exceptions import HTTPError, ConnectionError
from itertools import islice


class MockExemptionsPublisherScraper(ExemptionsPublisherScraper):
    """
    opens files from local filesystem instead of from the source
    """

    def __init__(self, publisher_id, write_prefix=None, mock_http_failures=0, **kwargs):
        self.write_prefix = write_prefix
        self._num_requests = {}
        self._mock_http_failures = mock_http_failures
        kwargs.setdefault("wait_between_retries", 0.001)
        super(MockExemptionsPublisherScraper, self).__init__(publisher_id, **kwargs)

    def _get_page_text(self, form_data=None):
        if form_data:
            filename = "SearchExemptionMessages.aspx.publisher{}-page{}".format(self._publisher_id, self._cur_page_num)
        else:
            filename = "SearchExemptionMessages.aspx"
        if filename not in self._num_requests:
            self._num_requests[filename] = 0
        self._num_requests[filename] += 1
        if self._mock_http_failures < self._num_requests[filename]:
            if self.write_prefix:
                with open(os.path.join(os.path.dirname(__file__), "{}{}".format(self.write_prefix, filename)), "w") as f:
                    if form_data:
                        real_text = super(MockExemptionsPublisherScraper, self)._get_page_text(form_data)
                        f.write(json.dumps(form_data))
                        f.write("\n\n")
                        f.write(real_text)
                    else:
                        real_text = super(MockExemptionsPublisherScraper, self)._get_page_text()
                        f.write(real_text)
                    return real_text
            else:
                with open(os.path.join(os.path.dirname(__file__), filename)) as f:
                    return f.read()
        elif self._num_requests[filename] == 1:
            raise ConnectionError("fake connecion error")
        else:
            raise HTTPError("fake http error")

class MockExemptionsPublisherScraperUnlimitedPages(ExemptionsPublisherScraper):

    def __init__(self, **kwargs):
        super(MockExemptionsPublisherScraperUnlimitedPages, self).__init__(10, **kwargs)

    def _get_page_text(self, form_data=None):
        if form_data:
            filename = "SearchExemptionMessages.aspx.publisher10-page1"
        else:
            filename = "SearchExemptionMessages.aspx"
        with open(os.path.join(os.path.dirname(__file__), filename)) as f:
            return f.read()

    def _get_num_pages(self):
        return self._cur_page_num + 1

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

def test_retries():
    # mock_http_failures parameter causes every http request to fail 3 times and succeed on the fourth time
    scraper = MockExemptionsPublisherScraper(10, mock_http_failures=3)
    # it works due to retry mechanism
    assert_publisher_10_urls(list(scraper.get_urls()))

def test_retries_too_many_failures():
    # but - if we have too many failures (by default - more then 10)
    try:
        list(MockExemptionsPublisherScraper(10, mock_http_failures=11).get_urls())
        got_exception = False
    except TooManyFailuresException:
        got_exception = True
    # then we get a TooManyFailuresException
    assert got_exception

def test_max_pages():
    # 1 page = 10 results
    assert_max_pages(max_pages=1, num_expected_results=10)
    assert_max_pages(max_pages=2, num_expected_results=20)
    # the scraper yields unlimited results
    assert_max_pages(max_pages=6, num_expected_results=60)
    # 0 or negative number - will yield all pages (in the assertion we islice it to 300)
    assert_max_pages(max_pages=0, num_expected_results=300)
    assert_max_pages(max_pages=-1, num_expected_results=300)

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

def assert_max_pages(max_pages, num_expected_results):
    scraper = MockExemptionsPublisherScraperUnlimitedPages(max_pages=max_pages)
    # the scraper will yield unlimited results, we stop at 300 results
    urls = islice(scraper.get_urls(), 300)
    assert sum(1 for _ in urls) == num_expected_results
