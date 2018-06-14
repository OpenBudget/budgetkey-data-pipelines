import logging
import requests
from datapackage_pipelines_budgetkey.common.resource_filter_processor import ResourceFilterProcessor
from requests import HTTPError

DEFAULT_INPUT_RESOURCE = "tender-urls"
DEFAULT_OUTPUT_RESOURCE = "tender-urls-downloaded-data"

TABLE_SCHEMA = {"fields": [{"name": "pid", "title": "publisher id", "type": "integer"},
                           {"name": "url", "title": "page url", "type": "string"},
                           {"name": "data", "title": "page html data", "type": "string"},
                           {"name": "tender_type", "type": "string"}]}


class DownloadPagesDataProcessor(ResourceFilterProcessor):

    def __init__(self, timeout=180, url_prefix="https://www.mr.gov.il", **kwargs):
        self._timeout = timeout
        self._url_prefix = url_prefix
        self.session = requests.session()
        self.stats = {'failed-urls': 0}
        super(DownloadPagesDataProcessor, self).__init__(
            default_input_resource=DEFAULT_INPUT_RESOURCE,
            default_output_resource=DEFAULT_OUTPUT_RESOURCE,
            default_replace_resource=True,
            table_schema=TABLE_SCHEMA,
            **kwargs
        )

    def filter_resource_data(self, data, parameters):
        count = 0
        for exemption in data:
            if not exemption['__is_stale']:
                continue
            publisher_id = exemption["id"]
            url = exemption["url"]
            if count < 5000:
                try:
                    count += 1
                    yield self._get_exemption_data(publisher_id, url, exemption["tender_type"])
                except HTTPError:
                    self.stats['failed-urls'] += 1
                    logging.exception('Failed to load %s', url)

    def get_stats(self):
        return self.stats

    def _get_exemption_data(self, publisher_id, url, tender_type):
        if not url.startswith("http"):
            url = "{}{}".format(self._url_prefix, url)
        return {"pid": publisher_id,
                "url": url,
                "data": self._get_url_response_text(url, self._timeout),
                "tender_type": tender_type}

    def _get_url_response_text(self, url, timeout):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) ' +
                          'AppleWebKit/537.36 (KHTML, like Gecko) ' +
                          'Chrome/54.0.2840.87 Safari/537.36'
        }
        response = requests.get(url, timeout=timeout, headers=headers)
        response.raise_for_status()
        return response.text


if __name__ == "__main__":
    DownloadPagesDataProcessor.main()
