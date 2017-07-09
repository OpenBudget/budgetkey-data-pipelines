import requests
from budgetkey_data_pipelines.common.resource_filter_processor import ResourceFilterProcessor


DEFAULT_INPUT_RESOURCE = "tender-urls"
DEFAULT_OUTPUT_RESOURCE = "tender-urls-downloaded-data"

TABLE_SCHEMA = {"fields": [{"name": "pid", "title": "publisher id", "type": "integer"},
                           {"name": "url", "title": "page url", "type": "string"},
                           {"name": "data", "title": "page html data", "type": "string"},
                           {"name": "tender_type", "type": "string"}]}


class DownloadPagesDataProcessor(ResourceFilterProcessor):

    def __init__(self, timeout=180, url_prefix="http://www.mr.gov.il", **kwargs):
        self._timeout = timeout
        self._url_prefix = url_prefix
        super(DownloadPagesDataProcessor, self).__init__(
            default_input_resource=DEFAULT_INPUT_RESOURCE,
            default_output_resource=DEFAULT_OUTPUT_RESOURCE,
            default_replace_resource=True,
            table_schema=TABLE_SCHEMA,
            **kwargs
        )

    def filter_resource_data(self, data, parameters):
        for exemption in data:
            publisher_id = exemption["id"]
            url = exemption["url"]
            if exemption['is_new']:
                yield self._get_exemption_data(publisher_id, url, exemption["tender_type"])

    def _get_exemption_data(self, publisher_id, url, tender_type):
        url = "{}{}".format(self._url_prefix, url)
        return {"pid": publisher_id,
                "url": url,
                "data": self._get_url_response_text(url, self._timeout),
                "tender_type": tender_type}

    def _get_url_response_text(self, url, timeout):
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.text


if __name__ == "__main__":
    DownloadPagesDataProcessor.main()
