from budgetkey_data_pipelines.common.resource_filter_processor import ResourceFilterProcessor
import requests


class DownloadExemptionPagesDataProcessor(ResourceFilterProcessor):

    def __init__(self, timeout=180, url_prefix="http://www.mr.gov.il", **kwargs):
        self._timeout = timeout
        self._url_prefix = url_prefix
        super(DownloadExemptionPagesDataProcessor, self).__init__(
            default_input_resource="publisher-urls",
            default_output_resource="publisher-urls-downloaded-data",
            default_replace_resource=True,
            table_schema={"fields": [{"name": "pid", "title": "publisher id", "type": "integer"},
                                     {"name": "url", "title": "exemption page url", "type": "string"},
                                     {"name": "data", "title": "exemption page html data", "type": "string"}]},
            **kwargs
        )

    def filter_resource_data(self, data, parameters):
        for exemption in data:
            publisher_id = exemption["id"]
            url = exemption["url"]
            yield self._get_exemption_data(publisher_id, url)

    def _get_exemption_data(self, publisher_id, url):
        return {"pid": publisher_id,
                "url": url,
                "data": self._get_url_response_text("{}{}".format(self._url_prefix, url), self._timeout)}

    def _get_url_response_text(self, url, timeout):
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        return response.text


if __name__ == "__main__":
    DownloadExemptionPagesDataProcessor.main()
