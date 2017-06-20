from budgetkey_data_pipelines.pipelines.procurement.tenders.exemptions.download_exemption_pages_data import (
    DownloadExemptionPagesDataProcessor
)
from ....common import listify_resources, unlistify_resources
import os


def get_mock_exemption_data(exemption_id):
    with open(os.path.join(os.path.dirname(__file__), "ExemptionMessage_{}".format(exemption_id))) as f:
        return f.read()


class MockDownloadExemptionPagesDataProcessor(DownloadExemptionPagesDataProcessor):

    def __init__(self, mock_existing_ids=None, **kwargs):
        self.mock_existing_ids = mock_existing_ids
        super(MockDownloadExemptionPagesDataProcessor, self).__init__(**kwargs)

    def spew(self):
        return self._get_spew_params()

    def _get_url_response_text(self, url, timeout):
        if timeout != 180:
            raise Exception("invalid timeout: {}".format(timeout))
        if url == "http://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=595431":
            return get_mock_exemption_data("595431")
        elif url == "http://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=594269":
            return get_mock_exemption_data("594269")
        else:
            raise Exception("invalid url: {}".format(url))


def test():
    resources = [[
        {"id": 71, "url": "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=595431", "is_new": True},
        {"id": 71, "url": "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=594269", "is_new": False}
    ]]
    ingest_response = ({}, {"resources": [{"name": "publisher-urls"}]}, unlistify_resources(resources))
    datapackage, resources, stats = MockDownloadExemptionPagesDataProcessor(ingest_response=ingest_response).spew()
    assert datapackage == {"resources": [{
        "name": "publisher-urls-downloaded-data",
        "path": "data/publisher-urls-downloaded-data.csv",
        "schema": {
            "fields": [
                {"name": "pid", "title": "publisher id", "type": "integer"},
                {"name": "url", "title": "exemption page url", "type": "string"},
                {"name": "data", "title": "exemption page html data", "type": "string"}
            ]
        }
    }]}
    assert listify_resources(resources) == [[
        {"pid": 71, "url": "http://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=595431", "data": get_mock_exemption_data("595431")},
    ]]
    assert stats == {}

