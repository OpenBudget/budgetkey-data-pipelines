from budgetkey_data_pipelines.pipelines.procurement.tenders.download_pages_data import (
    DownloadPagesDataProcessor
)
from ...common import listify_resources, unlistify_resources
import os
import requests


def get_mock_exemption_data(publication_id, url=None):
    filename = os.path.join(os.path.dirname(__file__), "fixtures", "publication_{}".format(publication_id))
    if url and not os.path.exists(filename):
        res = requests.get(url)
        with open(filename, 'w') as f:
            f.write(res.text)
    with open(filename) as f:
        return f.read()


class MockDownloadExemptionPagesDataProcessor(DownloadPagesDataProcessor):

    def __init__(self, mock_existing_ids=None, **kwargs):
        self.mock_existing_ids = mock_existing_ids
        super(MockDownloadExemptionPagesDataProcessor, self).__init__(**kwargs)

    def spew(self):
        return self._get_spew_params()

    def _get_url_response_text(self, url, timeout):
        if timeout != 180:
            raise Exception("invalid timeout: {}".format(timeout))
        if url == "http://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=595431":
            return get_mock_exemption_data("595431", url)
        elif url == "http://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=594269":
            return get_mock_exemption_data("594269", url)
        elif url == "http://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=598379":
            return get_mock_exemption_data("598379", url)
        elif url == "http://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=596915":
            return get_mock_exemption_data("596915", url)
        else:
            raise Exception("invalid url: {}".format(url))

def run_download_processor(resources):
    ingest_response = ({}, {"resources": [{"name": "publisher-urls"}]}, unlistify_resources(resources))
    datapackage, resources, stats = MockDownloadExemptionPagesDataProcessor(ingest_response=ingest_response).spew()
    assert datapackage == {"resources": [{
        "name": "publisher-urls-downloaded-data",
        "path": "data/publisher-urls-downloaded-data.csv",
        "schema": {
            "fields": [
                {"name": "pid", "title": "publisher id", "type": "integer"},
                {"name": "url", "title": "page url", "type": "string"},
                {"name": "data", "title": "page html data", "type": "string"}
            ]
        }
    }]}
    assert stats == {}
    resources = listify_resources(resources)
    assert len(resources) == 1
    return resources[0]


def assert_downloaded_resource(resource, expected_resource):
    for item, expected_item in zip(resource, expected_resource):
        assert item["pid"] == expected_item["pid"]
        assert item["url"] == expected_item["url"]
        assert item["data"] == expected_item["data"]


def test_exemptions():
    resource = run_download_processor([[
        {"id": 71, "url": "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=595431", "is_new": True},
        {"id": 71, "url": "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=594269", "is_new": True},
        {"id": 71, "url": "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=666666", "is_new": False}
    ]])
    assert_downloaded_resource(resource, [
        {"pid": 71, "url": "http://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=595431",
         "data": get_mock_exemption_data("595431")},
        {"pid": 71, "url": "http://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=594269",
         "data": get_mock_exemption_data("594269")},
    ])


def test_office():
    resource = run_download_processor([[
        {"id": 50, "url": "/officestenders/Pages/officetender.aspx?pID=598379", "is_new": True},
        {"id": 21, "url": "/officestenders/Pages/officetender.aspx?pID=596915", "is_new": True},
        {"id": 71, "url": "/officestenders/Pages/officetender.aspx?pID=666666", "is_new": False}
    ]])
    assert_downloaded_resource(resource, [
        {"pid": 50, "url": "http://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=598379",
         "data": get_mock_exemption_data("598379")},
        {"pid": 21, "url": "http://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=596915",
         "data": get_mock_exemption_data("596915")},
    ])
