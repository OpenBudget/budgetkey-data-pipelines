from datapackage_pipelines_budgetkey.pipelines.procurement.tenders.download_pages_data import (
    DownloadPagesDataProcessor, TABLE_SCHEMA as DOWNLOAD_PAGES_DATA_TABLE_SCHEMA
)
from ...common import listify_resources, unlistify_resources
import os
import requests


def get_mock_exemption_data(publication_id, url=None):
    filename = os.path.join(os.path.dirname(__file__), "fixtures", "publication_{}".format(publication_id))
    if url and not os.path.exists(filename):
        res = requests.get(url)
        with open(filename, 'w', encoding="utf-8") as f:
            f.write(res.text)
    with open(filename, encoding="utf-8") as f:
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
        if url == "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=595431":
            return get_mock_exemption_data("595431", url)
        elif url == "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=594269":
            return get_mock_exemption_data("594269", url)
        elif url == "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=598379":
            return get_mock_exemption_data("598379", url)
        elif url == "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=596915":
            return get_mock_exemption_data("596915", url)
        elif url == "https://www.mr.gov.il/CentralTenders/Goods/Pages/3-2013.aspx":
            return get_mock_exemption_data("Goods-3-2013", url)
        elif url == "https://www.mr.gov.il/CentralTenders/technology/Pages/15-2016.aspx":
            return get_mock_exemption_data("technology-15-2016", url)
        elif url == "https://www.mr.gov.il/CentralTenders/Goods/Pages/19-2017.aspx":
            return get_mock_exemption_data("Goods-19-2017", url)
        elif url == "https://www.mr.gov.il/CentralTenders/network/Pages/michraz3.aspx":
            return get_mock_exemption_data("network-michraz3", url)
        else:
            raise Exception("invalid url: {}".format(url))

def run_download_processor(resources):
    ingest_response = ({}, {"resources": [{"name": "tender-urls"}]}, unlistify_resources(resources))
    datapackage, resources, stats = MockDownloadExemptionPagesDataProcessor(ingest_response=ingest_response).spew()
    assert datapackage == {"resources": [{
        'dpp:streaming': True,
        "name": "tender-urls-downloaded-data",
        "path": "data/tender-urls-downloaded-data.csv",
        "schema": DOWNLOAD_PAGES_DATA_TABLE_SCHEMA
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
        assert item["tender_type"] == expected_item["tender_type"]


def test():
    resource = run_download_processor([[
        {"id": 71, "url": "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=595431", "is_new": True,
         "tender_type": "exemptions"},
        {"id": 71, "url": "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=594269", "is_new": True,
         "tender_type": "exemptions"},
        {"id": 71, "url": "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=666666", "is_new": False,
         "tender_type": "exemptions"},
        {"id": 50, "url": "/officestenders/Pages/officetender.aspx?pID=598379", "is_new": True,
         "tender_type": "office"},
        {"id": 21, "url": "/officestenders/Pages/officetender.aspx?pID=596915", "is_new": True,
         "tender_type": "office"},
        {"id": 71, "url": "/officestenders/Pages/officetender.aspx?pID=666666", "is_new": False,
         "tender_type": "office"},
        {"id": None, "url": "https://www.mr.gov.il/CentralTenders/Goods/Pages/3-2013.aspx",
         "tender_type": "central", "is_new": True},
        {"id": None, "url": "https://www.mr.gov.il/CentralTenders/Goods/Pages/3-2014.aspx",
         "tender_type": "central", "is_new": False},
        {"id": None, "url": "https://www.mr.gov.il/CentralTenders/technology/Pages/15-2016.aspx",
         "tender_type": "central", "is_new": True},
        {"id": None, "url": "https://www.mr.gov.il/CentralTenders/Goods/Pages/19-2017.aspx",
         "tender_type": "central", "is_new": True},
        {"id": None, "url": "https://www.mr.gov.il/CentralTenders/network/Pages/michraz3.aspx",
         "tender_type": "central", "is_new": True},
    ]])
    assert_downloaded_resource(resource, [
        {"pid": 71, "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=595431",
         "data": get_mock_exemption_data("595431"), "tender_type": "exemptions"},
        {"pid": 71, "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=594269",
         "data": get_mock_exemption_data("594269"), "tender_type": "exemptions"},
        {"pid": 50, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=598379",
         "data": get_mock_exemption_data("598379"), "tender_type": "office"},
        {"pid": 21, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=596915",
         "data": get_mock_exemption_data("596915"), "tender_type": "office"},
        {"pid": None, "url": "https://www.mr.gov.il/CentralTenders/Goods/Pages/3-2013.aspx",
         "data": get_mock_exemption_data("Goods-3-2013"), "tender_type": "central"},
    ])
