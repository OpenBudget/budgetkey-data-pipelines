from budgetkey_data_pipelines.pipelines.procurement.tenders.check_existing import CheckExistingProcessor
from budgetkey_data_pipelines.pipelines.procurement.tenders.add_publisher_urls_resource import \
    PUBLISHER_URLS_TABLE_SCHEMA
from ...common import listify_resources, unlistify_resources


class MockCheckExistingProcessor(CheckExistingProcessor):
    def __init__(self, mock_existing_ids, **kwargs):
        self.mock_existing_ids = mock_existing_ids
        super(MockCheckExistingProcessor, self).__init__(**kwargs)

    def get_all_existing_ids(self):
        return self.mock_existing_ids

    def spew(self):
        return self._get_spew_params()


def test_exemptions():
    ingest = ({"db-table": "dummy-db-table"},
              {"resources": [{"name": "publisher-urls",
                              "schema": PUBLISHER_URLS_TABLE_SCHEMA}]},
              unlistify_resources(
                  [[{"id": 71, "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=5"},
                    {"id": 71, "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=66"},
                    {"id": 71, "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=777"},
                    {"id": 71, "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=8888"}]]))
    datapackage, resources, stats = MockCheckExistingProcessor(mock_existing_ids=[5, 66], ingest_response=ingest).spew()
    resources = listify_resources(resources)
    assert len(resources) == 1
    assert resources[0] == [
        {"id": 71, "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=5", "is_new": False},
        {"id": 71,
         "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=66", "is_new": False},
        {"id": 71,
         "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=777", "is_new": True},
        {"id": 71,
         "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=8888", "is_new": True}]


def test_office():
    ingest = ({"db-table": "dummy-db-table"},
              {"resources": [{"name": "publisher-urls",
                              "schema": PUBLISHER_URLS_TABLE_SCHEMA}]},
              unlistify_resources(
                  [[{"id": 71, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=5"},
                    {"id": 71, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=66"},
                    {"id": 71, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=777"},
                    {"id": 71, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=8888"}]]))
    datapackage, resources, stats = MockCheckExistingProcessor(mock_existing_ids=[5, 66], ingest_response=ingest).spew()
    resources = listify_resources(resources)
    assert len(resources) == 1
    assert resources[0] == [
        {"id": 71, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=5", "is_new": False},
        {"id": 71,
         "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=66", "is_new": False},
        {"id": 71,
         "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=777", "is_new": True},
        {"id": 71,
         "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=8888", "is_new": True}]
