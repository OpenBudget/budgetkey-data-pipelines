from datapackage_pipelines_budgetkey.pipelines.procurement.tenders.check_existing import CheckExistingProcessor
from datapackage_pipelines_budgetkey.pipelines.procurement.tenders.add_publisher_urls_resource import \
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

def test_check_existing():
    ingest = ({"db-table": "dummy-db-table"},
              {"resources": [{"name": "tender-urls",
                              "schema": PUBLISHER_URLS_TABLE_SCHEMA}]},
              unlistify_resources(
                  [[{"id": 71, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=5",
                     "tender_type": "office"},
                    {"id": 71, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=66",
                     "tender_type": "office"},
                    {"id": 71, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=777",
                     "tender_type": "office"},
                    {"id": 71, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=8888",
                     "tender_type": "office"},
                    {"id": 71, "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=5",
                     "tender_type": "exemptions"},
                    {"id": 71, "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=66",
                     "tender_type": "exemptions"},
                    {"id": 71, "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=777",
                     "tender_type": "exemptions"},
                    {"id": 71, "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=8888",
                     "tender_type": "exemptions"},
                    {"id": None, "url": "https://www.mr.gov.il/CentralTenders/Goods/Pages/3-2013.aspx",
                     "tender_type": "central"},
                    {"id": None, "url": "https://www.mr.gov.il/CentralTenders/Goods/Pages/3-2014.aspx",
                     "tender_type": "central"}
                    ]]))
    datapackage, resources, stats = MockCheckExistingProcessor(mock_existing_ids=[(5, "exemptions", None),
                                                                                  (66, "office", None),
                                                                                  (None, "central", "Goods-3-2014")],
                                                               ingest_response=ingest).spew()
    resources = listify_resources(resources)
    assert len(resources) == 1
    assert resources[0] == [{"id": 71, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=5",
                             "tender_type": "office", "is_new": True},
                            # {"id": 71, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=66",
                            #  "tender_type": "office", "is_new": False},
                            {"id": 71, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=777",
                             "tender_type": "office", "is_new": True},
                            {"id": 71, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=8888",
                             "tender_type": "office", "is_new": True},
                            # {"id": 71,
                            #  "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=5",
                            #  "tender_type": "exemptions", "is_new": False},
                            {"id": 71,
                             "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=66",
                             "tender_type": "exemptions", "is_new": True},
                            {"id": 71,
                             "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=777",
                             "tender_type": "exemptions", "is_new": True},
                            {"id": 71,
                             "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=8888",
                             "tender_type": "exemptions", "is_new": True},
                            {'id': None, 'is_new': True,
                             'tender_type': 'central',
                             'url': 'https://www.mr.gov.il/CentralTenders/Goods/Pages/3-2013.aspx'},
                            # {'id': None, 'is_new': True,
                            #  'tender_type': 'central',
                            #  'url': 'https://www.mr.gov.il/CentralTenders/Goods/Pages/3-2014.aspx'}
                             ]
