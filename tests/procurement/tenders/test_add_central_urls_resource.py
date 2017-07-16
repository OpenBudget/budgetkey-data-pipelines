from budgetkey_data_pipelines.pipelines.procurement.tenders.add_central_urls_resource import AddCentralUrlsResource
from tests.common import listify_resources, unlistify_resources
import os, requests


class MockAddCentralUrls(AddCentralUrlsResource):

    def requests_get(self, url):
        if url == "https://www.mr.gov.il/CentralTenders/Pages/SearchTenders.aspx?start1=1&start2=1&start3=1&start4=1":
            filename = "SearchTenders.aspx_1_1_1_1"
        elif url == "https://www.mr.gov.il/CentralTenders/Pages/SearchTenders.aspx?start1=11&start2=11&start3=11&start4=11":
            filename = "SearchTenders.aspx_11_11_11_11"
        elif url == "https://www.mr.gov.il/CentralTenders/Pages/SearchTenders.aspx?start1=21&start2=21&start3=21&start4=21":
            filename = "SearchTenders.aspx_21_21_21_21"
        elif url == "https://www.mr.gov.il/CentralTenders/Pages/SearchTenders.aspx?start1=31&start2=31&start3=31&start4=31":
            filename = "SearchTenders.aspx_31_31_31_31"
        elif url == "https://www.mr.gov.il/CentralTenders/Pages/SearchTenders.aspx?start1=41&start2=41&start3=41&start4=41":
            filename = "SearchTenders.aspx_41_41_41_41"
        elif url == "https://www.mr.gov.il/CentralTenders/Pages/SearchTenders.aspx?start1=51&start2=51&start3=51&start4=51":
            filename = "SearchTenders.aspx_51_51_51_51"
        elif url == "https://www.mr.gov.il/CentralTenders/Pages/SearchTenders.aspx?start1=61&start2=61&start3=61&start4=61":
            filename = "SearchTenders.aspx_61_61_61_61"
        else:
            raise Exception("invalid url: {}".format(url))
        filename = os.path.join(os.path.dirname(__file__), "fixtures", filename)
        if not os.path.exists(filename):
            with open(filename, "w") as f:
                f.write(super(MockAddCentralUrls, self).requests_get(url))
        with open(filename) as f:
            return f.read()


def test():
    parameters = {}
    datapackage = {"resources": []}
    resources = unlistify_resources([])
    datapackage, resources, stats = MockAddCentralUrls(ingest_response=(parameters, datapackage, resources))._get_spew_params()
    resources = listify_resources(resources)
    assert len(resources) == 1
    resource = resources[0]
    assert len(resource) == 120
    assert resource[0] == {'id': None, 'tender_type': 'central',
                           'url': 'http://www.mr.gov.il/CentralTenders/Goods/Pages/3-2013.aspx'}

