from datapackage_pipelines_budgetkey.pipelines.entities.cooperatives.cooperatives_scraper import CooperativesScraper
import os, json, datetime
from tests.common import assert_doc_conforms_to_schema


class MockCooperativesScraper(CooperativesScraper):

    def requests_get(self, url):
        if url == "https://govapps.economy.gov.il/CooperativeSocieties/api/Agudot/SearchData?N_AGUDA=&C_STATUS_MISHPATI=&C_YESHUV=&C_SUG=&SHEM_AGUDA=&FromRowNum=1&quantity=15":
            filename = "GetAgudotInfo_1.json"
        else:
            raise Exception("unknown url: {}".format(url))
        filename = os.path.join(os.path.dirname(__file__), filename)
        if not os.path.exists(filename):
            with open(filename, 'w') as f:
                data = super(MockCooperativesScraper, self).requests_get(url)
                json.dump(data, f)
        with open(filename) as f:
            return json.loads(json.load(f))


def test_cooperatives():
    scraper = MockCooperativesScraper()
    resource_descriptor = scraper.get_resource_descriptor("cooperatives")
    cooperative = next(scraper.get_cooperatives())
    assert_doc_conforms_to_schema(cooperative, resource_descriptor["schema"])
    assert cooperative == {'address': 'השבעה 77 צפת 1323530 ת.ד: ת.ד. 5423',
                           'cooperative_registration_date': '19/10/2021',
                           'id': '570066829',
                           'inspector': 'טל מור',
                           'legal_status': 'פעילה - מאושרת',
                           'municipality_id': 8000,
                           'name': 'ליובאוויטש אגודה שיתופית בע"מ',
                           'phone': None,
                           'primary_type': 'שרותים',
                           'primary_type_id': 10,
                         }
