from datapackage_pipelines_budgetkey.pipelines.entities.cooperatives.cooperatives_scraper import CooperativesScraper
import os, json, datetime
from tests.common import assert_doc_conforms_to_schema


class MockCooperativesScraper(CooperativesScraper):

    def requests_get(self, url):
        if url == "https://apps.moital.gov.il/CooperativeSocieties/api/Agudot/GetAgudotInfo?N_AGUDA=&C_STATUS_MISHPATI=&C_YESHUV=&C_SUG=&SHEM_AGUDA=&FromRowNum=1&quantity=15":
            filename = "GetAgudotInfo_1.json"
        else:
            raise Exception("unknown url: {}".format(url))
        filename = os.path.join(os.path.dirname(__file__), filename)
        if not os.path.exists(filename):
            with open(filename, 'w') as f:
                data = super(MockCooperativesScraper, self).requests_get(url)
                json.dump(data, f)
        with open(filename) as f:
            return json.load(f)


def test():
    scraper = MockCooperativesScraper()
    resource_descriptor = scraper.get_resource_descriptor("cooperatives")
    cooperative = next(scraper.get_cooperatives())
    assert_doc_conforms_to_schema(cooperative, resource_descriptor["schema"])
    assert cooperative == {'address': 'דופקר ++++',
                           'id': '570000018',
                           'inspector': 'צוק חיים',
                           'last_status_date': '29/01/1970 12:00:00',
                           'legal_status': 'הודעה שניה על מחיקה',
                           'legal_status_id': '23',
                           'municipality': '',
                           'municipality_id': None,
                           'name': 'נחלת ישראל רמה אגודה שתופית בע"מ (במחיקה)',
                           'phone': '',
                           'primary_type': 'שיכון',
                           'primary_type_id': '43',
                           'registration_date': '06/02/1921 00:00:00',
                           'secondary_type': 'שיכון',
                           'secondary_type_id': '61',
                           'type': 'התאחדות האיכרים'}
