from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew
import requests, logging, datetime, os


class CooperativesScraper(object):

    BASE_URL = "https://apps.moital.gov.il/CooperativeSocieties/api/Agudot/GetAgudotInfo" \
               "?N_AGUDA=&C_STATUS_MISHPATI=&C_YESHUV=&C_SUG=&SHEM_AGUDA=" \
               "&FromRowNum={FromRowNum}&quantity={quantity}"


    def parse_date(self, date):
        return datetime.datetime.strptime(date, "%d/%m/%Y %H:%M:%S")


    def parse_integer(self, n):
        return n if n else None


    def parse_cooperatives(self, cooperatives):
        for cooperative in cooperatives:
            yield {"id": self.parse_integer(cooperative["N_AGUDA"]),
                   "name": cooperative["SHEM_AGUDA"],
                   "registration_date": self.parse_date(cooperative["D_RISHUM_AGUDA"]),
                   "phone": cooperative["TELEPHON1"],
                   "primary_type_id": self.parse_integer(cooperative["C_SUG_RASHI"]),
                   "primary_type": cooperative["TEUR_SUG_RASHI"],
                   "secondary_type_id": self.parse_integer(cooperative["C_SUG_MISHNI"]),
                   "secondary_type": cooperative["TEUR_SUG_MISHNEI"],
                   "legal_status_id": self.parse_integer(cooperative["C_STATUS_MISHPATI"]),
                   "legal_status": cooperative["TEUR_STATUS_MISHPATI"],
                   "last_status_date": self.parse_date(cooperative["DateStatusLast"]),
                   "type": cooperative["TEUR_SUG_TNUA"],
                   "municipality_id": self.parse_integer(cooperative["C_YESHUV"]),
                   "municipality": cooperative["shem_yeshuv"],
                   "inspector": cooperative["ShemMefakeach"],
                   "address": cooperative["Ktovet"],}

    def requests_get(self, url):
        res = requests.get(url, headers={'Content-type': 'application/json'})
        res.raise_for_status()
        return res.json()

    def get_cooperatives(self):
        results_per_page = 15
        max_pages = 99999
        override_max_results = os.environ.get("OVERRIDE_COOPERATIVES_MAX_RESULTS")
        for i in range(1, max_pages, results_per_page):
            url = self.BASE_URL.format(FromRowNum=i, quantity=results_per_page)
            cooperatives = self.requests_get(url)
            if len(cooperatives) < results_per_page:
                logging.info("last page: {}".format(i))
                break
            elif override_max_results and i > int(override_max_results):
                break
            yield from self.parse_cooperatives(cooperatives)


    def get_resource_descriptor(self, resource_name):
        return {"name": resource_name,
                PROP_STREAMING: True,
                "path": resource_name+".csv",
                "schema": {"fields": [{'name': 'id', 'type': 'string'},
                                      {'name': 'name', 'type': 'string'},
                                      {'name': 'registration_date', 'type': 'datetime'},
                                      {'name': 'phone', 'type': 'string'},
                                      {'name': 'primary_type_id', 'type': 'integer'},
                                      {'name': 'primary_type', 'type': 'string'},
                                      {'name': 'secondary_type_id', 'type': 'integer'},
                                      {'name': 'secondary_type', 'type': 'string'},
                                      {'name': 'legal_status_id', 'type': 'integer'},
                                      {'name': 'legal_status', 'type': 'string'},
                                      {'name': 'last_status_date', 'type': 'datetime'},
                                      {'name': 'type', 'type': 'string'},
                                      {'name': 'municipality_id', 'type': 'integer'},
                                      {'name': 'municipality', 'type': 'string'},
                                      {'name': 'inspector', 'type': 'string'},
                                      {'name': 'address', 'type': 'string'},],
                           "primaryKey": ["id"]}}

if __name__ == "__main__":
    scraper = CooperativesScraper()
    parameters, datapackage, resources = ingest()
    datapackage["resources"] = [scraper.get_resource_descriptor(parameters["resource-name"])]
    spew(datapackage, [scraper.get_cooperatives()])
