from budgetkey_data_pipelines.pipelines.entities.geocode_entities import GeoCodeEntities
import os, json, pytest
from sqlalchemy import create_engine, MetaData, Table, Column, Float, Text
from sqlalchemy.orm.session import sessionmaker


BING_API_KEY = os.environ.get("BING_API_KEY")


class MockGeoCodeEntities(GeoCodeEntities):

    PROVIDERS = [
        # limit to these 2 providers for testing
        {
            "provider": "bing",
            "env_vars": ["BING_API_KEY"],
            "limit": 50000
        },
        {
            "provider": "google",
            "limit": 2500
        },
    ]

    def __init__(self, parameters, datapackage, resources):
        super(MockGeoCodeEntities, self).__init__(parameters, datapackage, resources)
        self.geocoder_get_calls = []

    def initialize_db_session(self):
        connection_string = "sqlite://"
        engine = create_engine(connection_string)
        session = sessionmaker(bind=engine)()
        metadata = MetaData(bind=session.connection())
        entities_geo = Table("tests_entities_geo", metadata,
                             Column("entity_id", Text),
                             Column("location", Text),
                             Column("lat", Float),
                             Column("lng", Float),
                             Column("provider", Text),
                             Column("geojson", Text))
        entities_geo.create()
        entities_geo.insert().values(entity_id='589114263', location=' , בנימינה-גבעת עדה, , 3050000',
                                     lat=123.456, lng=456.123, provider="google", geojson="{}").execute()
        return session

    def geocoder_get(self, location, provider, timeout):
        assert provider in ["google", "bing"]
        self.geocoder_get_calls.append((location, provider))
        if location == 'קרן קימת לישראל 16, בת ים, , 5953101':
            filename = "kakal_16_batyam_5953101"
        elif location == ' , בנימינה-גבעת עדה, , 3050000':
            filename = "binyamina-givat ada, 3050000"
        elif location == 'ברל נורא 5, בני ברק, , 0':
            filename = "berl_nora_5_bney_brak"
        elif location == 'כיאט 3, חיפה, ישראל, 33261':
            filename = "kyat_3_haifa_israel_33261"
        else:
            raise Exception("unknown location: '{}'".format(location))
        if provider == "bing":
            filename += "_bing"
        filename = os.path.join(os.path.dirname(__file__), filename)
        if not os.path.exists(filename):
            g = super(MockGeoCodeEntities, self).geocoder_get(location, provider, 5)
            res = {"ok": g.ok, "lat": None, "lng": None, "geojson": g.geojson, "confidence": g.confidence, "provider": provider}
            if res["ok"]:
                res["lat"], res["lng"] = g.latlng
            with open(filename, "w") as f:
                json.dump(res, f)
        with open(filename) as f:
            res = json.load(f)
        return type("MockGeo", (object,),
                    {"ok": res["ok"],
                     "lat": res["lat"], "lng": res["lng"],
                     "geojson": res["geojson"],
                     "confidence": res["confidence"]})

def assert_geocode_provider(provider):
    parameters = {"output-resource": "entities-geo", "geo-table": "tests_entities_geo"}
    datapackage = {"resources": [{"name": "entities-geo"}]}
    entities_resource = [{"id": '500107487', "details": {"city": "בת ים", "street": "קרן קימת לישראל",
                                                         "zipcode": "5953101", "house_number": "16"}},
                         {"id": '500213525', "details": {"city": "בנימינה-גבעת עדה", "street": "",
                                                         "zipcode": "3050000", "house_number": ""}},
                         {"id": '500302369', "details": {"city": "בני ברק", "street": "ברל נורא",
                                                         "zipcode": "0", "house_number": "5"}},
                         {"id": '500409362', "details": {"city": "", "street": "",
                                                         "zipcode": "0", "house_number": ""}},
                         {"id": '510000268', "details": {"pob": "33261", "city": "חיפה",
                                                         "goal": "לעסוק בסוגי עיסוק שפורטו בתקנון",
                                                         "type": "חברה פרטית",
                                                         "limit": "מוגבלת", "__hash": "", "mafera": "",
                                                         "status": "מחוסלת מרצון",
                                                         "street": "כיאט",
                                                         "country": "ישראל", "__is_new": "False", "pob_city": "חיפה",
                                                         "__is_stale": "True",
                                                         "government": "חברה לא ממשלתית",
                                                         "located_at": "עו\"ד אנוש וקסמן",
                                                         "description": "",
                                                         "postal_code": "3326125", "street_number": "3",
                                                         "pob_postal_code": "3133201",
                                                         "last_report_year": 2010,
                                                         "__last_updated_at": "2017-07-27T09:40:57",
                                                         "registration_date": "1936-10-05",
                                                         "__last_modified_at": "2017-07-27T09:40:57",
                                                         "__next_update_days": "1"}},
                         {"id": '589114263', "details": {"address": ""}},
                         # this entity has the same address as previous entity - won't be geocoded again
                         {"id": '500302369', "details": {"city": "בני ברק", "street": "ברל נורא",
                                                         "zipcode": "0", "house_number": "5"}},]
    assert len(entities_resource) == 7
    resources = [entities_resource]
    processor = MockGeoCodeEntities(parameters, datapackage, resources)
    resources = processor.filter_resources()
    resources = list(resources)
    assert len(resources) == 1
    resource = list(resources[0])
    assert len(resource) == 5
    if provider == "google":
        assert resource[0]["geojson"] == {"type": "Feature",
                                       "properties": {"accuracy": "ROOFTOP",
                                                      'address': 'Kakal St 16, Bat Yam, Israel',
                                                      "bbox": [34.7550721197085, 32.02199471970849, 34.7577700802915, 32.02469268029149],
                                                      "city": "Bat Yam", "confidence": 9, "country": "IL",
                                                      "encoding": "utf-8", "housenumber": "16", "lat": 32.0233437,
                                                      "lng": 34.7564211,
                                                      "location": "\u05e7\u05e8\u05df \u05e7\u05d9\u05de\u05ea \u05dc\u05d9\u05e9\u05e8\u05d0\u05dc 16, \u05d1\u05ea \u05d9\u05dd, , 5953101",
                                                      "ok": True, "place": "ChIJM2gbRkGzAhURlVxwg59dKVo",
                                                      "provider": "google",
                                                      "quality": "street_address",
                                                      "state": "Center District",
                                                      "status": "OK",
                                                      "status_code": 200,
                                                      "street": "Kakal St"},
                                       "bbox": [34.7550721197085, 32.02199471970849, 34.7577700802915, 32.02469268029149],
                                       "geometry": {"type": "Point", "coordinates": [34.7564211, 32.0233437]}}
    nogeojson = lambda r: {k:r[k] for k in r if k != "geojson"}
    google = provider == "google"
    assert nogeojson(resource[0]) == {'entity_id': '500107487',
                                      'lat': 32.0233437 if google else 32.02332,
                                      'lng': 34.7564211 if google else 34.75646,
                                      'location': 'קרן קימת לישראל 16, בת ים, , 5953101',
                                      'provider': provider, }
    assert nogeojson(resource[1]) == {'entity_id': '500213525',
                                      'lat': 123.456, 'lng': 456.123,
                                      'location': ' , בנימינה-גבעת עדה, , 3050000',
                                      #   this item is from DB - hence the hard-coded provider
                                      'provider': 'google', }
    assert nogeojson(resource[2]) == {'entity_id': '500302369',
                                      'lat': None if google else 32.0833320617676,
                                      'lng': None if google else 34.8333320617676,
                                      'location': 'ברל נורא 5, בני ברק, , 0',
                                      'provider': provider, }
    assert nogeojson(resource[3]) == {'entity_id': '510000268',
                                      'lat': 32.8187663 if google else 32.81889,
                                      'lng': 34.9987921 if google else 34.99908,
                                      'location': 'כיאט 3, חיפה, ישראל, 33261',
                                      'provider': provider, }
    assert nogeojson(resource[4]) == {'entity_id': '500302369',
                                      'lat': None if google else 32.0833320617676,
                                      'lng': None if google else 34.8333320617676,
                                      'location': 'ברל נורא 5, בני ברק, , 0',
                                      'provider': provider, }
    assert processor.geocoder_get_calls == [('קרן קימת לישראל 16, בת ים, , 5953101', provider),
                                            ('ברל נורא 5, בני ברק, , 0', provider),
                                            ('כיאט 3, חיפה, ישראל, 33261', provider)]


def test_google():
    # we force the bing api key to be empty - this will cause the free google provider to be used
    os.environ["BING_API_KEY"] = ""
    assert_geocode_provider("google")

def test_bing():
    # set the bing api token, when using the mocks, the token can be invalid, as we don't make any http request
    os.environ["BING_API_KEY"] = BING_API_KEY if BING_API_KEY else "fake-bing-api-token"
    assert_geocode_provider("bing")
