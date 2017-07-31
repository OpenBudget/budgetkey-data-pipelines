import os, requests
from budgetkey_data_pipelines.pipelines.procurement.tenders.add_publishers_resource import resource_filter


def run_add_publishers_resource_processor(url, fixture_file_name, parameters):
    fixtures_dir = os.path.join(os.path.dirname(__file__), "fixtures")
    html_file = os.path.join(fixtures_dir, fixture_file_name)
    if not os.path.exists(html_file):
        with open(html_file, "w") as f:
            f.write(requests.get(url).text)
    with open(html_file) as f:
        html = f.read()
    publishers = list(resource_filter([{"data": line} for line in html.split("\n")],
                                      parameters=parameters))
    return publishers


def assert_publishers(publishers):
    assert len(publishers) == 110
    publisher_71 = [publisher for publisher in publishers if publisher["id"] == 71][0]
    publisher_13 = [publisher for publisher in publishers if publisher["id"] == 13][0]
    assert publisher_71 == {'id': 71, 'name': 'ההסתדרות הציונית העולמית - החטיבה להתישבות'}
    assert publisher_13 == {'id': 13, 'name': 'רשות מקרקעי ישראל'}


def test_office():
    publishers = run_add_publishers_resource_processor(url="https://www.mr.gov.il/OfficesTenders/Pages/SearchOfficeTenders.aspx",
                                                       fixture_file_name="SearchOfficeTenders.aspx",
                                                       parameters={"tender_type": "office"})
    assert_publishers(publishers)


def test_exemptions():
    publishers = run_add_publishers_resource_processor(url="http://www.mr.gov.il/ExemptionMessage/Pages/SearchExemptionMessages.aspx",
                                                       fixture_file_name="SearchExemptionMessages.aspx",
                                                       parameters={"tender_type": "exemptions"})
    assert_publishers(publishers)
