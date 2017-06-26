from budgetkey_data_pipelines.pipelines.procurement.tenders.exemptions import parse_exemption_data
from budgetkey_data_pipelines.pipelines.procurement.tenders.exemptions import download_exemption_pages_data
from ....common import listify_resources, unlistify_resources, assert_doc_conforms_to_schema
from .test_download_exemption_pages import get_mock_exemption_data
import json
from datetime import date


class MockParseExemptionDataProcessor(parse_exemption_data.ParseExemptionDataProcessor):

    def spew(self):
        return self._get_spew_params()


INPUT_RESOURCES = [[
    {"pid": 71, "url": "http://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=595431", "data": get_mock_exemption_data("595431")},
    {"pid": 71, "url": "http://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=594269", "data": get_mock_exemption_data("594269")}
]]

INPUT_DATAPACKAGE = {"resources": [{
    "name": download_exemption_pages_data.DEFAULT_OUTPUT_RESOURCE,
    "path": "data/{}.csv".format(download_exemption_pages_data.DEFAULT_OUTPUT_RESOURCE),
    "schema": download_exemption_pages_data.TABLE_SCHEMA
}]}

EXPECTED_OUTPUT_DATAPACKAGE = {"resources": [{
    "name": parse_exemption_data.OUTPUT_RESOURCE,
    "path": "data/{}.csv".format(parse_exemption_data.OUTPUT_RESOURCE),
    "schema": parse_exemption_data.TABLE_SCHEMA
}]}

EXPECTED_OUTPUT_RESOURCE_1_ROW_1 = {
    "publisher_id": 71,
    "publication_id": 595431,
    "page_url": "http://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=595431",
    "description": """התקשרות עם ספקית תשתית התקשרות ואינטרנט וכן מתן שרותי תמיכה, סיוע לקוחות בכל אתרי החטיבה בארץ, גיבוי, אבטחת מידע, שרתי תקשורת וכיוצ"ב עפ"י המתואר במסמך המצורף. הנ"ל מהווה חסכון של כ- 300 אלף ש"ח לכל שנה""",
    "supplier_id": "510791734",
    "supplier": "לשכה לעיבוד נתונים של הסוכנות היהוד",
    "contact": "",
    "publisher": "ההסתדרות הציונית העולמית - החטיבה להתישבות",
    "contact_email": "",
    "claim_date": None,
    "last_update_date": date(2017, 3, 14),
    "reason": "ספק יחיד בנסיבות העניין. לא היו השגות עד למועד שנקבע",
    "source_currency": "שקל",
    "regulation": "התקשרות עם ספק יחיד",
    "volume": "2,569,700",
    "subjects": "מחשוב ותקשורת",
    "start_date": date(2017, 3, 1),
    "end_date": date(2021, 12, 31),
    "decision": "נרשם",
    "page_title": "דיווח פטור משרדי",
    "documents": [{"description": "חוות דעת מקצועית",
                                   "link": "http://www.mr.gov.il/Files_Michrazim/234734.pdf",
                                   "update_time": "2017-03-14"}]
}


def test():
    ingest_response = ({}, INPUT_DATAPACKAGE, unlistify_resources(INPUT_RESOURCES))
    datapackage, resources, stats = MockParseExemptionDataProcessor(ingest_response=ingest_response).spew()
    assert datapackage == EXPECTED_OUTPUT_DATAPACKAGE
    assert listify_resources(resources)[0][0] == EXPECTED_OUTPUT_RESOURCE_1_ROW_1
    assert stats == {}
    assert_doc_conforms_to_schema(EXPECTED_OUTPUT_RESOURCE_1_ROW_1, parse_exemption_data.TABLE_SCHEMA)

