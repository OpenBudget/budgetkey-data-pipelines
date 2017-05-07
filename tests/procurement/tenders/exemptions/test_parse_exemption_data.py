from budgetkey_data_pipelines.pipelines.procurement.tenders.exemptions.parse_exemption_data import (
    ParseExemptionDataProcessor
)
from ....common import listify_resources, unlistify_resources
from .test_download_exemption_pages import get_mock_exemption_data
import json


class MockParseExemptionDataProcessor(ParseExemptionDataProcessor):

    def spew(self):
        return self._get_spew_params()


def test():
    resources = [[
        {"pid": 71, "url": "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=595431", "data": get_mock_exemption_data("595431")},
        {"pid": 71, "url": "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=594269", "data": get_mock_exemption_data("594269")}
    ]]
    datapackage = {"resources": [{
        "name": "publisher-urls-downloaded-data",
        "path": "data/publisher-urls-downloaded-data.csv",
        "schema": {
            "fields": [
                {"name": "pid", "title": "publisher id", "type": "integer"},
                {"name": "url", "title": "exemption page url", "type": "string"},
                {"name": "data", "title": "exemption page html data", "type": "string"}
            ]
        }
    }]}
    ingest_response = ({}, datapackage, unlistify_resources(resources))
    datapackage, resources = MockParseExemptionDataProcessor(ingest_response=ingest_response).spew()
    assert datapackage == {"resources": [{
        "name": "exemptions",
        "path": "data/exemptions.csv",
        "schema": {
            "primaryKeys": ["publisher_id", "page_url", "publication_id"],
            "fields": [
                {"name": "publisher_id", "type": "integer"},
                {"name": "page_url", "title": "exemption page url", "type": "string"},
                {"name": "publication_id", "type": "string"},
                {"name": 'description', "type": "string"},
                {"name": "supplier_id", "type": "string"},
                {"name": "supplier", "type": "string"},
                {"name": "contact", "type": "string"},
                {"name": "publisher", "type": "string"},
                {"name": "contact_email", "type": "string"},
                {"name": "claim_date", "type": "string"},
                {"name": "last_update_date", "type": "string"},
                {"name": "reason", "type": "string"},
                {"name": "source_currency", "type": "string"},
                {"name": "regulation", "type": "string"},
                {"name": "volume", "type": "string"},
                {"name": "subjects", "type": "string"},
                {"name": "start_date", "type": "string"},
                {"name": "end_date", "type": "string"},
                {"name": "decision", "type": "string"},
                {"name": "page_title", "type": "string"},
                {"name": "documents_json", "type": "string"}
            ]
        }
    }]}
    assert listify_resources(resources)[0][0] == {
        "publisher_id": 71,
        "page_url": "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=595431",
        "publication_id": "595431",
        "description": """התקשרות עם ספקית תשתית התקשרות ואינטרנט וכן מתן שרותי תמיכה, סיוע לקוחות בכל אתרי החטיבה בארץ, גיבוי, אבטחת מידע, שרתי תקשורת וכיוצ"ב עפ"י המתואר במסמך המצורף. הנ"ל מהווה חסכון של כ- 300 אלף ש"ח לכל שנה""",
        "supplier_id": "510791734",
        "supplier": "לשכה לעיבוד נתונים של הסוכנות היהוד",
        "contact": "",
        "publisher": "ההסתדרות הציונית העולמית - החטיבה להתישבות",
        "contact_email": "",
        "claim_date": "",
        "last_update_date": "14/03/2017",
        "reason": "ספק יחיד בנסיבות העניין. לא היו השגות עד למועד שנקבע",
        "source_currency": "שקל",
        "regulation": "התקשרות עם ספק יחיד",
        "volume": "2,569,700",
        "subjects": "מחשוב ותקשורת",
        "start_date": "01/03/2017",
        "end_date": "31/12/2021",
        "decision": "נרשם",
        "page_title": "דיווח פטור משרדי",
        "documents_json": json.dumps([{"description": "חוות דעת מקצועית",
                                       "link": "http://www.mr.gov.il/Files_Michrazim/234734.pdf",
                                       "update_time": "תאריך עדכון מסמך:   00:00 14/03/2017"}])
    }
