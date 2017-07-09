from budgetkey_data_pipelines.pipelines.procurement.tenders.parse_page_data import ParsePageDataProcessor
from ...common import listify_resources, unlistify_resources, assert_doc_conforms_to_schema
from .test_download_pages_data import get_mock_exemption_data
import datetime


class MockParseExemptionDataProcessor(ParsePageDataProcessor):

    def spew(self):
        return self._get_spew_params()


def run_parse_processor(type, resource):
    parameters = {"output_resource": type,
                  "tender_type": type}
    datapackage = {"resources": [{
        "name": "publisher-urls-downloaded-data",
        "path": "data/publisher-urls-downloaded-data.csv",
        "schema": {
            "fields": [
                {"name": "pid", "title": "publisher id", "type": "integer"},
                {"name": "url", "title": "page url", "type": "string"},
                {"name": "data", "title": "page html data", "type": "string"}
            ]
        }
    }]}
    resources = unlistify_resources([resource])
    ingest_response = (parameters, datapackage, resources)
    datapackage, resources, stats = MockParseExemptionDataProcessor(ingest_response=ingest_response).spew()
    resources = listify_resources(resources)
    assert len(resources) == 1
    return resources[0]


def test_exemptions():
    resource = run_parse_processor("exemptions", [
        {"pid": 71, "url": "http://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=595431",
         "data": get_mock_exemption_data("595431")},
        {"pid": 71, "url": "http://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=594269",
         "data": get_mock_exemption_data("594269")},
    ])
    assert len(resource) == 2
    assert resource[0] == {
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
        "last_update_date": datetime.date(2017, 3, 14),
        "reason": "ספק יחיד בנסיבות העניין. לא היו השגות עד למועד שנקבע",
        "source_currency": "שקל",
        "regulation": "התקשרות עם ספק יחיד",
        "volume": "2,569,700",
        "subjects": "מחשוב ותקשורת",
        "start_date": datetime.date(2017, 3, 1),
        "end_date": datetime.date(2021, 12, 31),
        "decision": "נרשם",
        "page_title": "דיווח פטור משרדי",
        "documents": [{"description": "חוות דעת מקצועית",
                                       "link": "http://www.mr.gov.il/Files_Michrazim/234734.pdf",
                                       "update_time": "2017-03-14"}]
    }
    assert resource[1]["publication_id"] == 594269

def test_office():
    resource = run_parse_processor("office", [
        {"pid": 50, "url": "http://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=598379",
         "data": get_mock_exemption_data("598379")},
        {"pid": 21, "url": "http://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=596915",
         "data": get_mock_exemption_data("596915")},
    ])
    print(resource)
    assert len(resource) == 2
    assert resource[0] == {'publisher_id': 50,
                           'publication_id': 598379,
                           'page_url': 'http://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=598379',
                           'description': 'לניהול אירועי שנת ה-70 למדינת ישראל - פרסום נוסף',
                           'publisher': 'משרד המדע התרבות והספורט',
                           'claim_date': datetime.datetime(2017, 7, 19, 12),
                           'last_update_date': datetime.date(2017, 7, 9),
                           'subjects': 'שירותי עריכה,כתיבה ,עיצוב ,גרפיקה ואומנות; שירותי עריכה ותרגום; כתיבה יצירתית; שירותי הדפסה; שירותי הפקת וידאו; שירותים הקשורים לטלוויזיה',
                           'start_date': datetime.date(2017, 5, 25),
                           'documents': [{'description': ' מענה לשאלות הבהרה מכרז פומבי 18/2017',
                                          'link': 'http://www.mr.gov.il/Files_Michrazim/242223.pdf',
                                          'update_time': '2017-07-09'},
                                         {'description': ' מענה לשאלות הבהרה מכרז פומבי 18/2017',
                                          'link': 'http://www.mr.gov.il/Files_Michrazim/242222.pdf',
                                          'update_time': '2017-07-09'},
                                         {'description': 'מודעה לעיתונות בערבית',
                                          'link': 'http://www.mr.gov.il/Files_Michrazim/242161.doc',
                                          'update_time': '2017-07-06'},
                                         {'description': 'מודעה לעיתונות בעברית',
                                          'link': 'http://www.mr.gov.il/Files_Michrazim/242160.doc',
                                          'update_time': '2017-07-06'},
                                         {'description': ' נספח ד',
                                          'link': 'http://www.mr.gov.il/Files_Michrazim/240174.pdf',
                                          'update_time': '2017-06-12'},
                                         {'description': ' נספח ד',
                                          'link': 'http://www.mr.gov.il/Files_Michrazim/240173.pdf',
                                          'update_time': '2017-06-12'},
                                         {'description': ' נספח ד',
                                          'link': 'http://www.mr.gov.il/Files_Michrazim/240172.docx',
                                          'update_time': '2017-06-12'},
                                         {'description': ' מכרז מס',
                                          'link': 'http://www.mr.gov.il/Files_Michrazim/240171.docx',
                                          'update_time': '2017-06-12'},
                                         {'description': 'מודעה לעיתונות בערבית',
                                          'link': 'http://www.mr.gov.il/Files_Michrazim/240170.docx',
                                          'update_time': '2017-06-12'},
                                         {'description': 'מודעה לעיתונות בעברית',
                                          'link': 'http://www.mr.gov.il/Files_Michrazim/240169.docx',
                                          'update_time': '2017-06-12'},
                                         {'description': 'מודעה לעיתונות בערבית',
                                          'link': 'http://www.mr.gov.il/Files_Michrazim/239176.DOCX',
                                          'update_time': '2017-05-28'},
                                         {'description': ' מכרז ניהול אירועי ה70 לישראל',
                                          'link': 'http://www.mr.gov.il/Files_Michrazim/239149.docx',
                                          'update_time': '2017-05-28'},
                                         {'description': 'מודעה לעיתונות בעברית',
                                          'link': 'http://www.mr.gov.il/Files_Michrazim/239148.docx',
                                          'update_time': '2017-05-28'}]}
    assert resource[1]["publication_id"] == 596915
