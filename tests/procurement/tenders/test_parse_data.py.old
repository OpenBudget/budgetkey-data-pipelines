import json

from datapackage_pipelines_budgetkey.pipelines.procurement.tenders.parse_page_data import ParsePageDataProcessor, object_storage
from ...common import listify_resources, unlistify_resources, assert_doc_conforms_to_schema
from .test_download_pages_data import get_mock_exemption_data
import os, tempfile, shutil


class MockParseExemptionDataProcessor(ParsePageDataProcessor):

    def spew(self):
        return self._get_spew_params()

    def requests_get_content(self, url):
        if url == "https://www.mr.gov.il/Files_Michrazim/201813.signed":
            filename = "Files_Michrazim_201813.signed"
        else:
            raise Exception("unknown url: {}".format(url))
        filename = os.path.join(os.path.dirname(__file__), "fixtures", filename)
        if not os.path.exists(filename):
            content = super(MockParseExemptionDataProcessor, self).requests_get_content(url)
            with open(filename, "wb") as f:
                f.write(content)
        with open(filename, "rb") as f:
            return f.read()

    def write_to_object_storage(self, object_name, data):
        return object_storage.urlfor(object_name)


def run_parse_processor(resource):
    datapackage = {"resources": [{
        "name": "tender-urls-downloaded-data",
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
    ingest_response = ({}, datapackage, resources)
    datapackage, resources, stats = MockParseExemptionDataProcessor(ingest_response=ingest_response).spew()
    resources = listify_resources(resources)
    assert len(resources) == 1
    return resources[0]


def docs(x):
    return json.dumps(x, sort_keys=True, ensure_ascii=False)

def test_parse_data():
    resource = run_parse_processor([
        {"pid": 71, "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=595431",
         "data": get_mock_exemption_data("595431"), "tender_type": "exemptions"},
        {"pid": 71, "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=594269",
         "data": get_mock_exemption_data("594269"), "tender_type": "exemptions"},
        {"pid": 50, "url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=574896",
         "data": get_mock_exemption_data("574896",
                                         url="https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=574896"),
         "tender_type": "exemptions"},
        {"pid": 50, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=598379",
         "data": get_mock_exemption_data("598379"), "tender_type": "office"},
        {"pid": 21, "url": "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=596915",
         "data": get_mock_exemption_data("596915"), "tender_type": "office"},
        {"pid": None, "url": "https://www.mr.gov.il/CentralTenders/Goods/Pages/3-2013.aspx",
         "data": get_mock_exemption_data("Goods-3-2013"), "tender_type": "central"},
        {"pid": None, "url": "https://www.mr.gov.il/CentralTenders/technology/Pages/15-2016.aspx",
         "data": get_mock_exemption_data("technology-15-2016"), "tender_type": "central"},
        {"pid": None, "url": "https://www.mr.gov.il/CentralTenders/Goods/Pages/19-2017.aspx",
         "data": get_mock_exemption_data("Goods-19-2017"), "tender_type": "central"},
        {"pid": None, "url": "https://www.mr.gov.il/CentralTenders/network/Pages/michraz3.aspx",
         "data": get_mock_exemption_data("network-michraz3"), "tender_type": "central"},
    ])
    assert len(resource) == 9
    assert resource[0] == {
        "publisher_id": 71,
        "publication_id": 595431,
        "page_url": "https://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID=595431",
        "description": """התקשרות עם ספקית תשתית התקשרות ואינטרנט וכן מתן שרותי תמיכה, סיוע לקוחות בכל אתרי החטיבה בארץ, גיבוי, אבטחת מידע, שרתי תקשורת וכיוצ"ב עפ"י המתואר במסמך המצורף. הנ"ל מהווה חסכון של כ- 300 אלף ש"ח לכל שנה""",
        "supplier_id": "510791734",
        "supplier": "לשכה לעיבוד נתונים של הסוכנות היהוד",
        "contact": "",
        "publisher": "ההסתדרות הציונית העולמית - החטיבה להתישבות",
        "contact_email": "",
        "claim_date": None,
        "last_update_date": '2017-03-14',
        "reason": "ספק יחיד בנסיבות העניין. לא היו השגות עד למועד שנקבע",
        "source_currency": "שקל",
        "regulation": "התקשרות עם ספק יחיד",
        "volume": "2,569,700",
        "subjects": "מחשוב ותקשורת",
        "start_date": '2017-03-01',
        "end_date": '2021-12-31',
        "decision": "נרשם",
        "page_title": "דיווח פטור משרדי",
        "tender_type": "exemptions",
        "tender_id": "none",
        "documents": docs([{"description": "חוות דעת מקצועית",
                            "link": "https://www.mr.gov.il/Files_Michrazim/234734.pdf",
                            "update_time": "2017-03-14"}])
    }
    assert resource[1]["publication_id"] == 594269
    assert resource[2]["publication_id"] == 574896
    unsigned_doc_link = "https://s3.amazonaws.com/budgetkey-files/procurement/tenders/201813.pdf"
    assert json.loads(resource[2]["documents"]) == [{"description": "חוות דעת מקצועית",
                                                     "link": unsigned_doc_link,
                                                     "update_time": "2015-12-31"}]
    assert resource[3] == {'publisher_id': 50,
                           'publication_id': 598379,
                           'page_url': 'https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=598379',
                           'description': 'לניהול אירועי שנת ה-70 למדינת ישראל - פרסום נוסף',
                           'publisher': 'משרד המדע התרבות והספורט',
                           'claim_date': '2017-07-19T12:00:0Z',
                           'last_update_date': '2017-07-09',
                           'subjects': 'שירותי עריכה,כתיבה ,עיצוב ,גרפיקה ואומנות; שירותי עריכה ותרגום; כתיבה יצירתית; שירותי הדפסה; שירותי הפקת וידאו; שירותים הקשורים לטלוויזיה',
                           'start_date': '2017-05-25',
                           "tender_id": "18/2017",
                           "tender_type": "office",
                           "decision": "עודכן",
                           'documents': docs([{'description': 'מענה לשאלות הבהרה מכרז פומבי 18/2017',
                                               'link': 'https://www.mr.gov.il/Files_Michrazim/242223.pdf',
                                               'update_time': '2017-07-09'},
                                              {'description': 'מענה לשאלות הבהרה מכרז פומבי 18/2017',
                                               'link': 'https://www.mr.gov.il/Files_Michrazim/242222.pdf',
                                               'update_time': '2017-07-09'},
                                              {'description': 'מודעה לעיתונות בערבית',
                                               'link': 'https://www.mr.gov.il/Files_Michrazim/242161.doc',
                                               'update_time': '2017-07-06'},
                                              {'description': 'מודעה לעיתונות בעברית',
                                               'link': 'https://www.mr.gov.il/Files_Michrazim/242160.doc',
                                               'update_time': '2017-07-06'},
                                              {'description': 'נספח ד',
                                               'link': 'https://www.mr.gov.il/Files_Michrazim/240174.pdf',
                                               'update_time': '2017-06-12'},
                                              {'description': 'נספח ד',
                                               'link': 'https://www.mr.gov.il/Files_Michrazim/240173.pdf',
                                               'update_time': '2017-06-12'},
                                              {'description': 'נספח ד',
                                               'link': 'https://www.mr.gov.il/Files_Michrazim/240172.docx',
                                               'update_time': '2017-06-12'},
                                              {'description': 'מכרז מס',
                                               'link': 'https://www.mr.gov.il/Files_Michrazim/240171.docx',
                                               'update_time': '2017-06-12'},
                                              {'description': 'מודעה לעיתונות בערבית',
                                               'link': 'https://www.mr.gov.il/Files_Michrazim/240170.docx',
                                               'update_time': '2017-06-12'},
                                              {'description': 'מודעה לעיתונות בעברית',
                                               'link': 'https://www.mr.gov.il/Files_Michrazim/240169.docx',
                                               'update_time': '2017-06-12'},
                                              {'description': 'מודעה לעיתונות בערבית',
                                               'link': 'https://www.mr.gov.il/Files_Michrazim/239176.DOCX',
                                               'update_time': '2017-05-28'},
                                              {'description': 'מכרז ניהול אירועי ה70 לישראל',
                                               'link': 'https://www.mr.gov.il/Files_Michrazim/239149.docx',
                                               'update_time': '2017-05-28'},
                                              {'description': 'מודעה לעיתונות בעברית',
                                               'link': 'https://www.mr.gov.il/Files_Michrazim/239148.docx',
                                               'update_time': '2017-05-28'}]),
                           'contact': None,
                           'contact_email': None,
                           'end_date': None,
                           'page_title': None,
                           'reason': None,
                           'regulation': None,
                           'source_currency': None,
                           'supplier': None,
                           'supplier_id': None,
                           'volume': None}
    assert resource[4]["publication_id"] == 596915
    assert resource[5] == {'claim_date': None,
                           'contact': 'אביתר פרץ;מנהל תחום התקשרויות;02-6663424',
                           'contact_email': None,
                           'decision': 'בתוקף',
                           'description': 'מכרז מרכזי מממ-03-2013 נועד להסדיר רכישה של סולר ומזוט '
                                          'על ידי כל משרדי הממשלה ויחידות הסמך בכל רחבי הארץ.',
                           'documents': docs([{'description': 'מסמכי המכרז',
                                               'link': '/officestenders/Pages/officetender.aspx?pID=542781',
                                               'update_time': None},
                                              {'description': 'הוראת שעה בתוקף 16.8.0.2 - אספקת סולר ומזוט למשרדי הממשלה',
                                               'link': 'http://www.mof.gov.il/takam/pages/horaot.aspx?k=16.8.0.2',
                                               'update_time': None}]),
                           'end_date': '2017-12-31',
                           'last_update_date': None,
                           'page_title': "מכרז 3-2013: אספקת סולר להסקה, סולר לתחבורה (עבור גנרטורים) ומזוט עבור משרדי הממשלה",
                           'page_url': 'https://www.mr.gov.il/CentralTenders/Goods/Pages/3-2013.aspx',
                           'publication_id': 542781,
                           'publisher': None,
                           'publisher_id': None,
                           'reason': None,
                           'regulation': 'מכרז',
                           'source_currency': None,
                           'start_date': None,
                           'subjects': 'דלקים ותוספי דלק, שמנים, חומרי סיכה וחומרים נגד חלודה (קורוזיה)',
                           'supplier': 'פז חברת דלק בע"מ',
                           'supplier_id': None,
                           'tender_id': 'Goods-3-2013',
                           'tender_type': 'central',
                           'volume': None}
    assert resource[6] == {'publisher_id': None, 'publication_id': 0, 'tender_type': 'central',
                           'page_url': 'https://www.mr.gov.il/CentralTenders/technology/Pages/15-2016.aspx',
                           'description': '', 'supplier_id': None, 'supplier': '',
                           'contact': 'ניסים בן-צרפתי;מנהל פרויקטים;02-6663439', 'publisher': None,
                           'contact_email': None, 'claim_date': None, 'last_update_date': None, 'reason': None,
                           'source_currency': None, 'regulation': 'מכרז', 'volume': None,
                           'subjects': 'מכשירי תקשורת ואביזרים; ציוד תקשורת ומולטימדיה; רכיבים לטכנולוגיות מידע ושידור',
                           'start_date': None, 'end_date': '2017-11-01', 'decision': 'עתידי',
                           'page_title': 'מכרז 15-2016: אספקת שירותי הקמה ואינטגרציה של חדרים חכמים',
                           'tender_id': 'technology-15-2016', 'documents': docs([])}
    assert resource[7] == {'publisher_id': None, 'publication_id': 598403, 'tender_type': 'central',
                           'page_url': 'https://www.mr.gov.il/CentralTenders/Goods/Pages/19-2017.aspx',
                           'description': '', 'supplier_id': None,
                           'supplier': 'כחלק מפעילות מינהל הרכש הממשלתי לייעול תהליכי הרכש בממשלה והרחבת היצע הספקים לממשלה ומגוון ספקיה, אנו שמחים לבשר על הקמת הקניון הדיגיטלי הממשלתי, מערכת רכש ממוחשבת חדשה אשר תשמש את משרדי הממשלה לצורך ביצוע הליכי תיחור ורכש ביעילות ובמהירות, ותאפשר בחירת זוכים בזמן מהיר.  מינהל הרכש הממשלתי באגף החשב הכללי במשרד האוצר יוצא במכרז למתן שירותי ייעוץ בתחום נגישות השירות למשרדי הממשלה ויחידות הסמך. במסגרת המכרז, תורכב על ידי מינהל הרכש הממשלתי רשימת ספקים רשומים, והמשרדים יצאו באופן עצמאי בפניות פרטניות לקבלת הצעות מחיר מהספקים הרשומים באמצעות הקניון הדיגיטלי הממשלתי.',
                           'contact': 'עמרי נצר;עורך מכרזים מרכזיים - יחידת הרכש הממשלתי בענבל;03-9779283',
                           'publisher': None, 'contact_email': None, 'claim_date': None, 'last_update_date': None,
                           'reason': None, 'source_currency': None, 'regulation': 'מכרז', 'volume': None,
                           'subjects': 'שירותי התוויית מדיניות וטיפול באזרחים; שירותים אישיים ומקומיים; שירותים למגזר הציבורי',
                           'start_date': None, 'end_date': None, 'decision': 'פורסם וממתין לתוצאות',
                           'page_title': 'מכרז 19-2017: מתן שירותי ייעוץ בתחום נגישות השירות למשרדי הממשלה ויחידות הסמך',
                           'tender_id': 'Goods-19-2017',
                           'documents': docs([{'description': 'מסמכי המכרז',
                                               'link': '/officestenders/Pages/officetender.aspx?pID=598403',
                                               'update_time': None}])}
    assert resource[8] == {'publisher_id': None, 'publication_id': 537982, 'tender_type': 'central',
                           'page_url': 'https://www.mr.gov.il/CentralTenders/network/Pages/michraz3.aspx',
                           'description': 'מכרז מממ-8-2009 לאספקת שירותי תקשורת בינלאומיים.', 'supplier_id': None,
                           'supplier': '', 'contact': 'ניסים בן-צרפתי;מנהל פרויקטים;02-6663439', 'publisher': None,
                           'contact_email': None, 'claim_date': None, 'last_update_date': None, 'reason': None,
                           'source_currency': None, 'regulation': 'מכרז', 'volume': None, 'subjects': 'שירותי תקשורת',
                           'start_date': None, 'end_date': None, 'decision': 'לא בתוקף',
                           'page_title': 'מכרז 8-2009: שירותי תקשורת בינ"ל', 'tender_id': 'network-michraz3',
                           'documents': docs([{'description': 'מסמכי המכרז',
                                               'link': '/officestenders/Pages/officetender.aspx?pID=537982',
                                               'update_time': None},
                                               {'description': 'הוראת שעה בתוקף 16.3.2',
                                               'link': 'http://hozrim.mof.gov.il/doc/hashkal/horaot.nsf/bynum/%d7%9e.16.3.2',
                                               'update_time': None}])}


def test_invalid_response():
    temp_dir = tempfile.mkdtemp(prefix="budgetkey-data-pipelines-tests-procurement-tenders")
    for tender_type in ["office", "central", "exemptions"]:
        for mock_data in ["invalid", "blocked"]:
            if tender_type == "office" or tender_type == "exemptions":
                url = "https://www.mr.gov.il/officestenders/Pages/officetender.aspx?pID=11111"
            elif tender_type == "central":
                url = "https://www.mr.gov.il/CentralTenders/technology/Pages/15-2016.aspx"
            ret = run_parse_processor([{"pid": 21, "url": url, "data": get_mock_exemption_data(mock_data),
                                        "tender_type": tender_type},])
            assert len(ret) == 0
    shutil.rmtree(temp_dir)
