from budgetkey_data_pipelines.common.resource_filter_processor import ResourceFilterProcessor
from budgetkey_data_pipelines.pipelines.procurement.tenders.check_existing import tender_id_from_url
from pyquery import PyQuery as pq
from datetime import datetime

TABLE_SCHEMA = {
    "primaryKey": ["publication_id", "tender_type", "tender_id"],
    "fields": [
        {"name": "publisher_id", "type": "integer", "required": False},
        {"name": "publication_id", "type": "integer", "required": False, "description": "* exemptions: pId\n"
                                                                                        "* office: pId\n"
                                                                                        "* central: מספר המכרז במנוף"},
        {"name": "tender_type", "type": "string", "required": True},
        {"name": "page_url", "title": "page url", "type": "string", "format": "uri", "required": True},
        {"name": 'description', "type": "string", "required": True},
        {"name": "supplier_id", "type": "string", "required": False, "description": "* exemptions: ח.פ\n"
                                                                                    "* office: not used"},
        {"name": "supplier", "type": "string", "required": False, "description": "* exemptions: supplier company name\n"
                                                                                "* office: not used\n"
                                                                                 "* central: ספק זוכה"},
        {"name": "contact", "type": "string", "required": False, "description": "* exemptions: supplier contact name\n"
                                                                                "* office: not used\n"
                                                                                "* central: איש קשר"},
        {"name": "publisher", "type": "string", "required": False},
        {"name": "contact_email", "type": "string", "required": False, "description": "* exemptions: supplier contact email\n"
                                                                                      "* office: not used"},
        {"name": "claim_date", "type": "datetime", "required": False, "description": "* exemptions: תאריך אחרון להגשת השגות\n"
                                                                                     "* office: מועד אחרון להגשה"},
        {"name": "last_update_date", "type": "date", "required": False, "description": "* exemptions: תאריך עדכון אחרון\n"
                                                                                      "* office: תאריך עדכון אחרון"},
        {"name": "reason", "type": "string", "required": False, "description": "* exemptions: נימוקים לבקשת הפטור\n"
                                                                               "* office: not used"},
        {"name": "source_currency", "type": "string", "required": False},
        {"name": "regulation", "type": "string", "required": False, "description": "* exemptions: מתן הפטור מסתמך על תקנה\n"
                                                                                  "* office: not used\n"
                                                                                   "* central: סוג ההליך"},
        {"name": "volume", "type": "number", "required": False, "groupChar": ","},
        {"name": "subjects", "type": "string", "required": False, "description": "* exemptions: נושא/ים\n"
                                                                                "* office: נושא/ים\n"
                                                                                 "* central: נושא/ים"},
        {"name": "start_date", "type": "date", "required": False, "description": "* exemptions: תחילת תקופת התקשרות\n"
                                                                                "* office: תאריך פרסום"},
        {"name": "end_date", "type": "date", "required": False, "description": "* exemptions: תום תקופת ההתקשרות\n"
                                                                              "* office: not used\n"
                                                                               "* central: תאריך סיום תוקף מכרז"},
        {"name": "decision", "type": "string", "required": False, "description": "* exemptions: סטטוס החלטה\n"
                                                                                "* office: סטטוס\n"
                                                                                 "* central: סטטוס"},
        {"name": "page_title", "type": "string", "required": False, "description": "* exemptions: title of the exemption page\n"
                                                                                  "* office: not used"},
        {"name": "tender_id", "type": "string", "required": False, "description": "* exemptions: not used\n"
                                                                                  "* office: מספר המכרז"},
        {"name": "documents", "type": "array", "required": False}
    ]
}

BASE_URL = "http://www.mr.gov.il"


def parse_date(s):
    if s is None or s.strip() == '':
        return None
    try:
        return datetime.strptime(s, "%H:%M %d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        return datetime.strptime(s, "%d/%m/%Y").strftime("%Y-%m-%d")

def parse_datetime(s):
    if s is None or s.strip() == '':
        return None
    try:
        return datetime.strptime(s, "%H:%M %d/%m/%Y").strftime("%Y-%m-%dT%H:%M:0Z")
    except ValueError:
        return datetime.strptime(s, "%d/%m/%Y").strftime("%Y-%m-%dT%H:%M:0Z")


class ParsePageDataProcessor(ResourceFilterProcessor):

    def __init__(self, **kwargs):
        self._base_url = kwargs.pop("base_url", BASE_URL)
        super(ParsePageDataProcessor, self).__init__(default_input_resource="tender-urls-downloaded-data",
                                                     default_replace_resource=True,
                                                     table_schema=TABLE_SCHEMA,
                                                     **kwargs)

    def filter_resource_data(self, data, parameters):
        for row in data:
            page = pq(row["data"])
            documents = []
            for update_time_elt, link_elt, img_elt in zip(page("#ctl00_PlaceHolderMain_pnl_Files .DLFUpdateDate"),
                                                          page("#ctl00_PlaceHolderMain_pnl_Files .MrDLFFileData a"),
                                                          page("#ctl00_PlaceHolderMain_pnl_Files .MrDLFFileData img")):
                update_time = parse_date(update_time_elt.text.split()[-1])
                if update_time is not None:
                    update_time = update_time
                documents.append({"description": img_elt.attrib.get("alt", ""),
                                  "link": "{}{}".format(BASE_URL, link_elt.attrib.get("href", "")),
                                  "update_time": update_time})
            if row["tender_type"] == "exemptions":
                yield self.get_exemptions_data(row, page, documents)
            elif row["tender_type"] == "office":
                yield self.get_office_data(row, page, documents)
            elif row["tender_type"] == "central":
                yield self.get_central_data(row, page, documents)
            else:
                raise Exception("invalid tender_type: {}".format(row["tender_type"]))

    def get_exemptions_data(self, row, page, documents):
        input_fields_text_map = {
            "publication_id": "SERIAL_NUMBER",
            "description": "PublicationName",
            "supplier_id": "SupplierNum",
            "supplier": "SupplierName",
            "contact": "ContactPersonName",
            "publisher": "PUBLISHER",
            "contact_email": "ContactPersonEmail",
            "claim_date": "ClaimDate",
            "last_update_date": "UpdateDate",
            "reason": "PtorReason",
            "source_currency": "Currency",
            "regulation": "Regulation",
            "volume": "TotalAmount",
            "subjects": "PublicationSUBJECT",
            "start_date": "StartDate",
            "end_date": "EndDate",
            "decision": "Decision",
            "page_title": "PublicationType",
        }
        source_data = {
            k: page("#ctl00_PlaceHolderMain_lbl_{}".format(v)).text() for k, v in input_fields_text_map.items()}
        return {
            "publisher_id": int(row["pid"]),
            "publication_id": int(source_data["publication_id"]),
            "tender_type": "exemptions",
            "page_url": row["url"],
            "description": source_data["description"],
            "supplier_id": source_data["supplier_id"],
            "supplier": source_data["supplier"],
            "contact": source_data["contact"],
            "publisher": source_data["publisher"],
            "contact_email": source_data["contact_email"],
            "claim_date": parse_datetime(source_data["claim_date"]),
            "last_update_date": parse_date(source_data["last_update_date"]),
            "reason": source_data["reason"],
            "source_currency": source_data["source_currency"],
            "regulation": source_data["regulation"],
            "volume": source_data["volume"],
            "subjects": source_data["subjects"],
            "start_date": parse_date(source_data["start_date"]),
            "end_date": parse_date(source_data["end_date"]),
            "decision": source_data["decision"],
            "page_title": source_data["page_title"],
            "tender_id": "",
            "documents": documents
        }

    def get_office_data(self, row, page, documents):
        input_fields_text_map = {
            "publication_id": "SERIAL_NUMBER",
            "publishnum": "PublishNum",
            "description": "PublicationName",
            "publisher": "Publisher",
            "claim_date": "ClaimDate",
            "last_update_date": "UpdateDate",
            "subjects": "PublicationSUBJECT",
            "publish_date": "PublishDate",
            "status": "PublicationSTATUS"
        }
        source_data = {
            k: page("#ctl00_PlaceHolderMain_lbl_{}".format(v)).text() for k, v in input_fields_text_map.items()}
        return {
            "publisher_id": int(row["pid"]),
            "publication_id": int(source_data["publication_id"]),
            "tender_type": "office",
            "page_url": row["url"],
            "description": source_data["description"],
            "supplier_id": None,
            "supplier": None,
            "contact": None,
            "publisher": source_data["publisher"],
            "contact_email": None,
            "claim_date": parse_datetime(source_data["claim_date"]),
            "last_update_date": parse_date(source_data["last_update_date"]),
            "reason": None,
            "source_currency": None,
            "regulation": None,
            "volume": None,
            "subjects": source_data["subjects"],
            "start_date": parse_date(source_data["publish_date"]),
            "end_date": None,
            "decision": source_data["status"],
            "page_title": None,
            "tender_id": source_data["publishnum"],
            "documents": documents,
        }

    def get_central_data(self, row, page, documents):
        # michraz_number = page("#ctl00_PlaceHolderMain_MichraznumberPanel div.value").text().strip()
        documents = []
        for elt in page("#ctl00_PlaceHolderMain_SummaryLinksPanel_SummaryLinkFieldControl1__ControlWrapper_SummaryLinkFieldControl a"):
            documents.append({"description": elt.text,
                              "link": elt.attrib["href"],
                              "update_time": None})
        for elt in page("#ctl00_PlaceHolderMain_SummaryLinks2Panel"):
            documents.append({"description": pq(elt).text().strip(),
                              "link": pq(elt).find("a")[0].attrib["href"],
                              "update_time": None})
        publication_id = page("#ctl00_PlaceHolderMain_ManofSerialNumberPanel div.value").text().strip()
        return {
            "publisher_id": None,
            "publication_id": int(publication_id) if publication_id else 0,
            "tender_type": "central",
            "page_url": row["url"],
            "description": page("#ctl00_PlaceHolderMain_GovXContentSectionPanel_Richhtmlfield1__ControlWrapper_RichHtmlField").text().strip(),
            "supplier_id": None,
            "supplier": page("#ctl00_PlaceHolderMain_GovXParagraph1Panel_ctl00__ControlWrapper_RichHtmlField div").text().strip(),
            "contact": page("#ctl00_PlaceHolderMain_WorkerPanel_WorkerPanel1 div.worker").text().strip(),
            "publisher": None,
            "contact_email": None,
            "claim_date": None,
            "last_update_date": None,
            "reason": None,
            "source_currency": None,
            "regulation": page("#ctl00_PlaceHolderMain_MIchrazTypePanel div.value").text().strip(),
            "volume": None,
            "subjects": page("#ctl00_PlaceHolderMain_MMDCategoryPanel div.value").text().strip(),
            "start_date": None,
            "end_date": parse_date(page("#ctl00_PlaceHolderMain_TokefEndDatePanel div.Datevalue").text().strip()),
            "decision": page("#ctl00_PlaceHolderMain_MichrazStatusPanel div.value").text().strip(),
            "page_title": page("h1.MainTitle").text().strip(),
            "tender_id": tender_id_from_url(row["url"]),
            "documents": documents,
        }

if __name__ == "__main__":
    ParsePageDataProcessor.main()
