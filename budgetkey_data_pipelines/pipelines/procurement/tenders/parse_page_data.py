from budgetkey_data_pipelines.common.resource_filter_processor import ResourceFilterProcessor
from pyquery import PyQuery as pq
from datetime import datetime

TABLE_SCHEMA = {
    "primaryKey": ["publication_id"],
    "fields": [
        {"name": "publisher_id", "type": "integer", "required": True},
        {"name": "publication_id", "type": "integer", "required": True},
        {"name": "page_url", "title": "page url", "type": "string", "format": "uri", "required": True},
        {"name": 'description', "type": "string", "required": True},
        {"name": "supplier_id", "type": "string", "required": False},
        {"name": "supplier", "type": "string", "required": True},
        {"name": "contact", "type": "string", "required": False},
        {"name": "publisher", "type": "string", "required": True},
        {"name": "contact_email", "type": "string", "required": False},
        {"name": "claim_date", "type": "datetime", "required": False},
        {"name": "last_update_date", "type": "date", "required": True},
        {"name": "reason", "type": "string", "required": False},
        {"name": "source_currency", "type": "string", "required": True},
        {"name": "regulation", "type": "string", "required": True},
        {"name": "volume", "type": "number", "required": False, "groupChar": ","},
        {"name": "subjects", "type": "string", "required": True},
        {"name": "start_date", "type": "date", "required": True},
        {"name": "end_date", "type": "date", "required": True},
        {"name": "decision", "type": "string", "required": True},
        {"name": "page_title", "type": "string", "required": True},
        {"name": "documents", "type": "array", "required": True}
    ]
}

BASE_URL = "http://www.mr.gov.il"


def parse_date(s):
    if s is None or s.strip() == '':
        return None
    return datetime.strptime(s, "%d/%m/%Y").date()

def parse_datetime(s):
    if s is None or s.strip() == '':
        return None
    return datetime.strptime(s, "%H:%M %d/%m/%Y")


class ParsePageDataProcessor(ResourceFilterProcessor):

    def __init__(self, **kwargs):
        self._base_url = kwargs.pop("base_url", BASE_URL)
        super(ParsePageDataProcessor, self).__init__(default_input_resource="publisher-urls-downloaded-data",
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
                    update_time = update_time.isoformat()
                documents.append({"description": img_elt.attrib.get("alt", ""),
                                  "link": "{}{}".format(BASE_URL, link_elt.attrib.get("href", "")),
                                  "update_time": update_time})
            if parameters["tender_type"] == "exemptions":
                yield self.get_exemptions_data(row, page, documents)
            elif parameters["tender_type"] == "office":
                yield self.get_office_data(row, page, documents)

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
            "page_url": row["url"],
            "description": source_data["description"],
            "supplier_id": source_data["supplier_id"],
            "supplier": source_data["supplier"],
            "contact": source_data["contact"],
            "publisher": source_data["publisher"],
            "contact_email": source_data["contact_email"],
            "claim_date": parse_date(source_data["claim_date"]),
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
            "documents": documents
        }

    def get_office_data(self, row, page, documents):
        input_fields_text_map = {
            "publication_id": "SERIAL_NUMBER",
            "description": "PublicationName",
            "publisher": "Publisher",
            "claim_date": "ClaimDate",
            "last_update_date": "UpdateDate",
            "subjects": "PublicationSUBJECT",
            "publish_date": "PublishDate",
        }
        source_data = {
            k: page("#ctl00_PlaceHolderMain_lbl_{}".format(v)).text() for k, v in input_fields_text_map.items()}
        return {
            "publisher_id": int(row["pid"]),
            "publication_id": int(source_data["publication_id"]),
            "page_url": row["url"],
            "description": source_data["description"],
            "publisher": source_data["publisher"],
            "claim_date": parse_datetime(source_data["claim_date"]),
            "last_update_date": parse_date(source_data["last_update_date"]),
            "subjects": source_data["subjects"],
            "start_date": parse_date(source_data["publish_date"]),
            "documents": documents,
        }

if __name__ == "__main__":
    ParsePageDataProcessor.main()
