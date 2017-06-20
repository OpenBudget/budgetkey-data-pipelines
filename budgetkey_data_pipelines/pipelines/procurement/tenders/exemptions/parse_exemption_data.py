from budgetkey_data_pipelines.common.resource_filter_processor import ResourceFilterProcessor
import json
from pyquery import PyQuery as pq
from datetime import datetime

INPUT_RESOURCE = "publisher-urls-downloaded-data"
OUTPUT_RESOURCE = "exemptions"
TABLE_SCHEMA = {
    # we take the safe route here and use a combination of values to determine the primary key
    # I'm not sure how consistent the source data is, and how much we should rely on it for reference keys
    "primaryKey": ["publication_id"],
    "fields": [
        {"name": "publisher_id", "type": "integer", "required": True},
        {"name": "publication_id", "type": "integer", "required": True},
        {"name": "page_url", "title": "exemption page url", "type": "string", "format": "uri", "required": True},
        {"name": 'description', "type": "string", "required": True},
        {"name": "supplier_id", "type": "string", "required": False},
        {"name": "supplier", "type": "string", "required": True},
        {"name": "contact", "type": "string", "required": False},
        {"name": "publisher", "type": "string", "required": True},
        {"name": "contact_email", "type": "string", "required": False},
        {"name": "claim_date", "type": "date", "required": False},
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
        # documents should be in a related table (there could be multiple documents per exemption)
        {"name": "documents", "type": "array", "required": True}
    ]
}
BASE_URL = "http://www.mr.gov.il"
INPUT_FIELDS_TEXT_MAP = {
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


class ParseExemptionDataProcessor(ResourceFilterProcessor):

    def __init__(self, **kwargs):
        self._base_url = kwargs.pop("base_url", BASE_URL)
        super(ParseExemptionDataProcessor, self).__init__(default_input_resource=INPUT_RESOURCE,
                                                          default_output_resource=OUTPUT_RESOURCE,
                                                          default_replace_resource=True,
                                                          table_schema=TABLE_SCHEMA,
                                                          **kwargs)

    def filter_resource_data(self, data, parameters):
        for exemption in data:
            page = pq(exemption["data"])
            documents = []
            for update_time_elt, link_elt, img_elt in zip(page("#ctl00_PlaceHolderMain_pnl_Files .DLFUpdateDate"),
                                                          page("#ctl00_PlaceHolderMain_pnl_Files .MrDLFFileData a"),
                                                          page("#ctl00_PlaceHolderMain_pnl_Files .MrDLFFileData img")):
                documents.append({"description": img_elt.attrib.get("alt", ""),
                                  "link": "{}{}".format(BASE_URL, link_elt.attrib.get("href", "")),
                                  "update_time": update_time_elt.text,})
            source_data = {
                k: page("#ctl00_PlaceHolderMain_lbl_{}".format(v)).text() for k, v in INPUT_FIELDS_TEXT_MAP.items()}
            yield {
                "publisher_id": int(exemption["pid"]),
                "publication_id": int(source_data["publication_id"]),
                "page_url": exemption["url"],
                "description": source_data["description"],
                "supplier_id": source_data["supplier_id"],
                "supplier": source_data["supplier"],
                "contact": source_data["contact"],
                "publisher": source_data["publisher"],
                "contact_email": source_data["contact_email"],
                "claim_date": datetime.strptime(source_data["claim_date"], "%d/%m/%Y").date() if source_data["claim_date"] else None,
                "last_update_date": datetime.strptime(source_data["last_update_date"], "%d/%m/%Y").date() if source_data["last_update_date"] else None,
                "reason": source_data["reason"],
                "source_currency": source_data["source_currency"],
                "regulation": source_data["regulation"],
                "volume": source_data["volume"],
                "subjects": source_data["subjects"],
                "start_date": datetime.strptime(source_data["start_date"], "%d/%m/%Y").date() if source_data["start_date"] else None,
                "end_date": datetime.strptime(source_data["end_date"], "%d/%m/%Y").date() if source_data["end_date"] else None,
                "decision": source_data["decision"],
                "page_title": source_data["page_title"],
                "documents": documents
            }

if __name__ == "__main__":
    ParseExemptionDataProcessor.main()
