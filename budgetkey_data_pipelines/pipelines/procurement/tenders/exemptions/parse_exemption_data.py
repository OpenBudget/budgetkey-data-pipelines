from budgetkey_data_pipelines.common.resource_filter_processor import ResourceFilterProcessor
import json
from pyquery import PyQuery as pq

INPUT_RESOURCE = "publisher-urls-downloaded-data"
OUTPUT_RESOURCE = "exemptions"
DOCUMENTS_JSON_FIELD = "documents_json"
TABLE_SCHEMA = {
    # we take the safe route here and use a combination of values to determine the primary key
    # I'm not sure how consistent the source data is, and how much we should rely on it for reference keys
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
        # this resource is removed and added as a new resource in the add_exemption_documents_resource processor
        {"name": DOCUMENTS_JSON_FIELD, "type": "string"}
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
            exemption_data = {
                k: page("#ctl00_PlaceHolderMain_lbl_{}".format(v)).text() for k, v in INPUT_FIELDS_TEXT_MAP.items()}
            exemption_data.update({"publisher_id": exemption["pid"],
                                   "page_url": exemption["url"],
                                   "documents_json": json.dumps(documents)})
            yield exemption_data

if __name__ == "__main__":
    ParseExemptionDataProcessor.main()
