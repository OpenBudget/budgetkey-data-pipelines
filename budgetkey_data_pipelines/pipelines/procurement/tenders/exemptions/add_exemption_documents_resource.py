from budgetkey_data_pipelines.common.resource_filter_processor import ResourceFilterProcessor
import json
from pyquery import PyQuery as pq
from .parse_exemption_data import DOCUMENTS_JSON_FIELD

INPUT_RESOURCE = "exemptions"
OUTPUT_RESOURCE = "exemption-documents"
TABLE_SCHEMA = {
    # we take the safe route here and use a combination of values to determine the primary key
    # I'm not sure how consistent the source data is, and how much we should rely on it for reference keys
    "fields": [
        {"name": "publisher_id", "type": "integer"},
        {"name": "page_url", "title": "exemption page url", "type": "string"},
        {"name": "publication_id", "type": "string"},
        {"name": 'description', "type": "string"},
        {"name": "link", "type": "string"},
        {"name": "update_time", "type": "string"},
    ],
    "foreignKeys": [
        {
            "fields": ["publisher_id", "page_url", "publication_id"],
            "reference": {
                "resource": "exemptions",
                "fields": ["publisher_id", "page_url", "publication_id"],
            }
        }
    ]
}


class ExemptionDocumentsResourceProcessor(ResourceFilterProcessor):

    def __init__(self, **kwargs):
        super(ExemptionDocumentsResourceProcessor, self).__init__(default_input_resource=INPUT_RESOURCE,
                                                                  default_output_resource=OUTPUT_RESOURCE,
                                                                  default_replace_resource=False,
                                                                  table_schema=TABLE_SCHEMA,
                                                                  **kwargs)

    def filter_resource_data(self, data, parameters):
        for exemption in data:
            for document in json.loads(exemption[DOCUMENTS_JSON_FIELD]):
                yield {"publisher_id": exemption["publisher_id"],
                       "page_url": exemption["page_url"],
                       "publication_id": exemption["publication_id"],
                       "description": ""}
            page = pq(exemption["data"])
            documents = []
            for update_time_elt, link_elt, img_elt in zip(page("#ctl00_PlaceHolderMain_pnl_Files .DLFUpdateDate"),
                                                          page("#ctl00_PlaceHolderMain_pnl_Files .MrDLFFileData a"),
                                                          page("#ctl00_PlaceHolderMain_pnl_Files .MrDLFFileData img")):
                documents.append({"description": img_elt.attrib.get("alt", ""),
                                  "link": "{}{}".format("http://www.mr.gov.il", link_elt.attrib.get("href", "")),
                                  "update_time": update_time_elt.text,})
            yield {"publisher_id": exemption["pid"],
                   "page_url": exemption["url"],
                   "publication_id": page("#ctl00_PlaceHolderMain_lbl_SERIAL_NUMBER").text(),
                   "description": page("#ctl00_PlaceHolderMain_lbl_PublicationName").text(),
                   "supplier_id": page("#ctl00_PlaceHolderMain_lbl_SupplierNum").text(),
                   "supplier": page("#ctl00_PlaceHolderMain_lbl_SupplierName").text(),
                   "contact": page("#ctl00_PlaceHolderMain_lbl_ContactPersonName").text(),
                   "publisher": page("#ctl00_PlaceHolderMain_lbl_PUBLISHER").text(),
                   "contact_email": page("#ctl00_PlaceHolderMain_lbl_ContactPersonEmail").text(),
                   "claim_date": page("#ctl00_PlaceHolderMain_lbl_ClaimDate").text(),
                   "last_update_date": page("#ctl00_PlaceHolderMain_lbl_UpdateDate").text(),
                   "reason": page("#ctl00_PlaceHolderMain_lbl_PtorReason").text(),
                   "source_currency": page("#ctl00_PlaceHolderMain_lbl_Currency").text(),
                   "regulation": page("#ctl00_PlaceHolderMain_lbl_Regulation").text(),
                   "volume": page("#ctl00_PlaceHolderMain_lbl_TotalAmount").text(),
                   "subjects": page("#ctl00_PlaceHolderMain_lbl_PublicationSUBJECT").text(),
                   "start_date": page("#ctl00_PlaceHolderMain_lbl_StartDate").text(),
                   "end_date": page("#ctl00_PlaceHolderMain_lbl_EndDate").text(),
                   "decision": page("#ctl00_PlaceHolderMain_lbl_Decision").text(),
                   "page_title": page("#ctl00_PlaceHolderMain_lbl_PublicationType").text(),
                   "documents_json": json.dumps(documents)}

if __name__ == "__main__":
    ParseExemptionDataProcessor.main()
