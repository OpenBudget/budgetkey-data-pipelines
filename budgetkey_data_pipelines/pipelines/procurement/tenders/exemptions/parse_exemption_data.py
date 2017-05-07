from budgetkey_data_pipelines.common.resource_filter_processor import ResourceFilterProcessor
import json
from pyquery import PyQuery as pq


class ParseExemptionDataProcessor(ResourceFilterProcessor):

    def __init__(self, **kwargs):
        super(ParseExemptionDataProcessor, self).__init__(
            default_input_resource="publisher-urls-downloaded-data",
            default_output_resource="exemptions",
            default_replace_resource=True,
            table_schema={
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
                    # TODO: figure out a better way to return the related documents
                    # ideally - would like to have the documents as a separate linked resource
                    # but I couldn't find a good way to do it while supporting streaming
                    {"name": "documents_json", "type": "string"}
                ]
            },
            **kwargs
        )

    def filter_resource_data(self, data, parameters):
        for exemption in data:
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
