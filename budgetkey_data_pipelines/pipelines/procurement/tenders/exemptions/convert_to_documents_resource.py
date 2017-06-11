from budgetkey_data_pipelines.common.resource_filter_processor import ResourceFilterProcessor
import json


OUTPUT_TABLE_SCHEMA = {
    "primaryKey": ["publisher_id", "publication_id", "i"],
    "fields": [
        {"name": "publisher_id", "type": "integer", "required": True},
        {"name": "publication_id", "type": "integer", "required": True},
        {"name": "i", "type": "integer", "required": True},
        {"name": "description", "type": "string", "required": True},
        {"name": "link", "type": "string", "format": "uri", "required": True},
        {"name": "update_time", "type": "string", "required": True}
    ],
    # this doesn't seem to work well with the dump.to_sql at the moment
    # it tries to create a foreign key in SQL but fails
    # "foreignKeys": [
    #     {
    #         "fields": ["publisher_id", "publication_id"],
    #         "reference": {
    #             "datapackage": "procurement-tenders-exemptions",
    #             "resource": "exemptions",
    #             "fields": ["publisher_id", "publication_id"]
    #         }
    #     }
    # ]
}


class ConvertToDocumentsResource(ResourceFilterProcessor):

    def __init__(self, **kwargs):
        super(ConvertToDocumentsResource, self).__init__(default_input_resource="exemptions",
                                                         default_output_resource="exemption-documents",
                                                         default_replace_resource=True,
                                                         table_schema=OUTPUT_TABLE_SCHEMA, **kwargs)

    def filter_resource_data(self, data, parameters):
        for exemption in data:
            documents = json.loads(exemption["documents_json"])
            for i, document in enumerate(documents):
                yield {
                    "publisher_id": int(exemption["publisher_id"]),
                    "publication_id": int(exemption["publication_id"]),
                    "i": i,
                    "description": document["description"],
                    "link": document["link"],
                    "update_time": document["update_time"],
                }


if __name__ == "__main__":
    ConvertToDocumentsResource.main()
