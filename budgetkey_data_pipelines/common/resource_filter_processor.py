from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher


class ResourceFilterProcessor(object):

    def __init__(self, ingest_response=None,
                 default_input_resource=None, default_output_resource=None, default_replace_resource=True,
                 table_schema=None, resource_filter=None):
        if not ingest_response:
            ingest_response = ingest()
        self.parameters, self.datapackage, self.resource_iterator = ingest_response
        self.set_default_parameters(default_input_resource, default_output_resource, default_replace_resource)
        self.resource_filter = resource_filter
        self.input_resource_matcher = ResourceMatcher(self.parameters["input_resource"])
        self.output_resource_name = self.parameters["output_resource"]
        self.output_resource_descriptor = {"name": self.output_resource_name,
                                           "path": "data/{}.csv".format(self.output_resource_name),
                                           "schema": table_schema}

    def set_default_parameters(self, default_input_resource, default_output_resource, default_replace_resource):
        self.parameters.setdefault("input_resource", default_input_resource)
        self.parameters.setdefault("output_resource", default_output_resource)
        self.parameters.setdefault("replace_resource", default_replace_resource)

    def filter_data(self):
        input_resource_data = None
        for resource_descriptor in self.datapackage["resources"]:
            resource_data = next(self.resource_iterator)
            if self.parameters["replace_resource"] and self.input_resource_matcher.match(resource_descriptor["name"]):
                input_resource_data = resource_data
            if resource_descriptor["name"] == self.parameters["output_resource"]:
                yield self.resource_filter(input_resource_data if input_resource_data else resource_data, self.parameters)
            else:
                yield resource_data

    def filter_datapackage(self):
        if self.parameters["replace_resource"]:
            for resource in self.datapackage["resources"]:
                if self.input_resource_matcher.match(resource["name"]):
                    resource.update(self.output_resource_descriptor)
        else:
            self.datapackage["resources"].append(self.output_resource_descriptor)

    def spew(self):
        self.filter_datapackage()
        spew(self.datapackage, self.filter_data())
