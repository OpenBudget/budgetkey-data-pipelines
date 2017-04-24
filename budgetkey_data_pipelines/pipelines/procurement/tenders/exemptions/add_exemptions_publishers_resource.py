from datapackage_pipelines.wrapper import ingest, spew
import logging
from pyquery import PyQuery as pq
from datapackage_pipelines.utilities.resource_matcher import ResourceMatcher


logger = logging.getLogger(__name__)


parameters, datapackage, resource_iterator = ingest()


parameters.setdefault("input_resource", "mr-gov-il-search-exemption-messages")
parameters.setdefault("output_resource", "publishers")
parameters.setdefault("replace_resource", True)

input_resource_matcher = ResourceMatcher(parameters["input_resource"])
output_resource_name = parameters["output_resource"]
output_resource_descriptor = {"name": output_resource_name,
                              "path": "data/{}.csv".format(output_resource_name),
                              "schema": {"fields": [{"name": "id", "type": "integer"},
                                                    {"name": "name", "type": "string"}]}}

def get_publishers_generator(resource_data):
    html_text = "\n".join([r["data"] for r in resource_data])
    options = pq(html_text)("#ctl00_m_g_cf609c81_a070_46f2_9543_e90c7ce5195b_ctl00_ddlPublisher").find("option")
    for option in options:
        publisher = {"id": int(option.attrib["value"]), "name": option.attrib["title"]}
        if publisher["id"] != 0:
            yield publisher

def scrape_publishers(resource_iterator):
    input_resource_data = None
    for resource_descriptor in datapackage["resources"]:
        resource_data = next(resource_iterator)
        if parameters["replace_resource"] and input_resource_matcher.match(resource_descriptor["name"]):
            input_resource_data = resource_data
        if resource_descriptor["name"] == parameters["output_resource"]:
            yield get_publishers_generator(input_resource_data if input_resource_data else resource_data)
        else:
            yield resource_data


if parameters["replace_resource"]:
    for resource in datapackage["resources"]:
        if input_resource_matcher.match(resource["name"]):
            resource.update(output_resource_descriptor)
else:
    datapackage["resources"].append(output_resource_descriptor)

spew(datapackage, scrape_publishers(resource_iterator))
