from pyquery import PyQuery as pq
from budgetkey_data_pipelines.common.resource_filter_processor import ResourceFilterProcessor


def resource_filter(resource_data, parameters):
    html_text = "\n".join([r["data"] for r in resource_data])
    options = pq(html_text)("#ctl00_m_g_cf609c81_a070_46f2_9543_e90c7ce5195b_ctl00_ddlPublisher").find("option")
    for option in options:
        publisher = {"id": int(option.attrib["value"]), "name": option.attrib["title"]}
        if publisher["id"] != 0:
            yield publisher


ResourceFilterProcessor(default_input_resource="mr-gov-il-search-exemption-messages",
                        default_output_resource="publishers",
                        default_replace_resource=True,
                        table_schema={"fields": [{"name": "id", "type": "integer"},
                                                 {"name": "name", "type": "string"}]},
                        resource_filter=resource_filter).spew()
