from pyquery import PyQuery as pq
from datapackage_pipelines_budgetkey.common.resource_filter_processor import ResourceFilterProcessor

def resource_filter(resource_data, parameters):
    html_text = "\n".join([r["data"] for r in resource_data])
    if parameters["tender_type"] == "office":
        ddlPublisher_element_id = "#ctl00_m_g_7b9ec4d5_a2ee_4fc4_8523_cd0a65f2d290_ctl00_ddlPublisher"
    else:
        ddlPublisher_element_id = "#ctl00_m_g_cf609c81_a070_46f2_9543_e90c7ce5195b_ctl00_ddlPublisher"
    options = pq(html_text)(ddlPublisher_element_id).find("option")
    for option in options:
        publisher = {"id": int(option.attrib["value"]), "name": option.attrib["title"]}
        if publisher["id"] != 0:
            yield publisher


if __name__ == "__main__":
    ResourceFilterProcessor(default_output_resource="publishers",
                            default_replace_resource=True,
                            table_schema={"fields": [{"name": "id", "type": "integer"},
                                                     {"name": "name", "type": "string"}]},
                            resource_filter=resource_filter).spew()
