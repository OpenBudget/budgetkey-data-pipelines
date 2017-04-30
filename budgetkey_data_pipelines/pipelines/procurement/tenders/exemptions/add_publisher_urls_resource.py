from budgetkey_data_pipelines.common.resource_filter_processor import ResourceFilterProcessor
import requests
from pyquery import PyQuery as pq


def filter(resource_data):
    for publisher in resource_data:
        search_params = {"publisher_index": publisher["id"]}
        # get the main search page
        session = requests.Session()
        response = session.request("GET", timeout=180, url="http://www.mr.gov.il/ExemptionMessage/Pages/SearchExemptionMessages.aspx")
        page = pq(response.text)
        # post the search form
        form_data = {'__EVENTTARGET': '', '__EVENTARGUMENT': '', }
        for input_elt in page("#aspnetForm input"):
            if "name" in input_elt.attrib and "value" in input_elt.attrib:
                    form_data[input_elt.attrib["name"]] = input_elt.attrib["value"]
        for select_elt in page("#WebPartWPQ3 select"):
            form_data[select_elt.attrib["name"]] = 0
        for input_elt in page("#WebPartWPQ3 input"):
            form_data[input_elt.attrib["name"]] = ""
        form_data['ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$ddlPublisher'] = search_params['publisher_index']
        # the clear button was not clicked
        form_data.pop('ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$btnClear')
        if form_data['__EVENTTARGET']:
            # if a page number was presses, the search button was not clicked
            form_data.pop('ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$btnSearch')
        response = session.request("POST", timeout=180, url="http://www.mr.gov.il/ExemptionMessage/Pages/SearchExemptionMessages.aspx", data=form_data)
        page = pq(response.text)
        records_range_str = page(".resultsSummaryDiv").text()
        # "tozaot 1-10 mitoch 100 reshumot
        if len(records_range_str.split(' ')) == 3:  # lo nimtzeu reshoomot
            result_indexes = {'range': [0, 0], 'total': 0}
        else:
            result_indexes = {'range': [int(x) for x in records_range_str.split(' ')[1].split('-')],
                             'total': int((records_range_str.split(' ')[3]))}
        records_per_page = 10
        total_pages = (result_indexes['total'] + (records_per_page - 1)) / records_per_page
        for page_num in range(1, int(total_pages) + 1):
            # web_page.go_to_page_num(page_num)
            urls = (e for e in page("#ctl00_m_g_cf609c81_a070_46f2_9543_e90c7ce5195b_ctl00_grvMichrazim a") if e.attrib.get("href", "").startswith("http://www.mr.gov.il/ExemptionMessage/Pages/ExemptionMessage.aspx?pID="))
            for url in urls:
                yield {"id": publisher["id"], "url": url}

ResourceFilterProcessor(default_input_resource="publishers",
                        default_output_resource="publisher-urls",
                        default_replace_resource=True,
                        table_schema={"fields": [{"name": "id", "type": "integer"},
                                                 {"name": "url", "type": "string"}]},
                        filter=filter).spew()
