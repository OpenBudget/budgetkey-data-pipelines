from budgetkey_data_pipelines.common.resource_filter_processor import ResourceFilterProcessor
from budgetkey_data_pipelines.pipelines.procurement.tenders.add_publisher_urls_resource import PUBLISHER_URLS_TABLE_SCHEMA
import os
import logging
import requests
from pyquery import PyQuery as pq


TABLE_SCHEMA = PUBLISHER_URLS_TABLE_SCHEMA


class AddCentralUrlsResource(ResourceFilterProcessor):

    def __init__(self, **kwargs):
        super(AddCentralUrlsResource, self).__init__(default_output_resource="central",
                                                     table_schema=TABLE_SCHEMA,
                                                     default_replace_resource=False,
                                                     **kwargs)

    def filter_datapackage(self):
        self.datapackage["resources"].append(self.output_resource_descriptor)
        return self.datapackage

    def filter_data(self):
        yield self.yield_tab_urls()

    def requests_get(self, url):
        return requests.get(url).text

    def yield_tab_urls(self):
        results_per_page = 10
        max_pages = 50
        for start in range(1, max_pages*results_per_page + 1, results_per_page):
            url = "https://www.mr.gov.il/CentralTenders/Pages/SearchTenders.aspx?start1={s}&start2={s}&start3={s}&start4={s}".format(s=start)
            page = pq(self.requests_get(url))
            i = 0
            for a in page("a"):
                if a.attrib.get("id", "").endswith("_Title") and a.attrib.get("id", "").startswith("SRB_"):
                    yield {"id": None, "url": a.attrib.get("href"), "tender_type": "central"}
                    i += 1
            if i == 0:
                return
        raise Exception("got more then {} pages, this could be an indication that something is broken".format(max_pages))


if __name__ == "__main__":
    AddCentralUrlsResource.main()
