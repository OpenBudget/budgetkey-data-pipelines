from datapackage_pipelines_budgetkey.common.resource_filter_processor import ResourceFilterProcessor
from datapackage_pipelines_budgetkey.pipelines.procurement.tenders.add_publisher_urls_resource import PUBLISHER_URLS_TABLE_SCHEMA
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
        self.found = set()

    def filter_datapackage(self):
        self.datapackage["resources"].append(self.output_resource_descriptor)
        return self.datapackage

    def filter_data(self):
        yield self.yield_tab_urls()

    def requests_get(self, url):
        return requests.get(url).text

    def yield_tab_urls(self):
        url = "https://www.mr.gov.il/CentralTenders/Pages/SearchTenders.aspx"
        page = pq(self.requests_get(url))
        i = 0
        for a in page('.ContentLayoutRightSide .liWithChildren a'):
            url = a.attrib.get('href')
            i += 1
            if url in self.found:
                continue
            self.found.add(url)
            yield dict(id=None, url=url, tender_type='central')
        if i == 0:
            raise Exception("got no data, something is broken")


if __name__ == "__main__":
    AddCentralUrlsResource.main()
