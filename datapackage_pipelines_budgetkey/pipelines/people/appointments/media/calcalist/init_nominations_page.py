from pyquery import PyQuery as pq
from urllib.parse import urljoin


class InitNominationsPage(object):

    def __init__(self, base_url, page_source):
        self.page = pq(page_source)
        self.base_url = base_url

    def nominations_url(self):
        relative_url = self._relative_url()
        return urljoin(self.base_url, relative_url)

    def _relative_url(self):
        nominations_iframe = self.page('#CdaNominationList')
        return nominations_iframe.attr['src']

