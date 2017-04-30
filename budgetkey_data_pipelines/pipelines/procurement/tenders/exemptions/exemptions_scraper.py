import requests
from pyquery import PyQuery as pq
from itertools import chain


class ExemptionsPublisherScraper(object):

    def __init__(self, publisher_id):
        self._publisher_id = publisher_id

    def get_urls(self):
        self._initialize_session()
        self._page = pq(self._get_search_page_text())
        self._post_search_form(self._publisher_id)
        self._next_page = 1
        return chain(*[self._get_next_page_urls() for _ in range(1, int(self._get_num_pages())+1)])

    def _initialize_session(self):
        self._session = requests.Session()

    def _get_next_page_urls(self):
        if self._next_page != 1:
            # first page was already fetched in the first _post_search_form call
            # for next pages we have to go to the next page
            self._post_search_form(self._publisher_id, next_page=True)
        for a_elt in self._page("#ctl00_m_g_cf609c81_a070_46f2_9543_e90c7ce5195b_ctl00_grvMichrazim a"):
            base_exemption_message_url = "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID="
            href = a_elt.attrib.get("href", "")
            if href.startswith(base_exemption_message_url):
                yield href

    def _get_search_page_text(self):
        return self._session.request("GET", timeout=180,
                                     url="http://www.mr.gov.il/ExemptionMessage/Pages/SearchExemptionMessages.aspx")

    def _get_post_page_text(self, form_data):
        return self._session.request("POST", timeout = 180,
                                     url = "http://www.mr.gov.il/ExemptionMessage/Pages/SearchExemptionMessages.aspx",
                                     data = form_data)

    def _get_num_pages(self):
        records_range_str = self._page(".resultsSummaryDiv").text()
        # "tozaot 1-10 mitoch 100 reshumot
        if len(records_range_str.split(' ')) == 3:  # lo nimtzeu reshoomot
            # results_range = [0, 0]
            total_results = 0
        else:
            # results_range = [int(x) for x in records_range_str.split(' ')[1].split('-')]
            total_results = int((records_range_str.split(' ')[3]))
        records_per_page = 10
        total_pages = (total_results + (records_per_page - 1)) / records_per_page
        return total_pages

    def _get_form_data(self, publisher_id, next_page=False):
        form_data = {'__EVENTTARGET': '', '__EVENTARGUMENT': '', }
        for input_elt in self._page("#aspnetForm input"):
            if "name" in input_elt.attrib and "value" in input_elt.attrib:
                form_data[input_elt.attrib["name"]] = input_elt.attrib["value"]
        for select_elt in self._page("#WebPartWPQ3 select"):
            form_data[select_elt.attrib["name"]] = 0
        for input_elt in self._page("#WebPartWPQ3 input"):
            form_data[input_elt.attrib["name"]] = ""
        form_data['ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$ddlPublisher'] = publisher_id
        # the clear button was not clicked
        form_data.pop('ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$btnClear')
        if form_data['__EVENTTARGET']:
            # if a page number was presses, the search button was not clicked
            form_data.pop('ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$btnSearch')
        return form_data

    def _post_search_form(self, publisher_id, next_page=False):
        form_data = self._get_form_data(publisher_id, next_page)
        self._page = pq(self._get_post_page_text(form_data))
