import requests
from pyquery import PyQuery as pq
import math
import logging
from requests.exceptions import HTTPError, ConnectionError
import time


logger = logging.getLogger(__name__)


class PublisherScraper(object):

    def __init__(self, publisher_id,
                 # seconds to wait between retries of http requests
                 wait_between_retries=10,
                 # seconds to wait between two http requests
                 wait_between_requests=0,
                 # maximum number of retries - once reached entire process fails
                 max_retries=3,
                 # http request timeout in seconds
                 timeout=60,
                 # maximum number of results pages to fetch, default is to get all results
                 max_pages=-1,
                 # support different tender types
                 tender_type="exemptions"):
        self._publisher_id = publisher_id
        self._wait_between_retries = wait_between_retries
        self._max_retries = max_retries
        self._timeout = timeout
        self._max_pages = max_pages
        self._wait_between_requests = wait_between_requests
        self._tender_type = tender_type
        if self._tender_type == "office":
            self._elts_guid = "7b9ec4d5_a2ee_4fc4_8523_cd0a65f2d290"
            self._base_exemption_message_url = "/officestenders/Pages/officetender.aspx?pID="
            self._search_page_url = "https://www.mr.gov.il/OfficesTenders/Pages/SearchOfficeTenders.aspx"
        else:
            self._elts_guid = "cf609c81_a070_46f2_9543_e90c7ce5195b"
            self._base_exemption_message_url = "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID="
            self._search_page_url = "http://www.mr.gov.il/ExemptionMessage/Pages/SearchExemptionMessages.aspx"

    def get_urls(self):
        self._initialize_session()
        self._page = pq(self._get_page_text_retry())
        # need to start at -1 because we post the first page twice -
        # once to get the initial request data (but without the publisher urls)
        # then another time to get the actual first page data
        self._cur_page_num = 0
        while self._has_next_page():
            retries = 0
            while retries < self._max_retries:
                try:
                    for url in self._get_next_page_urls():
                        yield url
                    break
                except TooManyFailuresException:
                    retries += 1
                    if retries < self._max_retries:
                        logger.error("Failed to get page URLS, reinitializing session (attempt %d)", retries)
                        self._initialize_session()
                        self._cur_page_num -= 1
                    else:
                        logger.error("Failed to get page URLS for publisher %d, got %d / %d",
                                     self._publisher_id, self._cur_page_num, self._max_pages)
                        self._max_pages = self._cur_page_num

    def _initialize_session(self):
        self._session = requests.Session()

    def _get_next_page_urls(self):
        self._cur_page_num += 1
        self._post_next_page(self._publisher_id)
        for a_elt in self._page("#ctl00_m_g_{}_ctl00_grvMichrazim a".format(self._elts_guid)):
            href = a_elt.attrib.get("href", "")
            if href.startswith(self._base_exemption_message_url):
                yield href

    def _get_page_text(self, form_data=None):
        time.sleep(self._wait_between_requests)
        response = self._session.request("POST" if form_data else "GET",
                                         timeout=self._timeout,
                                         url=self._search_page_url,
                                         data=form_data)
        response.raise_for_status()
        return response.text

    def _get_page_text_retry(self, form_data=None):
        i = 0
        while True:
            try:
                return self._get_page_text(form_data)
            except (HTTPError, ConnectionError, requests.exceptions.ReadTimeout) as e:
                i += 1
                if i > self._max_retries:
                    raise TooManyFailuresException("too many failures, last exception message: {}".format(e))
                else:
                    logger.error("Failed to get page text, attempt %d (%s)", i, e)
                    time.sleep(self._wait_between_retries)

    def _has_next_page(self):
        if self._cur_page_num < 1:
            return True
        elif 0 < self._max_pages <= self._cur_page_num:
            return False
        else:
            num_pages = self._get_num_pages()
            return self._cur_page_num < num_pages

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
        return int(math.ceil(total_results / records_per_page))

    def _get_form_data(self, publisher_id):
        """
        collects data from the input and select fields on the current page
        :param publisher_id: optionally - set the publisher id select box to this value
        :return: dict of data gathered from the input and select fields
        """
        form_data = {}
        # input elementss
        for input_elt in self._page("#aspnetForm input"):
            if input_elt.attrib.get("name") and input_elt.attrib.get("value"):
                form_data[input_elt.attrib["name"]] = input_elt.attrib["value"]
        # select boxes
        for select_elt in self._page("#WebPartWPQ3 select"):
            if select_elt.attrib.get("name"):
                if publisher_id and select_elt.attrib["name"].endswith("$ddlPublisher"):
                    # set the publisher id
                    form_data[select_elt.attrib["name"]] = publisher_id
                elif select_elt.attrib["name"]:
                    form_data[select_elt.attrib["name"]] = 0
        # some more input elements
        for input_elt in self._page("#WebPartWPQ3 input"):
            if input_elt.get("name"):
                form_data[input_elt.attrib["name"]] = ""
        return form_data

    def _get_next_page_post_data(self, publisher_id=None):
        """
        return dict with post data for getting the next page
        :param publisher_id:
        :return: dict
        """
        # form_data is the input and select values collected from the current page
        form_data = self._get_form_data(publisher_id)
        # we use some of this collected data in the post_data dict below
        post_data = {
            "MSOWebPartPage_PostbackSource": "",
            "MSOTlPn_SelectedWpId": "",
            "MSOTlPn_View": "0",
            "MSOTlPn_ShowSettings": "False",
            "MSOGallery_SelectedLibrary": "",
            "MSOGallery_FilterString": "",
            "MSOTlPn_Button": "none",
            "__EVENTTARGET": "" if self._cur_page_num == 1 else "ctl00$m$g_{}$ctl00$grvMichrazim$ctl13$lnkNext".format(self._elts_guid),
            "__EVENTARGUMENT": "",
            "__REQUESTDIGEST": form_data["__REQUESTDIGEST"],
            "MSOSPWebPartManager_DisplayModeName": "Browse",
            "MSOSPWebPartManager_ExitingDesignMode": "false",
            "MSOWebPartPage_Shared": "",
            "MSOLayout_LayoutChanges": "",
            "MSOLayout_InDesignMode": "",
            "_wpSelected": "",
            "_wzSelected": "",
            "MSOSPWebPartManager_OldDisplayModeName": "Browse",
            "MSOSPWebPartManager_StartWebPartEditingName": "false",
            "MSOSPWebPartManager_EndWebPartEditing": "false",
            "__VIEWSTATE": form_data["__VIEWSTATE"],
            "__VIEWSTATEGENERATOR": form_data["__VIEWSTATEGENERATOR"],
            "__VIEWSTATEENCRYPTED": "",
            "__EVENTVALIDATION": form_data["__EVENTVALIDATION"],
            "SearchFreeText": "חפש",
            "ctl00_PlaceHolderHorizontalNav_RadMenu1_ClientState": "",
            "ctl00$m$g_{}$ctl00$txtFree".format(self._elts_guid): "",
            "ctl00$m$g_{}$ctl00$ddlPublisher".format(self._elts_guid): str(publisher_id),
            "ctl00$m$g_{}$ctl00$ddlSubject".format(self._elts_guid): "0",
            "ctl00$m$g_{}$ctl00$ddlTakanot".format(self._elts_guid): "0",
            "ctl00$m$g_{}$ctl00$ddlHachlatot".format(self._elts_guid): "0",
            "ctl00$m$g_{}$ctl00$txtSuplier".format(self._elts_guid): "",
            "ctl00$m$g_{}$ctl00$txtPublishManofNum".format(self._elts_guid): "",
            "ctl00$m$g_{}$ctl00$dtStart$dtStartDate".format(self._elts_guid): "",
            "ctl00$m$g_{}$ctl00$dtEnd$dtEndDate".format(self._elts_guid): "",
            "ctl00$m$g_{}$ctl00$txtMinAmount".format(self._elts_guid): "",
            "ctl00$m$g_{}$ctl00$txtMaxAmount".format(self._elts_guid): "",
            "_wpcmWpid": "",
            "wpcmVal": "",
        }
        if self._cur_page_num == 1:
            # if this attribute exists when getting pages after the 1st page - you get an error page about bad request
            post_data["ctl00$m$g_{}$ctl00$btnSearch".format(self._elts_guid)] = "חפש"
        return post_data

    def _post_next_page(self, publisher_id):
        form_data = self._get_next_page_post_data(publisher_id)
        self._page = pq(self._get_page_text_retry(form_data))


class TooManyFailuresException(Exception):
    pass
