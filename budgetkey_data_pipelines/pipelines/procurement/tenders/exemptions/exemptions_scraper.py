import requests
from pyquery import PyQuery as pq
import math


class ExemptionsPublisherScraper(object):

    def __init__(self, publisher_id):
        self._publisher_id = publisher_id

    def get_urls(self):
        self._initialize_session()
        self._page = pq(self._get_search_page_text())
        self._cur_page_num = 0
        while self._has_next_page():
            for url in self._get_next_page_urls():
                yield url

    def _initialize_session(self):
        self._session = requests.Session()

    def _get_next_page_urls(self):
        self._cur_page_num += 1
        self._post_next_page(self._publisher_id)
        for a_elt in self._page("#ctl00_m_g_cf609c81_a070_46f2_9543_e90c7ce5195b_ctl00_grvMichrazim a"):
            base_exemption_message_url = "/ExemptionMessage/Pages/ExemptionMessage.aspx?pID="
            href = a_elt.attrib.get("href", "")
            if href.startswith(base_exemption_message_url):
                yield href

    def _get_search_page_text(self):
        return self._session.request("GET", timeout=180,
                                     url="http://www.mr.gov.il/ExemptionMessage/Pages/SearchExemptionMessages.aspx").text

    def _get_post_page_text(self, form_data):
        return self._session.request("POST", timeout = 180,
                                     url = "http://www.mr.gov.il/ExemptionMessage/Pages/SearchExemptionMessages.aspx",
                                     data = form_data).text

    def _has_next_page(self):
        if self._cur_page_num == 0:
            return True
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
        return math.ceil(total_results / records_per_page)

    def _get_next_page_form_data(self, publisher_id):
        form_data = {}
        # copy all the form data from the html input elements
        for input_elt in self._page("#aspnetForm input"):
            if input_elt.attrib.get("name") and input_elt.attrib.get("value"):
                form_data[input_elt.attrib["name"]] = input_elt.attrib["value"]
        for select_elt in self._page("#WebPartWPQ3 select"):
            if select_elt.attrib.get("name"):
                if select_elt.attrib["name"].endswith("$ddlPublisher"):
                    # set the publisher id
                    form_data[select_elt.attrib["name"]] = publisher_id
                elif select_elt.attrib["name"]:
                    form_data[select_elt.attrib["name"]] = 0
        for input_elt in self._page("#WebPartWPQ3 input"):
            if input_elt.get("name"):
                form_data[input_elt.attrib["name"]] = ""
        # hard-code the form_data to search next page and other required properties
        form_data.update({
                             "__EVENTTARGET": "" if self._cur_page_num == 1 else "ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$grvMichrazim$ctl13$lnkNext",
                             "__EVENTARGUMENT": "",
                             "ctl00_PlaceHolderHorizontalNav_RadMenu1_ClientState": "",
                             "ctl00$Accessibility$bigAccimg": "",
                             "ctl00$Accessibility$normalAccimg": "",
                             "ctl00$Accessibility$smallAccimg": "",
                             "ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$btnClear": "",
                             "ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$btnSearch": "\u05d7\u05e4\u05e9",
                             "SearchFreeText": "\u05d7\u05e4\u05e9"})
        # for some reason above code doesn't work
        # I found that hard-coding the values works better
        # while taking only what's needed from the auto-generated form_data
        form_data = {
            "MSOWebPartPage_PostbackSource": "",
            "MSOTlPn_SelectedWpId": "",
            "MSOTlPn_View": "0",
            "MSOTlPn_ShowSettings": "False",
            "MSOGallery_SelectedLibrary": "",
            "MSOGallery_FilterString": "",
            "MSOTlPn_Button": "none",
            "__EVENTTARGET": form_data["__EVENTTARGET"],
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
            "ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$txtFree": "",
            "ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$ddlPublisher": str(publisher_id),
            "ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$ddlSubject": "0",
            "ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$ddlTakanot": "0",
            "ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$ddlHachlatot": "0",
            "ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$txtSuplier": "",
            "ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$txtPublishManofNum": "",
            "ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$dtStart$dtStartDate": "",
            "ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$dtEnd$dtEndDate": "",
            "ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$txtMinAmount": "",
            "ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$txtMaxAmount": "",
            "ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$btnSearch": "חפש",
            "_wpcmWpid": "",
            "wpcmVal": "",
        }
        if self._cur_page_num != 1:
            form_data.pop("ctl00$m$g_cf609c81_a070_46f2_9543_e90c7ce5195b$ctl00$btnSearch")
        return form_data

    def _post_next_page(self, publisher_id):
        form_data = self._get_next_page_form_data(publisher_id)
        self._page = pq(self._get_post_page_text(form_data))