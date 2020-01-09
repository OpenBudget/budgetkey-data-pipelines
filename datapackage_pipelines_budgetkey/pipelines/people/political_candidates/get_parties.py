# coding=utf-8
import typing
from urllib.parse import urljoin

import requests
from dataflows import Flow, printer, update_resource, set_type
from pyquery import PyQuery as pq, PyQuery
from requests import Response

BASE_DOMAIN = "https://bechirot21.bechirot.gov.il"
BASE_URL_FOR_CANDIDATES = f"{BASE_DOMAIN}/election/Candidates/Pages/default.aspx"
MEMBER_BEHALF_OF_STRING = "מטעם מפלגת "


def get_party_data_from_party_page(link_to_party_page: str) -> dict:
    party_page_req = requests.get(link_to_party_page)
    if party_page_req.status_code != 200:
        raise ValueError(f"Can't load page {link_to_party_page}")

    data_party_dict: dict = {}
    data_of_party_page: bytes = party_page_req.content
    pq_of_page: PyQuery = pq(data_of_party_page.decode(party_page_req.encoding))

    # Get party ballot img
    party_img_url = ""
    party_img_items: PyQuery = pq_of_page.find("li.dfwp-item img")
    if party_img_items is not None:
        for party_img_item in party_img_items:
            if party_img_item is not None and "src" in party_img_item.attrib:
                img_url_part = party_img_item.attrib["src"]
                if img_url_part.startswith("/election/Candidates/PublishingImages"):
                    party_img_url = urljoin(BASE_URL_FOR_CANDIDATES, img_url_part)
                    break
    data_party_dict["party_img_url"] = party_img_url

    # Get party member list and order
    members: dict = {}
    for party_entry in pq_of_page.find("li.dfwp-item"):
        member_place_p = party_entry.find(".//div[@class=\"ListTdBrown\"]//p")
        member_name_div = party_entry.findall(".//div[@class=\"candidate\"]//div")
        if member_place_p is None or len(member_name_div) == 0:  # Sometimes we get some unwanted content
            continue

        member_from = ""
        if len(member_name_div) > 1:  # If we have more then one element then the second element is the member sub party
            member_from_text = member_name_div[1].text.strip()
            if member_from_text.startswith(MEMBER_BEHALF_OF_STRING):
                member_from = member_from_text[len(MEMBER_BEHALF_OF_STRING):]

        member_place: str = member_place_p.text
        member_name: str = member_name_div[0].text
        members[member_place] = {"member_name": member_name, "member_from": member_from}
    data_party_dict["members"] = members

    data_party_dict["agreement_doc_link"] = []
    for party_entry in pq_of_page.find("div.candidatesComments"):
        # Get agreements
        hrefs_for_agreements: list = party_entry.xpath(".//div//div//a")
        if len(hrefs_for_agreements) == 0:
            hrefs_for_agreements = party_entry.xpath(".//div//div//div//a")

        agreement_doc_links = set([])
        for href in hrefs_for_agreements:
            if "href" not in href.attrib:
                continue

            agreement_doc_link = urljoin(BASE_URL_FOR_CANDIDATES, href.attrib["href"])
            if "agreement_doc_link" in data_party_dict:
                agreement_doc_links.add(agreement_doc_link)
        data_party_dict["agreement_doc_link"] = list(
            agreement_doc_links)  # dataflows use tableschema which support list and not set

    # Get resign candidates
    data_party_dict["resign_candidates"] = ""
    for party_entry in pq_of_page.find("div.candidatesResign"):
        data_party_dict["resign_candidates"] = party_entry.text

    return data_party_dict


def get_all_parties_data() -> typing.Generator[dict, None, None]:
    req_of_candidates: Response = requests.get(BASE_URL_FOR_CANDIDATES)
    if req_of_candidates.status_code != 200:
        raise ValueError(f"Can't load page {BASE_URL_FOR_CANDIDATES}")

    data_of_page: bytes = req_of_candidates.content
    pq_of_page: PyQuery = pq(data_of_page.decode(req_of_candidates.encoding))
    for party_entry in pq_of_page.find("li.dfwp-item"):
        party_letter = party_entry.find(".//div[@class=\"listlettrs\"]//p")
        if party_letter is None:
            continue

        party_page_link_a = party_entry.find(".//div[@class=\"internallinkKnessetTitle\"]//a")
        if party_page_link_a is None:
            continue

        party_name: str = party_page_link_a.text
        base_party_dict = {"party_letter": party_letter.text, "party_name": party_name}
        party_page_link: str = urljoin(BASE_URL_FOR_CANDIDATES, party_page_link_a.attrib["href"])
        party_data = get_party_data_from_party_page(party_page_link)
        base_party_dict.update(party_data)
        yield base_party_dict


def flow(*_) -> Flow:
    return Flow(
        get_all_parties_data(),
        update_resource(
            None, **{'dpp:streaming': True, 'name': 'election_candidates21'}
        ),
        set_type('party_letter', type='string'),
        set_type('party_name', type='string'),
        set_type('party_img_url', type='string', default=''),
        set_type('resign_candidates', type='string', default=''),
        set_type('members', type='object', default=None),
        set_type('agreement_doc_link', type='array', default=[]),
    )


if __name__ == '__main__':
    Flow(
        flow(),
        printer(),
    ).process()
