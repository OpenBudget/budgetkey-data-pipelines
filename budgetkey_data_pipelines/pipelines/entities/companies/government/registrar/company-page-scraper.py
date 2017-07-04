import logging
import re
import tempfile
from lxml.html import HtmlElement

from selenium import webdriver
from selenium.webdriver.remote.remote_connection import LOGGER
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from pyquery import PyQuery as pq

from datapackage_pipelines.wrapper import ingest, spew

import textract
import requests


LOGGER.setLevel(logging.WARNING)
logging.getLogger().setLevel(logging.INFO)

parameters, datapackage, res_iter = ingest()


head_data_label_to_key = {
    'תחום פעילות:': 'activity_field',
    'משרד אחראי:': 'ministry_responsible',
    'תחום עיסוק עיקרי:': 'main_area_of_activity',
    'אתר החברה:': 'company_website',
    'סיווג:': 'classification'
}

company_number_labels = [
    "מס' החברה ברשם החברות",
    "מספר החברה ברשם החברות",
    "מס' ברשם החברות"
]

string_headers = sorted(head_data_label_to_key.values()) + ['num_of_state_directors', 'company_number']
array_headers = ['directors_in_company', 'directors_of_state', 'directors_not_by_state', 'directors_candidates']
headers = string_headers + array_headers
hostname = 'http://mof.gov.il'


def get_href(href):
    if href is None:
        return href
    else:
        return hostname + href


def get_key(key_text):
    if key_text not in head_data_label_to_key:
        logging.warning('Cannot find key for label %r ' % key_text)
        return
    return head_data_label_to_key[key_text]


def get_value(key, value):
    # Fix problem with websites that are duplicated with new line character between.
    if key == 'company_website' and '\n' in value:
        return value[0:value.index('\n')]
    # Fix address for the company actual site instead of website
    if key == 'company_website' and "\u0590" <= value[0] <= "\u05EA":
        return None
    return value


def get_company_head_data_item(company_head_data_item):
    data = {}
    company_head_data_item_pq = pq(company_head_data_item)
    item_children = company_head_data_item_pq.children()
    if not item_children:
        return None
    first_child = item_children[0]
    second_child = item_children[1]
    key = get_key(pq(first_child).text())
    value = get_value(key, pq(second_child).text())
    if value is None:
        return None
    data['key'] = key
    data['value'] = value
    return data


def get_next_sibling(node):
    node_siblings = node.siblings()
    if node_siblings:
        return get_first_html_element(node_siblings)


def get_all_sibling(node):
    siblings = []
    node_siblings = node.siblings()
    if node_siblings:
        for sib in node_siblings:
            if isinstance(sib, HtmlElement):
                siblings += [sib]
    return siblings


def get_first_html_element(nodes):
    for node in nodes:
        if isinstance(node, HtmlElement):
            return node


def get_all_children(node):
    children = []
    node_children = pq(node).children()
    if node_children:
        for child in node_children:
            if isinstance(child, HtmlElement):
                children += [child]
    return children


def get_director_of_state(directors_of_state_elem):
    director_of_state = {}
    director_of_state_href_elem = pq(directors_of_state_elem.find('a'))
    director_of_state_href = director_of_state_href_elem.attr('href')
    director_of_state_last_name = director_of_state_href_elem.children()[0].text
    director_of_state_first_name = director_of_state_href_elem.children()[1].text
    director_of_state['href'] = get_href(director_of_state_href)
    director_of_state['first_name'] = director_of_state_first_name.strip()
    director_of_state['last_name'] = director_of_state_last_name.strip()
    return director_of_state


def get_director_in_company(directors_in_company_href_wrapper):
    director_in_company = {}
    director_in_company_href_element = pq(directors_in_company_href_wrapper.find('a'))
    director_in_company_href = director_in_company_href_element.attr('href')
    director_in_company_role = director_in_company_href_element.find('.role').text()
    director_in_company_last_name = director_in_company_href_element.children()[1].text
    director_in_company_first_name = director_in_company_href_element.children()[2].text
    director_in_company['href'] = get_href(director_in_company_href)
    director_in_company['first_name'] = director_in_company_first_name.strip()
    director_in_company['last_name'] = director_in_company_last_name.strip()
    director_in_company['role'] = director_in_company_role.strip()
    return director_in_company


def get_director_not_by_state(directors_not_by_state_elem):
    director_not_by_state = {}
    director_not_by_state_elem_children = pq(directors_not_by_state_elem).children()
    director_not_by_state_last_name = director_not_by_state_elem_children[0].text
    director_not_by_state_first_name = director_not_by_state_elem_children[1].text
    director_not_by_state['first_name'] = director_not_by_state_first_name.strip()
    director_not_by_state['last_name'] = director_not_by_state_last_name.strip()
    return director_not_by_state


def get_director_candidate(directors_candidate_elem):
    director_candidate = {}
    director_candidate_elem_children = pq(directors_candidate_elem).children()
    director_candidate_last_name = director_candidate_elem_children[0].text
    director_candidate_first_name = director_candidate_elem_children[1].text
    director_candidate['first_name'] = director_candidate_first_name.strip()
    director_candidate['last_name'] = director_candidate_last_name.strip()
    return director_candidate


def get_company_data(company_page_data_pq):
    element = {}
    company_head_data = company_page_data_pq.find('.HeadData')

    for company_head_data_item in company_head_data.children():
        data_item = get_company_head_data_item(company_head_data_item)
        if data_item is not None and data_item['value'] is not None:
            element[data_item['key']] = data_item['value']

    company_directors_data = company_page_data_pq.find('.DirectorsData')

    num_of_state_directors_label = company_directors_data.find('.NumberOfDirectors')
    num_of_state_directors_value = get_next_sibling(num_of_state_directors_label)
    if num_of_state_directors_value is not None:
        element['num_of_state_directors'] = num_of_state_directors_value.text

    directors_in_company_node = company_directors_data.find('.DirectorsInConpany')
    directors_in_company_next_sibling = get_next_sibling(directors_in_company_node)

    directors_in_company = []
    if directors_in_company_next_sibling is not None:
        directors_in_company_elem_list = get_all_children(directors_in_company_next_sibling)
        for directors_in_company_href_wrapper in directors_in_company_elem_list:
            directors_in_company.append(get_director_in_company(directors_in_company_href_wrapper))

    element['directors_in_company'] = directors_in_company

    directors_of_the_state_label = company_directors_data.find('.DirectorsOfTheState')
    directors_of_state = []
    if directors_of_the_state_label is not None and directors_of_the_state_label.html() is not None:
        directors_of_the_state_list = get_all_sibling(directors_of_the_state_label)
        for directors_of_the_state_elem in directors_of_the_state_list:
            directors_of_state.append(get_director_of_state(directors_of_the_state_elem))

    element['directors_of_state'] = directors_of_state

    directors_not_by_state_label = company_directors_data.find('.DirectorsNotByTheState')
    directors_not_by_state = []
    if directors_not_by_state_label is not None and directors_not_by_state_label.html() is not None:
        element['directors_not_by_state_last_update'] = directors_not_by_state_label.text().split(' ')[-1].replace(':', '')
        directors_not_by_state_list = get_all_sibling(directors_not_by_state_label)
        for directors_not_by_state_elem in directors_not_by_state_list:
            directors_not_by_state.append(get_director_not_by_state(directors_not_by_state_elem))

    element['directors_not_by_state'] = directors_not_by_state

    directors_candidates_label = company_page_data_pq.find('.DirectorsData .Candidate')
    directors_candidates = []
    if directors_candidates_label is not None and directors_candidates_label.html() is not None:
        directors_candidates_list = get_all_sibling(directors_candidates_label)
        for directors_candidate_elem in directors_candidates_list:
            directors_candidates.append(get_director_candidate(directors_candidate_elem))
    element['directors_candidates'] = directors_candidates

    return element


def get_company_number(text):
    return re.findall(r'\d+', text)[0]


def get_company_number_title_index(text):
    for label in company_number_labels:
        index = text.find(label)
        if index >= 0:
            return index


def get_last_activity_report_data(last_activity_report_href):
    data = {}
    response = requests.get(last_activity_report_href)
    temp_file = tempfile.NamedTemporaryFile(suffix='.pdf')
    temp_file.write(response.content)
    text = textract.process(temp_file.name).decode('utf-8')
    temp_file.close()

    title_index = get_company_number_title_index(text)

    new_line_index = text.find("\n", title_index)
    data['company_number'] = get_company_number(text[title_index:new_line_index])
    return data


def scrape_company_details(cmp_recs):
    logging.info('PREPARING')
    # Prepare Driver
    # driver = webdriver.Chrome()

    driver = webdriver.Remote(
        command_executor='http://tzabar.obudget.org:8910',
        desired_capabilities=DesiredCapabilities.PHANTOMJS)
    driver.set_window_size(1200, 800)
    for i, cmp_rec in enumerate(cmp_recs):
        logging.debug('GETTING DATA FOR COMPANY: ' + cmp_rec['name'])
        driver.get(cmp_rec['href'])

        page = pq(driver.page_source)
        company_page_data_pq = pq(page.find('.CompanyPage > div'))

        company_data = get_company_data(company_page_data_pq)
        if cmp_rec['last_activity_report_href'] is not None:
            last_activity_report_data = get_last_activity_report_data(cmp_rec['last_activity_report_href'])
            if last_activity_report_data is not None and last_activity_report_data['company_number'] is not None:
                cmp_rec['company_number'] = last_activity_report_data['company_number']

        for header in headers:
            if header in company_data:
                cmp_rec[header] = company_data[header]

        logging.debug('DATA: ' + str(cmp_rec.__dict__['_inner']))

        yield cmp_rec


def process_resources(res_iter_):
    item = next(res_iter_)
    yield scrape_company_details(item)
    for res in res_iter_:
        yield res


resource = datapackage['resources'][0]
for h in string_headers:
    resource['schema']['fields'].append({
        'name': h,
        'type': 'string'
    })
for h in array_headers:
    resource['schema']['fields'].append({
        'name': h,
        'type': 'array'
    })

spew(datapackage, process_resources(res_iter))
