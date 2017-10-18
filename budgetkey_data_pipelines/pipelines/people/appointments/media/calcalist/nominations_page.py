import logging

from pyquery import PyQuery as pq
from urllib.parse import urljoin


class NominationsPage(object):

    def __init__(self, current_url, base_url, page_source):
        self.page = pq(page_source)
        self.base_url = base_url
        self.proof_url = current_url

    def next_url(self):
        next_relative_url = self._next_relative_url()
        return urljoin(self.base_url, next_relative_url)

    def _next_relative_url(self):
        iframe_links = self.page('.Comments_Pagging')('td')('a')
        logging.info('iframe_links: %s' % iframe_links)
        for iframe_link in iframe_links.items():
            link = IFrameLink(iframe_link)
            if link.has_element('הבא'):
                return link.url()

        return None

    def nominations(self):
        logging.info('Extracting nominations')

        nominations = []
        nominations_area = self.page('.MainArea_Inner')
        for inner in nominations_area.items():
            nomination_tables = inner('table')
            for i, nomination_table_element in enumerate(nomination_tables.items()):
                if nomination_table_element.attr['class'] is None:
                    nomination = self._extract_nomination(nomination_table_element)
                    logging.info('%d) Adding nomination: %s' % (i, nomination))
                    nominations.append(nomination)

        return nominations

    def _extract_nomination(self, table_element):
        return {
            'date': table_element('.Nom_Date').html(),
            'full_name': table_element('.Nom_Title').text(),
            'company': table_element('.Nom_Comp').text(),
            'description': table_element('.Nom_SubTitle').text(),
            'proof_url': self.proof_url

        }


class IFrameLink(object):

    def __init__(self, iframe_link):
        self.link = iframe_link
        self.link_desc = iframe_link.text()

    def has_element(self, element):
        return self.link_desc.find(element) == 0

    def url(self):
        return self.link.attr['href']


