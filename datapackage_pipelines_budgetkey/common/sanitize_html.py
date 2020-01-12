from pyquery import PyQuery as pq
import lxml.html.clean

cleaner = lxml.html.clean.Cleaner()


def sanitize_html(container_el):
    if container_el:
        container_el = pq(cleaner.clean_html(container_el.outerHtml()))
        for el in container_el.find('*'):
            if el.tag == 'a':
                href = el.attrib.get('href')
                el.attrib.clear()
                if href:
                    el.attrib['href'] = href
                    el.attrib['target'] = '_blank'
            else:
                el.attrib.clear()
        return container_el.html()
    return ''
