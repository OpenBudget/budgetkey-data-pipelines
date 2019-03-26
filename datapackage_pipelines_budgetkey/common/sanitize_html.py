def sanitize_html(container_el):
    if container_el:
        for el in container_el.find('*'):
            if el.tag == 'a':
                href = el.attrib.get('href')
                el.attrib.clear()
                el.attrib['href'] = href
                el.attrib['target'] = '_blank'
            else:
                el.attrib.clear()
        return container_el.html()
    return ''

