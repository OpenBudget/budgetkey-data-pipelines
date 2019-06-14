from pyquery import PyQuery as pq
from collections import defaultdict

PARSER_VERSION = 1

def parse_maya_html_form(url):
    parser = MayaParser(url)
    return {
        'id': parser.id,
        'company': parser.company,
        'type': parser.type,
        'fix_for': parser.fix_for,
        'document': parser.document
    }


class MayaParser():
    def __init__(self, url):
        self.page = pq(url, parser='html')

    @property
    def id(self):
        elements = self.page.find('#HeaderProofValue')
        if len(elements) == 0:
            elements = self.page.find('#HeaderProof ~ span:first')
        if len(elements)==0:
            raise ValueError("Could not find אסמכתא in form")

        #Some Forms contain data duplication:
        #https://mayafiles.tase.co.il/RHtm/1072001-1073000/H1072716.htm
        values = set(pq(e).text().strip() for e in elements)
        if len(values) >1:
            raise ValueError("Found multiple אסמכתא in form")
        return next(iter(values))

    @property
    def company(self):
        elements = self.page.find('#HeaderEntityNameEB')
        if len(elements)==0:
            raise ValueError("Could not find company name in form")

        values = set(pq(e).text().strip() for e in elements)
        if len(values) >1:
            raise ValueError("Found multiple company names in form")
        return next(iter(values))


    @property
    def type(self):
        elements = self.page.find('#HeaderFormNumber')
        if len(elements)==0:
            raise ValueError("Could not find form type in form")

        values = set(pq(e).text().strip() for e in elements)
        if len(values) >1:
            raise ValueError("Found multiple form types in form")
        return next(iter(values))


    @property
    def fix_for(self):
        elements = self.page.find('#HeaderFixtReport')
        if len(elements)==0:
            return None

        values = set(pq(e).find("#HeaderProofFormat").text().strip() for e in elements)
        if len(values) >1:
            raise ValueError("Found multiple form replacements")
        return next(iter(values))

    @property
    def document(self):
        result = defaultdict(list)
        elements = self.page.find("[fieldalias]")

        for e in elements:
            elem = pq(e)
            attr_name = elem.attr('fieldalias').strip()
            if attr_name:
                result[attr_name].append(elem.text().strip())
        return result


if __name__ == '__main__':
    parser = MayaParser("https://mayafiles.tase.co.il/RHtm/1072001-1073000/H1072716.htm")
    print(parser.id, parser.type, parser.fix_for)
