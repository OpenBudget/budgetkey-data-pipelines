import requests
from pyquery import PyQuery as pq
import time

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()

classes = {
    '.views-field-title-field': 'guidestar_title',
    '.views-field-field-gov-objectives.has-text > .field-content': 'objective',
    '.views-field-php-1 > .field-content': 'address',
    '#block-views-organization-page-block-2 .views-field-php > .field-content': 'org_kind',
    '#block-views-organization-page-block-2 .views-field-field-gov-year-established > .field-content': 'year_established',
    '#block-views-organization-page-block-2 .views-field-field-gov-proper-management > .field-content': 'proper_management',
    '.field-collection-item-field-gov-founder .content': 'founders[]',
    '#nonononono': 'id'
}


def scrape_guidestar(ass_recs):
    count = 0
    for i, ass_rec in enumerate(ass_recs):

        if ass_rec.get('id') is not None:
            yield ass_rec
            continue

        count+=1
        if count > 10:
            continue

        assert 'Association_Number' in ass_rec

        guidestar_url = 'http://www.guidestar.org.il/he/organization/{}'.format(ass_rec['Association_Number'])
        page = None
        while page is None:
            try:
                page = pq(requests.get(guidestar_url).content)
            except:
                time.sleep(60)

        rec = {}
        for selector, field in classes.items():
            field = 'association_' + field
            if not field.endswith('[]'):
                value = page.find(selector).text().strip().replace('\n', '')
                if len(value) > 0:
                    rec[field] = value
            else:
                field = field.replace('[]', '')
                values = page.find(selector)
                values = [pq(value).text().strip().replace('\n', '') for value in values]
                values = [value for value in values if len(value) > 0]
                if len(values) > 0:
                    rec[field] = ";".join(values)
            rec.setdefault(field, '')
        rec['id'] = ass_rec['Association_Number']
        rec['association_registration_date'] = ass_rec['Association_Registration_Date']
        rec['association_title'] = ass_rec['Association_Name']

        yield rec


def process_resources(res_iter_):
    first = next(res_iter_)
    yield scrape_guidestar(first)

    for res in res_iter_:
        yield res


headers = ["id",
           "association_title",
           "association_guidestar_title",
           "association_org_kind",
           "association_proper_management",
           "association_year_established",
           "association_address",
           "association_objective",
           "association_founders",
           "association_registration_date"]

resource = datapackage['resources'][0]
resource.update(parameters)
resource.setdefault('schema', {})['fields'] = [
    {'name': header, 'type': 'string'}
    for header in headers
]

spew(datapackage, process_resources(res_iter))
