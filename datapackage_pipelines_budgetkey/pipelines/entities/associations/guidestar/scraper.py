import requests
from pyquery import PyQuery as pq
import time

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()

classes = {
    '.views-field-title-field': 'guidestar_title',
    '.views-field-field-vol-slogan': 'guidestar_slogan',
    '#main .views-field-field-gov-objectives.has-text > .field-content': 'objective',
    '.region-sidebar-second .views-field-field-gov-objectives.has-text > .field-content': 'field_of_activity',
    '.views-field-php-1 > .field-content > p:not([title_removed])': 'address_lines[]',
    '.views-field-php-1 > .field-content > p[title_removed]': 'activity_region',
    '.views-field-php-1 > .field-content div:nth-of-type(2)': 'email',
    '#block-views-organization-page-block-2 .views-field-php > .field-content': 'org_kind',
    '#block-views-organization-page-block-2 .views-field-field-gov-status > .field-content': 'org_status',
    '#block-views-organization-page-block-2 .views-field-field-gov-year-established > .field-content': 'year_established',
    '[id="j_id0:GS_Template:j_id68"]': 'proper_management',
    '[id="j_id0:GS_Template:j_id78"]': 'has_article_46',
    '[id="j_id0:GS_Template:j_id620"] .field-content': 'gov_bits[]',
    '.field-collection-item-field-gov-founder .content': 'founders[]',
    '[id="j_id0:GS_Template:j_id291:j_id292:j_id294:salaryTable"] .office': 'roles[]',
    '[id="j_id0:GS_Template:j_id291:j_id292:j_id294:salaryTable"] .subject': 'salaries[]',
}

bits = {
    'מחזור שנתי': 'annual_turnover',
    'מספר עובדים': 'num_employees',
    'מספר מתנדבים': 'num_volunteers'
}


def scrape_guidestar(ass_recs):
    count = 0
    for i, ass_rec in enumerate(ass_recs):

        if not ass_rec['__is_stale']:
            continue

        count += 1
        if count > 10000:
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
                value = page.find(selector)
                if len(value) > 0:
                    value = pq(value[0]).text().strip().replace('\n', '')
                if len(value) > 0:
                    rec[field] = value
            else:
                field = field.replace('[]', '')
                values = page.find(selector)
                values = [pq(value).text().strip().replace('\n', '') for value in values]
                values = [value for value in values if len(value) > 0]
                if len(values) > 0:
                    rec[field] = values
                else:
                    rec[field] = []
            rec.setdefault(field, '')

        for bit in rec['association_gov_bits']:
            found = False
            for k, v in bits.items():
                if bit.startswith(k+':'):
                    bit = bit[len(k)+1:].strip()
                    rec['association_'+v] = bit
                    found = True
                    break
            assert found, "Failed to find bit mathcing %r" % bit
        del rec['association_gov_bits']
        rec['id'] = ass_rec['Association_Number']
        rec['association_registration_date'] = ass_rec['Association_Registration_Date']
        rec['association_title'] = ass_rec['Association_Name']
        rec['association_address'] = ' '.join(rec['association_address_lines'])
        rec['association_activity_region'] = rec['association_activity_region'].replace('מקום פעילות:', '').strip()

        rec['association_top_salaries'] = [
            dict(
                role=r,
                salary=float(s.replace(',', '').replace('\u20aa', ''))
            )
            for r, s in zip(rec['association_roles'], rec['association_salaries'])
        ]
        del rec['association_roles']
        del rec['association_salaries']
        yield rec


def process_resources(res_iter_):
    first = next(res_iter_)
    yield scrape_guidestar(first)

    for res in res_iter_:
        yield res


headers = sorted(classes.values())
bits_headers = sorted(bits.values())

resource = datapackage['resources'][0]
resource.update(parameters)
resource.setdefault('schema', {})['fields'] = [
    {'name': 'association_' + header.replace('[]', ''),
     'type': 'string' if not header.endswith('[]') else 'array'}
    for header in headers + bits_headers
    if header not in ('gov_bits[]', 'roles[]', 'salaries[]')
]
resource['schema']['fields'].extend([
    {
        'name': 'association_top_salaries',
        'type': 'array',
        'es:itemType': 'object',
        'es:index': False
    },
    {
        'name': 'association_address',
        'type': 'string'
    },
    {
        'name': 'id',
        'type': 'string'
    },
    {
        'name': 'association_title',
        'type': 'string'
    },
    {
        'name': 'association_registration_date',
        'type': 'date'
    }
])
resource['schema']['missingValues'] = ['', "לא קיים דיווח מקוון"]

spew(datapackage, process_resources(res_iter))
