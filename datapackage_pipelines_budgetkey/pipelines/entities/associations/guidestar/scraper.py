import requests
from pyquery import PyQuery as pq
import time

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()


class Extractor:

    def __init__(self, selector, field, prefix=None, subselector=None):
        self.selector = selector
        self.field = 'association_' + field
        self.array = self.field.endswith('[]')
        self.field = self.field.replace('[]', '')
        self.prefix = prefix
        self.subselector = subselector
        self.calls = 0
        self.matches = 0

    def __call__(self, page, rec):
        self.calls += 1
        assert self.calls < 9000 or self.matches > 0, "No mathces for {} with {}".format(self.field, repr(self.selector))
        if not self.array:
            for value in page.find(self.selector):
                contents = pq(value).text().replace('\n', '')
                if self.prefix is not None:
                    if self.prefix in contents:
                        if self.subselector is None:
                            while self.prefix in contents:
                                contents = contents[contents.find(self.prefix)+len(self.prefix):]
                        else:
                            contents = pq(value).find(self.subselector).text()
                    else:
                        continue
                contents = contents.strip().replace('\n', '')
                if len(contents) > 0:
                    rec[self.field] = contents
                    self.matches += 1
                    break
        else:
            el = page
            selector = self.selector
            if self.prefix is not None:
                for value in page.find(self.selector):
                    contents = pq(value).text()
                    if self.prefix in contents:
                        el = pq(value)
                        selector = self.subselector
                        break

            if el is not None and selector is not None:
                values = el.find(selector)
                values = [pq(value).text().strip().replace('\n', '') for value in values]
                values = [value for value in values if len(value) > 0]
                if len(values) > 0:
                    self.matches += 1
                rec[self.field] = values
        rec.setdefault(self.field, '')


rules = [
    Extractor('.field-collection-item-field-gov-founder .content', 'founders[]'),
    Extractor('.views-field-title-field', 'guidestar_title'),
    Extractor('.views-field-field-vol-slogan', 'guidestar_slogan'),
    Extractor('.views-field-php-1 > .field-content > p[title_removed]', 'activity_region'),
    Extractor('.views-field-php-1 > .field-content div:nth-of-type(2)', 'email'),
    Extractor('.views-field-field-gov-proper-management', 'proper_management', prefix='ניהול תקין'),
    Extractor('.views-field-field-gov-proper-management', 'has_article_46', prefix='זיכוי ממס לתרומות'),
    Extractor('.gov-field', 'field_of_activity', prefix='תחום פעילות הארגון', subselector='.field-content'),
    Extractor('.gov-field', 'objective', prefix='מטרות ארגון רשמיות', subselector='.field-content'),
    Extractor('.gov-field .field-content', 'yearly_turnover', prefix='מחזור שנתי:'),
    Extractor('.gov-field .field-content', 'num_employees', prefix='מספר עובדים:'),
    Extractor('.gov-field .field-content', 'num_volunteers', prefix='מספר מתנדבים:'),
    Extractor('.views-field-field-gov-year-established > .field-content', 'year_established'),
    Extractor('.views-field-field-gov-status > .field-content', 'org_status'),
    Extractor('.views-field-php', 'org_kind', prefix='סוג הארגון:'),
    Extractor('.views-field-php-1 > .field-content > p:not([title_removed])', 'address_lines[]'),
    Extractor('.gov-field', 'roles[]', prefix='חמשת מקבלי השכר הגבוה בעמותה', subselector='.office'),
    Extractor('.gov-field', 'salaries[]', prefix='חמשת מקבלי השכר הגבוה בעמותה', subselector='.subject'),
    Extractor('.gov-field .comp-header', 'online_data_update_year', prefix='מידע מקוון לשנת '),
    Extractor('#annual-reports:first .year:first .year-label', 'last_report_year'),
]


def scrape_guidestar(ass_recs):
    for i, ass_rec in enumerate(ass_recs):

        if not ass_rec['__is_stale']:
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
        for rule in rules:
            rule(page, rec)

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


resource = datapackage['resources'][0]
resource.update(parameters)
resource.setdefault('schema', {})['fields'] = [
    {'name': rule.field,
     'type': 'string' if not rule.array else 'array'}
    for rule in sorted(rules, key=lambda r:r.field)
    if rule.field not in ('association_roles', 'association_salaries')
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
