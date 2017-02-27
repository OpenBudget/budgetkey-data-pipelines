from pyquery import PyQuery as pq

from datapackage_pipelines.wrapper import ingest, spew

import requests

parameters, datapackage, res_iter = ingest()

selectors = {
    '#CPHCenter_lblCompanyNameHeb': 'company_name',
    '#CPHCenter_lblCompanyNameEn': 'company_name_eng',
    '#CPHCenter_lblStatus': 'company_status',
    '#CPHCenter_lblCorporationType': 'company_type',
    '#CPHCenter_lblGovCompanyType': 'company_government',
    '#CPHCenter_lblLimitType': 'company_limit',
    '#CPHCenter_lblZipCode': 'company_postal_code',
    '#CPHCenter_lblStatusMafera1': 'company_mafera',
    '#CPHCenter_lblStreet': 'company_street',
    '#CPHCenter_lblStreetNumber': 'company_street_number',
    '#CPHCenter_lblCountry': 'company_country',
    '#CPHCenter_lblCity': 'company_city',
    '#CPHCenter_lblPOB': 'company_pob',
    '#CPHCenter_lblCityPB': 'company_pob_city',
    '#CPHCenter_lblZipCodePB': 'company_pob_postal_code',
    '#CPHCenter_lblLocatedAt': 'company_located_at',
    '#CPHCenter_lblCompanyGoal': 'company_goal',
    '#CPHCenter_lblCompanyDesc': 'company_description',
    '#CPHCenter_lblDochShana': 'company_last_report_year'
}


def scrape_company_details(cmp_recs):
    for i, cmp_rec in enumerate(cmp_recs):

        if cmp_rec.get('id') is not None:
            yield cmp_rec
            continue

        assert 'Company_Number' in cmp_rec
        company_id = cmp_rec['Company_Number']

        row = {
            'id': company_id
        }

        session = requests.Session()
        response = session.get('http://havarot.justice.gov.il/CompaniesList.aspx', timeout=60)
        page = pq(response.text)

        form_data = {
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
        }

        for form_data_elem_ in page.find('#aspnetForm input'):
            form_data_elem = pq(form_data_elem_)
            form_data[form_data_elem.attr('name')] = pq(form_data_elem).attr('value')

        form_data['ctl00$CPHCenter$txtCompanyNumber'] = company_id

        response = session.post('http://havarot.justice.gov.il/CompaniesList.aspx', data=form_data, timeout=60)
        page = pq(response.text)

        if len(page.find('#CPHCenter_GridBlock').find('a')) == 0:
            continue

        response = session.get('http://havarot.justice.gov.il/CompaniesDetails.aspx?id=%s' % company_id, timeout=60)
        page = pq(response.text)

        for k, v in selectors.items():
            row[v] = page.find(k).text()

        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield scrape_company_details(first)

    for res in res_iter_:
        yield res


headers = sorted(selectors.values())

resource = datapackage['resources'][0]
resource.update(parameters)
resource.setdefault('schema', {})['fields'] = [
    {'name': header, 'type': 'string'}
    for header in headers + ['id']
    ]

spew(datapackage, process_resources(res_iter))
