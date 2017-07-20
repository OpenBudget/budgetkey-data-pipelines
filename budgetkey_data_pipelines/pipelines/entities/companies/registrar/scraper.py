import time
import logging

from pyquery import PyQuery as pq
import requests

from datapackage_pipelines.wrapper import ingest, spew


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


def retryer(session, method, *args, **kwargs):
    while True:
        try:
            kwargs['timeout'] = 60
            return getattr(session, method)(*args, **kwargs)
        except Exception as e:
            logging.info('Got Exception %s, retrying', e)
            time.sleep(60)


def scrape_company_details(cmp_recs):
    count = 0
    for i, cmp_rec in enumerate(cmp_recs):

        if not cmp_rec['__is_stale']:
            continue

        count += 1
        if count > 1000:
            continue

        company_id = cmp_rec['Company_Number']

        row = {
            'id': company_id,
            'company_registration_date': cmp_rec['Company_Registration_Date']
        }

        session = requests.Session()
        response = retryer(session, 'get', 'http://havarot.justice.gov.il/CompaniesList.aspx')
        page = pq(response.text)

        form_data = {
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
        }

        for form_data_elem_ in page.find('#aspnetForm input'):
            form_data_elem = pq(form_data_elem_)
            form_data[form_data_elem.attr('name')] = pq(form_data_elem).attr('value')

        form_data['ctl00$CPHCenter$txtCompanyNumber'] = company_id

        response = retryer(session, 'post', 'http://havarot.justice.gov.il/CompaniesList.aspx', data=form_data)
        page = pq(response.text)

        if len(page.find('#CPHCenter_GridBlock').find('a')) >= 0:
            response = retryer(session, 'get', 'http://havarot.justice.gov.il/CompaniesDetails.aspx?id=%s' % company_id)
            page = pq(response.text)

            for k, v in selectors.items():
                row[v] = page.find(k).text()
            if row['company_name'].strip() == '':
                if row['company_name_eng'].strip() != '':
                    row['company_name'] = row['company_name_eng']
                else:
                    logging.error('Failed to get data for company %s: %r', company_id, row)

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
resource['schema']['fields'].append({
    'name': 'company_registration_date',
    'type': 'date'
})

spew(datapackage, process_resources(res_iter))
