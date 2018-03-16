import time
import logging

from pyquery import PyQuery as pq
import requests

from datapackage_pipelines.wrapper import ingest, spew

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

parameters, datapackage, res_iter = ingest()

selectors = {
    'DisplayCompanyPurpose': 'company_goal',
    'DisplayCompanyLimitType': 'company_limit',
    'Company.Name': 'company_name',
    'Company.NameEnglish': 'company_name_eng',
    'Company.StatusString': 'company_status',
    'DisplayCompanyType': 'company_type',
    'Company.IsGovernmental': 'company_is_government',
    'Company.IsMunicipal': 'company_is_municipal',
    'Company.ActivityDescription': 'company_description',
    'Company.Addresses.0.ZipCode': 'company_postal_code',
    'Company.Addresses.0.StreetName': 'company_street',
    'Company.Addresses.0.HouseNumber': 'company_street_number',
    'Company.Addresses.0.FlatNumber': 'company_flat_number',
    'Company.Addresses.0.CountryName': 'company_country',
    'Company.Addresses.0.CityName': 'company_city',
    'Company.Addresses.0.PostBox': 'company_pob',
    'Company.Addresses.0.AtAddress': 'company_located_at',
    'ContactValidations.LastYearlyReport': 'company_last_report_year',
    'ContactValidations.IsViolatingCompany': 'company_is_mafera',
}


def extract(rec, path):
    path = path.split('.')
    while len(path) > 0:
        part = path.pop(0)
        try:
            part = int(part)
            if len(rec) > part:
                rec = rec[part]
            else:
                return None
        except:
            rec = rec.get(part)
        if rec is None:
            break
    return rec


def get_company_rec(company_id):
    backoff = 1.2
    logging.info('Company %s', company_id)
    for i in range(60):
        headers = {
            'Origin': 'https://ica.justice.gov.il',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9,he;q=0.8',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Accept': '*/*',
            'Referer': 'https://ica.justice.gov.il/GenericCorporarionInfo/SearchCorporation?unit=8',
            'X-Requested-With': 'XMLHttpRequest',
            'Connection': 'keep-alive',
        }
        params = {
            'UnitsType': '8',
            'CorporationType': '3',
            'ContactType': '3',
            'CorporationNameDisplayed': 'no',
            'CorporationNumberDisplayed': '0',
            'CorporationName': '',
            'CorporationNumber': company_id,
            'currentJSFunction': 'Process.SearchCorporation.Search()',
            'RateExposeDocuments': '33.00',
            'TollCodeExposeDocuments': '129',
            'RateCompanyExtract': '10.00',
            'RateYearlyToll': '1120.00',
        }
        resp = requests.post('https://ica.justice.gov.il/GenericCorporarionInfo/SearchGenericCorporation',
                             headers=headers,
                             data=params,
                             allow_redirects=False)
        if resp.status_code != 200:
            time.sleep(backoff)
        else:
            try:
                ret = resp.json()
                logging.exception('Company %s succeeded (%d attempts)', company_id, i+1)
                return ret
            except Exception as e:
                logging.exception('Company %s erred %s, %s', company_id, e, resp.content)
                time.sleep(backoff)
        # backoff *= 1.2


def scrape_company_details(cmp_recs):
    count = 0
    erred = 0
    start = time.time()

    for cmp_rec in cmp_recs:

        if not cmp_rec['__is_stale']:
            continue

        now = time.time()
        count += 1
        if count > 36000 or erred > 4 or (now - start > 3600*6 - 120):
            # limit run time to 6 hours minutes
            continue

        company_id = cmp_rec['Company_Number']

        row = {
            'id': company_id,
            'company_registration_date': cmp_rec['Company_Registration_Date']
        }

        company_rec = get_company_rec(company_id)

        if company_rec is None:
            erred += 1
            continue
        
        company_rec = extract(company_rec, 'Data.0')

        if company_rec is None:
            erred += 1
            continue

        for k, v in selectors.items():
            row[v] = extract(company_rec, k)
            if k in ('Company.Addresses.0.ZipCode', 'Company.Addresses.0.PostBox') and row[v] is not None:
                row[v] = str(row[v])

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
