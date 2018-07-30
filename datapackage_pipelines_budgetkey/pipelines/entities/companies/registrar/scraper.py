import time
import logging
import collections
import os

from pyquery import PyQuery as pq
import requests
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError, OperationalError

from datapackage_pipelines.wrapper import ingest, spew

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

parameters, datapackage, res_iter = ingest()

if 'db-table' in parameters:
    db_table = parameters.pop('db-table')
    connection_string = os.environ['DPP_DB_ENGINE'].replace('@postgres', '@data-next.obudget.org')
    engine = create_engine(connection_string)
else:
    db_table = None
    engine = None

def skip_entry(id):
    query = """
update {} 
set (__last_updated_at,__next_update_days)=(date_trunc('second',current_timestamp),60) 
where id='{}'
    """
    if None not in (db_table, engine):
        query = query.format(db_table, id)
        logging.info('Skipping company %s', id)
        logging.info('Query: %r', query)
        engine.execute(query)

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
    backoff = 3
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
        if resp.status_code == 200:
            try:
                ret = resp.json()
                if ret.get('Success'):
                    logging.info('Company %s succeeded (%d attempts)', company_id, i+1)
                    return ret
                else:
                    logging.error('Company %s got error response', company_id)
                    return 'not-found'
            except Exception:
                logging.exception('Company %s erred %s', company_id, resp.content)

        time.sleep(backoff)
        # backoff *= 1.2
    logging.error('Company %s erred timeout', company_id)


def scrape_company_details(cmp_recs):
    count = 0
    erred = 0
    start = time.time()

    for cmp_rec in cmp_recs:

        if not cmp_rec['__is_stale']:
            continue

        now = time.time()
        count += 1
        if count > 7000 or erred > 4:
            # limit run time to 6 hours minutes
            logging.info('count=%d, erred=%d, elapsed=%d', count, erred, int(now - start))
            collections.deque(cmp_recs, 0)
            break

        company_id = cmp_rec['Company_Number']

        row = {
            'id': company_id,
            'company_name': cmp_rec['Company_Name'],
            'company_registration_date': cmp_rec['Company_Registration_Date']
        }

        company_rec_ = get_company_rec(company_id)

        if company_rec_ is None:
            logging.error('COMPANY REC is NONE: %s', company_id)
            erred += 1
            continue
        
        if company_rec_ != 'not-found':
            company_rec = extract(company_rec_, 'Data.0')

            if company_rec is None:
                logging.error('COMPANY DATA is NONE: %r', company_rec_)
                erred += 1
                continue

            for k, v in selectors.items():
                row[v] = extract(company_rec, k)
                if k in ('Company.Addresses.0.ZipCode', 'Company.Addresses.0.PostBox') and row[v] is not None:
                    row[v] = str(row[v])
        else:
            skip_entry(company_id)
            continue

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
