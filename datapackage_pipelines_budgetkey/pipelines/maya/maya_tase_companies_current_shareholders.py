from dataflows import Flow, printer, update_resource, load, add_field
import logging

from time import sleep
import requests
import dateutil.parser

logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

session = requests.Session()

def get_company_details(company_id):
    retries = 0
    wait = 5
    while retries < 3:
        headers= {
            "accept": "application/json, text/plain, */*",
            "accept-language": "he-IL",
            "X-Maya-With": "allow"
        }

        session.cookies.clear()
        res = session.get(f"https://mayaapi.tase.co.il/api/company/alldetails?companyId={company_id}", headers=headers)
        sleep(wait)
        if res.status_code != 200:
            retries += 1
            wait *= 2
            logging.error(f"Retry To get Company {company_id}  in {wait} seconds")
            continue

        json = res.json()
        company_json = json['CompanyDetails']

        details = {
            "CompanyLongName": company_json["CompanyLongName"],
            "CompanyName": company_json["CompanyName"],
            "CorporateNo": company_json["CorporateNo"],
            "Site": company_json["Site"]
        }

        shareholders = json["ShareHolders"]["ShareHoldersList"]
        return details,shareholders
    logging.error(f"FAILED To get Company details {company_id}")
    return None,[]


def process_companies(rows):
    for row in rows:
        company_id = int(row['CompanyTaseId'])
        details, shareholders = get_company_details(company_id)
        for shareholder in shareholders:
            del shareholder['CompanyId']
            yield {
                **row,
                **details,
                **shareholder,
                "LastUpdateDate": dateutil.parser.parse(shareholder["LastUpdateDate"])
            }

def flow(*_):
    return Flow(
        update_resource(
            -1, name='maya_tase_companies_current_shareholders', path="data/maya_tase_companies_current_shareholders.csv",
        ),
        add_field('CompanyName', 'string'),
        add_field('CompanyLongName', 'string'),
        add_field('CorporateNo', 'string'),
        add_field('Site', 'string'),

        add_field('CapitalPercent', 'number'),
        add_field('EndBalance', 'number'),
        add_field('HolderId', 'number'),
        add_field('HolderName', 'string'),
        add_field('IsTradeWritten', 'number'),
        add_field('LastUpdateDate', 'date'),
        add_field('MarketValue', 'number'),
        add_field('Remark', 'string'),
        add_field('SecurityId', 'number'),
        add_field('SecurityType', 'number'),
        add_field('SecurityName', 'string'),
        add_field('ShareHolderCompanyId', 'number'),
        add_field('IsTradeWritten', 'number'),
        add_field('VoteCapital', 'number'),
        process_companies
    )


if __name__ == '__main__':
    Flow(
        load('/var/datapackages/maya/scrape-maya-tase-companies/datapackage.json'),
        flow(),
        printer(),
    ).process()
