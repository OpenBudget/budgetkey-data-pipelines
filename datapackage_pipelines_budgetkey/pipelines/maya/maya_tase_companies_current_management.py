from dataflows import Flow, printer, update_resource, load, add_field
import logging

from time import sleep
import requests

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

        management = json["ManagementDetails"]["ManagementAndSeniorExecutives"]
        return details,management
    logging.error(f"FAILED To get Company details {company_id}")
    return None,[]



def process_companies(rows):
    for row in rows:

        details,management = get_company_details(int(row['CompanyTaseId']))
        for manager in management:
            del manager["LastBalanceDate"]
            yield {
                **row,
                **details,
                **manager
            }


def flow(*_):
    return Flow(
        update_resource(
            -1, name='maya_tase_companies_current_management', path="data/maya_tase_companies_current_management.csv",
        ),
        add_field('CompanyLongName', 'string'),
        add_field('CorporateNo', 'string'),
        add_field('Site', 'string'),
        add_field('CapitalPercent', 'string'),
        add_field('EndBalance', 'string'),
        add_field('Id', 'string'),
        add_field('IsFinancialExpert', 'number'),
        add_field('IsInspectionComitee', 'number'),
        add_field('IsManager', 'boolean'),
        add_field('Name', 'string'),
        add_field('RoleType', 'string'),
        add_field('SecurityName', 'string'),
        add_field('VoteCapital', 'string'),
        process_companies
    )


if __name__ == '__main__':
    Flow(
        load('/var/datapackages/maya/scrape-maya-tase-companies/datapackage.json'),
        flow(),
        printer(),
    ).process()
