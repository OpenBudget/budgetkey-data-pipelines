import datetime
import re

import dataflows

from datapackage_pipelines.utilities.resources import PROP_STREAMING

SOURCE_URL = 'http://www.mevaker.gov.il/(X(1)S(uuokupc212t5005shmjbxtbd))/he/mimun/donation/Pages/default.aspx?AspxAutoDetectCookieSupport=1'


def add_fields_to_schema(package):
    package.pkg.descriptor['resources'][0]['schema']['fields'].extend([
        {'name': 'full_name', 'type': 'string'},
        {'name': 'company', 'type': 'string'},
        {'name': 'date', 'type': 'date'},
        {'name': 'title', 'type': 'string'},
        {'name': 'event', 'type': 'string'},
        {'name': 'sources', 'type': 'array'},
    ])
    package.pkg.descriptor['resources'][0][PROP_STREAMING] = True

    yield package.pkg
    yield from package


def convert_donation_transaction_to_people(row):
    return {
        'full_name': row['CandidateName'],
        'company': '',
        'date': _parse_date_time(row['GD_Date']),
        'title': '{donator} from {place} {donated_or_guaranteed} {money_sum} to {candidate_name}'.format(
            donator=row['GD_Name'], money_sum=row['GD_Sum'], candidate_name=row['CandidateName'],
            place='{}, {}'.format(row['City'], row['Country']),
            donated_or_guaranteed='donated' if row['GuaranteeOrDonation'] == "תרומה" else 'guaranteed',
        ),
        'event': 'political donation',
        'sources': [{'url': SOURCE_URL}],
    }


def _parse_date_time(date_str):
    match = re.match(r'/Date\((\d+)\)/', date_str)
    return datetime.date.fromtimestamp(int(match.group(1)) / 1000) if match else ''


def flow(parameters, datapackage, resources, stats):
    return dataflows.Flow(
        dataflows.load('https://next.obudget.org/datapackages/donations/transactions/datapackage.json'),
        add_fields_to_schema,
        convert_donation_transaction_to_people,
    )
