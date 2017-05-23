import datetime
import json
import requests
import logging
import time


from datapackage_pipelines.wrapper import ingest, spew

params, dp, res_iter = ingest()

RANGES = [
    ('01/01', '03/01'),
    ('03/02', '04/30'),
    ('05/01', '06/30'),
    ('07/01', '08/31'),
    ('09/01', '10/31'),
    ('11/01', '12/31'),
]


def get_for_range(cid, year_start, range_start, year_end, range_end):
    logging.info('%r %s/%s -> %s/%s', cid, year_start, range_start, year_end, range_end)
    data = {"PartyID": None,
            "EntityID": cid,
            "EntityTypeID": 1,
            "PublicationSearchType": "1",
            "GD_Name": "",
            "CityID": "",
            "CountryID": "",
            "FromDate": "%s/%d" % (range_start, year_start),
            "ToDate": "%s/%d" % (range_end, year_end),
            "FromSum": "",
            "ToSum": "",
            "ID": None,
            "State": 0,
            "URL": None,
            "IsControl": False,
            "IsUpdate": False}

    resp = []
    for retries in range(10):
        try:
            resp = requests.post('https://statements.mevaker.gov.il/Handler/GuarantyDonationPublisherHandler.ashx',
                                 data={'action': 'gds',
                                       'd': json.dumps(data)}).json()
            break
        except requests.exceptions.RequestException as e:
            logging.error('Retrying in a bit (%r)', e)
            time.sleep(1)

    assert len(resp) == 6
    if len(resp[0]) < 1000:
        return resp[0]

def get_for_candidate(cid):
    year_start = 2010
    year_end = datetime.datetime.now().year
    resp = get_for_range(cid, year_start, RANGES[0][0], year_end, RANGES[-1][-1])
    if resp is None:
        for year in range(year_start, year_end+1):
            resp = get_for_range(cid, year, RANGES[0][0], year, RANGES[-1][-1])
            if resp is None:
                for range_start, range_end in RANGES:
                    resp = get_for_range(cid, year, range_start, year, range_end)
                    assert resp is not None
                    yield resp
            else:
                yield resp
    else:
        yield resp

def get_transactions(rows):
    for row in rows:
        cid = str(row['ID'])
        for resp in get_for_candidate(cid):
            for rr in resp:
                rr["Party"] = row["Party"]
                yield rr

def process_resources(res_iter_):
    first = next(res_iter_)
    yield get_transactions(first)

dp['resources'][0] = {
    'name': 'transactions',
    'path': 'data/candidates.csv',
    'schema': {
        'fields': [
            {'name': 'CandidateName', 'type': 'string'},
            {'name': 'City', 'type': 'string'},
            {'name': 'Country', 'type': 'string'},
            {'name': 'GD_Date', 'type': 'string'},
            {'name': 'GD_Name', 'type': 'string'},
            {'name': 'GD_Sum', 'type': 'string'},
            {'name': 'GuaranteeOrDonation', 'type': 'string'},
            {'name': 'ID', 'type': 'integer'},
            {'name': 'IsControl', 'type': 'boolean'},
            {'name': 'IsUpdate', 'type': 'boolean'},
            {'name': 'Party', 'type': 'string'},
            {'name': 'PublisherTypeID', 'type': 'integer'},
            {'name': 'State', 'type': 'integer'},
            {'name': 'SumInCurrency', 'type': 'string'},
            {'name': 'URL', 'type': 'string'},
        ]
    }
}

spew(dp, process_resources(res_iter))