import datetime
import json
import requests
import logging
import time

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew


class GetTransactions(object):

    RANGES = [
        ('01/01', '01/15'),
        ('01/16', '02/01'),
        ('02/02', '02/15'),
        ('02/16', '03/01'),
        ('03/02', '03/15'),
        ('03/16', '04/01'),
        ('04/02', '04/15'),
        ('04/16', '05/01'),
        ('05/02', '05/15'),
        ('05/16', '06/01'),
        ('06/02', '06/15'),
        ('06/16', '07/01'),
        ('07/02', '07/15'),
        ('07/16', '08/01'),
        ('08/02', '08/15'),
        ('08/16', '09/01'),
        ('09/02', '09/15'),
        ('09/16', '10/01'),
        ('10/02', '10/15'),
        ('10/16', '11/01'),
        ('11/02', '11/15'),
        ('11/16', '12/01'),
        ('12/02', '12/15'),
        ('12/16', '12/31')
    ]

    def requests_post(self, url, data):
        return requests.post(url, data=data).json()

    def get_for_range(self, cid, year_start, range_start, year_end, range_end):
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
                resp = self.requests_post('https://statements.mevaker.gov.il/Handler/GuarantyDonationPublisherHandler.ashx',
                                          data={'action': 'gds',
                                                'd': json.dumps(data)})
                break
            except requests.exceptions.RequestException as e:
                logging.error('Retrying in a bit (%r)', e)
                time.sleep(1)

        assert len(resp) == 6
        if len(resp[0]) < 1000:
            return resp[0]
        else:
            print(len(resp[0]))

    def get_for_candidate(self,cid):
        year_start = 2010
        year_end = datetime.datetime.now().year
        resp = self.get_for_range(cid, year_start, self.RANGES[0][0], year_end, self.RANGES[-1][-1])
        if resp is None:
            for year in range(year_start, year_end + 1):
                resp = self.get_for_range(cid, year, self.RANGES[0][0], year, self.RANGES[-1][-1])
                if resp is None:
                    for range_start, range_end in self.RANGES:
                        resp = self.get_for_range(cid, year, range_start, year, range_end)
                        assert resp is not None
                        yield resp
                else:
                    yield resp
        else:
            yield resp

    def get_transactions(self, rows):
        for row in rows:
            cid = str(row['ID'])
            for resp in self.get_for_candidate(cid):
                for rr in resp:
                    rr["Party"] = row["Party"]
                    yield rr


def process_resources(res_iter_):
    first = next(res_iter_)
    yield GetTransactions().get_transactions(first)

def get_resource_descriptor():
    return {
        'name': 'transactions',
        PROP_STREAMING: True,
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


if __name__ == "__main__":
    params, dp, res_iter = ingest()
    dp['resources'][0] = get_resource_descriptor()
    spew(dp, process_resources(res_iter))
