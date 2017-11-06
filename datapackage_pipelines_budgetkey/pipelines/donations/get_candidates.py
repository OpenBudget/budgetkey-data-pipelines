import itertools
import requests
from datapackage_pipelines.utilities.resources import PROP_STREAMING

from datapackage_pipelines.wrapper import ingest, spew

params, dp, res_iter = ingest()


def get_candidates(rows):
    for row in rows:
        party_id = row['ID']
        resp = requests.post('https://statements.mevaker.gov.il/Handler/GuarantyDonationPublisherHandler.ashx',
                             data={'action': 'gcbp',
                                   'p': party_id,
                                   'ele': 'null'}).json()
        for rr in resp:
            rr['Party'] = row['Name']
            yield rr

def process_resources(res_iter_):
    first = next(res_iter_)
    yield get_candidates(first)

dp['resources'][0] = {
    'name': 'candidates',
    PROP_STREAMING: True,
    'path': 'data/candidates.csv',
    'schema': {
        'fields': [
            {'name': 'ID', 'type': 'integer'},
            {'name': 'Name', 'type': 'string'},
            {'name': 'Party', 'type': 'string'},
            {'name': 'IsControl', 'type': 'boolean'},
            {'name': 'IsUpdate', 'type': 'boolean'},
            {'name': 'State', 'type': 'integer'},
            {'name': 'URL', 'type': 'string'},
        ]
    }
}

spew(dp, process_resources(res_iter))