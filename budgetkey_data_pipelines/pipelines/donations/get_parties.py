import itertools
import requests

from datapackage_pipelines.wrapper import ingest, spew

params, dp, res_iter = ingest()


def get_parties():
    resp = requests.post('https://statements.mevaker.gov.il/Handler/GuarantyDonationPublisherHandler.ashx',
                         data={'action': 'gpn'}).json()
    import logging; logging.info('%r', resp)
    return resp

dp['resources'].append({
    'name': 'parties',
    'path': 'data/parties.csv',
    'schema': {
        'fields': [
            {'name': 'ID', 'type': 'integer'},
            {'name': 'Name', 'type': 'string'},
            {'name': 'IsControl', 'type': 'boolean'},
            {'name': 'IsUpdate', 'type': 'boolean'},
            {'name': 'State', 'type': 'integer'},
            {'name': 'URL', 'type': 'string'},
        ]
    }
})

spew(dp, [get_parties()])