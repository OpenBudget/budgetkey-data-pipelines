import os
import json
import logging

import requests

from datapackage_pipelines.lib.dump_to_path import flow as dtp_flow


def resource_url(out_path, res_path):
    assert out_path.startswith('/var/datapackages')
    assert out_path.endswith('datapackage.json')
    dp_path = out_path[5:-16]
    res_path = os.path.join(dp_path, res_path)
    return 'https://next.obudget.org/{}'.format(res_path)


if __name__ == '__main__':
    with ingest() as ctx:
        spew_flow(dtp_flow(ctx.parameters, ctx.stats), ctx)

        if any(os.environ.get('DATAHUB_'+x) is None
               for x in ('OWNER', 'OWNERID', 'ENDPOINT', 'TOKEN')):
            return

        dp_path = ctx.stats[STATS_DPP_KEY][STATS_OUT_DP_URL_KEY]
        datapackage = json.load(open(dp_path))
        flowspec = {
            'meta': {
                'owner': os.environ['DATAHUB_OWNER'],
                'ownerid': os.environ['DATAHUB_OWNERID'],
                'dataset': datapackage['name'],
                'version': 1,
                'findability': 'published'
            },
            'inputs': [
                {
                    'kind': 'datapackage',
                    'url': resource_url(dp_path, 'datapackage.json'),
                    'parameters': {
                        'descriptor': datapackage,
                        'resource-mapping': dict(
                            (r['name'], resource_url(dp_path, r['path']))
                            for r in datapackage['resources']
                        )
                    }
                }
            ]
        }
        resp = requests.get(os.environ['DATAHUB_ENDPOINT']+'/auth/authorize', 
                            params={'service': 'source'},
                             headers={
                                 'Auth-Token': os.environ['DATAHUB_TOKEN']
                             }).json()
        token = resp['token']
        resp = requests.post(os.environ['DATAHUB_ENDPOINT']+'/source/upload', json=flowspec,
                             headers={
                                 'Auth-Token': token
                             }
                            ).json()
        logging.info('UPLOAD response: %r', resp)
        assert resp['success'], json.dumps(resp, indent=2)
