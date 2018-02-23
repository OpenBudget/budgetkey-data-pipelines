import os
import json
import logging

import requests

from datapackage_pipelines.lib.dump.to_path import PathDumper


class BudgetKeyPathDumper(PathDumper):

    def datapackage_path(self):
        datapackage_path = os.path.join(self.out_path, 'datapackage.json')
        return datapackage_path

    def resource_url(self, res_path):
        assert self.out_path.startswith('/var/datapackages')
        dp_path = self.out_path[5:]
        res_path = os.path.join(dp_path, res_path)
        return 'https://next.obudget.org/{}'.format(res_path)

    def finalize(self):
        super(BudgetKeyPathDumper, self).finalize()
        if any(os.environ.get('DATAHUB_'+x) is None
               for x in ('OWNER', 'OWNERID', 'ENDPOINT', 'TOKEN')):
            return
        dp_path = self.datapackage_path()
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
                    'url': self.resource_url('datapackage.json'),
                    'parameters': {
                        'descriptor': datapackage,
                        'resource-mapping': dict(
                            (r['name'], self.resource_url(r['path']))
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
        logging.info('GOT response from DataHub %r', resp)
        token = resp['token']
        resp = requests.post(os.environ['DATAHUB_ENDPOINT']+'/source/upload', json=flowspec,
                             headers={
                                 'Auth-Token': token
                             }
                            ).json()
        logging.info('GOT response from DataHub %r', resp)
        assert resp['success']


if __name__ == '__main__':
    BudgetKeyPathDumper()()