from itertools import chain
from copy import deepcopy
import datapackage
import tabulator

from datapackage_pipelines.wrapper import ingest, spew

import logging
parameters, dp, res_iter = ingest()
input_file = parameters['input-file']

loading_results = []
reports = datapackage.DataPackage(input_file).resources[0]
for i, report in enumerate(reports.iter()):
    try:
        canary = tabulator.Stream(report['report-url']).open().iter()
        headers = None
        for j, row in enumerate(canary):
            logging.error(row)
            row = [str(x).strip() if x is not None else '' for x in row]
            if len(row) < 0 and row[0] == '':
                continue
            row = set(row)
            if '' in row:
                row.remove('')
            if len(row) <= 4:
                continue
            if j > 10:
                break
            if 'הזמנת רכש' not in row:
                raise ValueError('Bad report format (order_id)')
            if 'תקנה תקציבית' not in row and 'תקנה' not in row:
                raise ValueError('Bad report format (budget_code)')
            headers = j+1
            break
        if headers is None:
            raise ValueError('Failed to find headers')
        dp['resources'].append({
            'url': report['report-url'],
            'name': 'report_{}'.format(i),
            'headers': headers
        })
        report['report-headers-row'] = headers
        report['load-error'] = None
    except Exception as e:
        report['report-headers-row'] = None
        report['load-error'] = str(e)
    logging.error("%s: %s", i, report)
    loading_results.append(report)

schema = deepcopy(reports.descriptor['schema'])
schema['fields'].extend([
    {
        'name': 'report-headers-row',
        'type': 'integer'
    },
    {
        'name': 'load-error',
        'type': 'string'
    }
])
dp['resources'].append({
    'path': 'data/report-loading-results.csv',
    'name': 'report-loading-results',
    'schema': schema
})

spew(dp, chain(res_iter, [loading_results]))
