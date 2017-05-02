import tabulator

from datapackage_pipelines.wrapper import ingest, spew

parameters, datapackage, res_iter = ingest()
input_file = parameters['input-file']

reports = tabulator.Stream(input_file, headers=1).open().iter(keyed=True)
for i, report in enumerate(reports):
    datapackage['resources'].append({
        'url': report['report-url'],
        'name': 'report_{}'.format(i)
    })

import logging; logging.info(datapackage)
spew(datapackage, res_iter)
