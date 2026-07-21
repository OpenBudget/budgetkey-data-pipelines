import os
import logging
import requests
import tempfile
import shutil
import tabulator
import itertools

from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest

from datapackage_pipelines_budgetkey.processors.resource_without_excel_formulas_lib import iter_resource_without_excel_formulas


with tempfile.NamedTemporaryFile(suffix='.csv') as out:
    with ingest() as ctx:
        parameters, datapackage, res_iter = tuple(ctx)

        url = parameters.get('url')
        resource = parameters.get('resource')
        resource[PROP_STREAMING] = True

        headers, stream_iter = iter_resource_without_excel_formulas(url, parameters)

        datapackage['resources'].append(resource)

        resource['schema'] = {
            'fields': [
                {'name': h, 'type': 'string'}
                for h in headers
            ]
        }
        ctx.resource_iterator = itertools.chain(res_iter, [stream_iter])
