import itertools

from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING, PATH_PLACEHOLDER

params, dp, res_iter = ingest()

dp['resources'].insert(0, {
    'name': 'report_00000000',
    'path': PATH_PLACEHOLDER,
    PROP_STREAMING: True,
    'schema': {
        'fields': [
            {'name': 'dummy', 'type': 'string'}
        ]
    }
})

spew(dp, itertools.chain([[]], res_iter))