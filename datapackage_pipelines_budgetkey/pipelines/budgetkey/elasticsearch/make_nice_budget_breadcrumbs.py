import logging
from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()


def process_resource(res_):
    for row in res_:
        levels = ''
        if row.get('hierarchy'):
            levels = [x[1] for x in row.get('hierarchy', [])[1:]]
            years = ''
            history = row.get('history')
            if history:
                years = '{} - '.format(min(history.keys()))
            years += '{}'.format(row.get('year'))
            levels = [years] + levels
            levels = ' / '.join(levels)
        else:
            logging.warning('No hierarchy for row %r', row)
        row['nice-breadcrumbs'] = levels
        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_resource(first)
    yield from res_iter_


dp['resources'][0]['schema']['fields'].append(
    {
        'name': 'nice-breadcrumbs',
        'type': 'string'
    }
)

spew(dp, process_resources(res_iter))
