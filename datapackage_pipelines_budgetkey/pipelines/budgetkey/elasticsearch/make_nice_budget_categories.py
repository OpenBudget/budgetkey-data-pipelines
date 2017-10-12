from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew

parameters, dp, res_iter = ingest()


def process_resource(res_):
    for row in res_:
        okay = True
        for f in ('func_cls_title_1', 'func_cls_title_2'):
            if f not in row:
                okay = False
                break
            if len(row[f]) != 1:
                okay = False
                break
        if okay:
            row['nice-category'] = '{func_cls_title_1[0]} / {func_cls_title_2[0]}'.format(**row)
        else:
            row['nice-category'] = ''
        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_resource(first)
    yield from res_iter_


dp['resources'][0]['schema']['fields'].append(
    {
        'name': 'nice-category',
        'type': 'string'
    }
)

spew(dp, process_resources(res_iter))
