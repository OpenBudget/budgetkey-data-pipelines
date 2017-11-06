import json

from datapackage_pipelines.wrapper import spew, ingest

parameters, datapackage, res_iter = ingest()


datapackage['resources'][0]['schema']['fields'].extend([
    {
        'name': 'econ_cls_json',
        'type': 'string'
    },
    {
        'name': 'func_cls_json',
        'type': 'string'
    }
])


def process_first(rows):
    for row in rows:
        econ_cls = [row[t] for t in ['econ_cls_code_1',
                                     'econ_cls_title_1',
                                     'econ_cls_code_2',
                                     'econ_cls_title_2']]
        row['econ_cls_json'] = json.dumps(econ_cls)
        func_cls = [row[t] for t in ['func_cls_code_1',
                                     'func_cls_title_1',
                                     'func_cls_code_2',
                                     'func_cls_title_2']]
        row['func_cls_json'] = json.dumps(func_cls)
        yield row


def process_resources(res_iter_):
    first = next(res_iter_)
    yield process_first(first)

    for res in res_iter_:
        yield res

spew(datapackage, process_resources(res_iter))
