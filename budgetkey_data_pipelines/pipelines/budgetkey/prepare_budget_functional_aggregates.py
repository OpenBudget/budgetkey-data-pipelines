import datetime

from datapackage_pipelines.wrapper import process

curyear = datetime.datetime.now().year


def process_row(row, *_):
    if len(row['code']) == 10 and row['year'] == curyear:
        ret = {
            'func_cls_title_1': row['func_cls_title_1'][0],
            'func_cls_title_2': row['func_cls_title_2'][0],
            'net_allocated': row['net_allocated']
        }
        return ret


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'] = [
        {'name': 'func_cls_title_1', 'type': 'string'},
        {'name': 'func_cls_title_2', 'type': 'string'},
        {'name': 'net_allocated', 'type': 'number'},
    ]
    return dp

process(modify_datapackage=modify_datapackage,
        process_row=process_row)