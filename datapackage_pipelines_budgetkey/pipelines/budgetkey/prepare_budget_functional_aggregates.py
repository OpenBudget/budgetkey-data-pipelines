import datetime

from datapackage_pipelines.wrapper import process

curyear = datetime.datetime.now().year


def process_row(row, *_):
    if len(row['code']) == 10 and row['year'] == curyear:
        ret = {
            'year': row['year'],
            'func_cls_title_1': row['func_cls_title_1'][0],
            'func_cls_title_2': row['func_cls_title_2'][0],
            'net_revised': int(row['net_revised'] or 0),
        }
        return ret


def modify_datapackage(dp, *_):
    dp['resources'][0]['schema']['fields'] = [
        {'name': 'func_cls_title_1', 'type': 'string'},
        {'name': 'func_cls_title_2', 'type': 'string'},
        {'name': 'net_revised', 'type': 'integer'},
        {'name': 'year', 'type': 'integer'},
    ]
    dp['resources'][0]['name'] = 'budget-functional-aggregates'
    return dp


process(modify_datapackage=modify_datapackage,
        process_row=process_row)