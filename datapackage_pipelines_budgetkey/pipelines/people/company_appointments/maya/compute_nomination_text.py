import logging
from datapackage_pipelines.wrapper import process


def modify_datapackage(datapackage, *_):
    datapackage['resources'][0]['schema']['fields'].extend([
        {'name': 'details', 'type': 'string'},
    ])
    return datapackage


def process_row(row, *_):
    if row['gender'] == 'man':
        row['details'] = '{} מונה ל{} ב{}'.format(row['name'], row['position'], row['organisation_name'])
    elif row['gender'] == 'woman':
        row['details'] = '{} מונתה ל{} ב{}'.format(row['name'], row['position'], row['organisation_name'])
    else:
        row['details'] = '{} מונ/תה ל{} ב{}'.format(row['name'], row['position'], row['organisation_name'])
    return row


process(process_row=process_row, modify_datapackage=modify_datapackage)
